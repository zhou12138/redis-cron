"""单元测试：核心逻辑。"""

from __future__ import annotations

import asyncio
import json
import time

import pytest
import pytest_asyncio

from redis_cron.models import CronTask, Task
from redis_cron.utils import calc_next_fire, calc_shard_id, calc_stable_jitter


# ========== models 测试 ==========


class TestTask:
    def test_to_redis_roundtrip(self):
        """Task 序列化和反序列化应保持一致。"""
        t = Task(
            task_id="abc123",
            task_type="send_email",
            payload={"to": "test@example.com"},
            user_id=10001,
            shard_id=17,
            fire_time=1715234400.0,
        )
        data = t.to_redis()
        assert data["task_type"] == "send_email"
        assert data["is_cron"] == "0"
        assert data["status"] == "active"

        restored = Task.from_redis("abc123", data)
        assert restored.task_id == "abc123"
        assert restored.task_type == "send_email"
        assert restored.payload == {"to": "test@example.com"}
        assert restored.user_id == 10001
        assert restored.status == "active"
        assert restored.max_retries == 0
        assert restored.retry_delay == 60

    def test_cron_task_roundtrip(self):
        """CronTask 序列化和反序列化应保持一致。"""
        t = CronTask(
            task_id="cron001",
            task_type="daily_report",
            cron="0 8 * * *",
            user_id=10001,
            shard_id=17,
            payload={"subject": "报表"},
            max_retries=3,
            retry_delay=30,
        )
        data = t.to_redis()
        assert data["is_cron"] == "1"
        assert data["cron"] == "0 8 * * *"
        assert data["max_retries"] == "3"
        assert data["retry_delay"] == "30"

        restored = CronTask.from_redis("cron001", data)
        assert restored.cron == "0 8 * * *"
        assert restored.task_type == "daily_report"
        assert restored.max_retries == 3
        assert restored.retry_delay == 30

    def test_status_fields(self):
        """新增的状态和统计字段应正确序列化。"""
        t = Task(
            task_id="t1",
            task_type="test",
            status="running",
            run_count=5,
            fail_count=2,
            last_error="timeout",
            last_run_at=1234567890.0,
        )
        data = t.to_redis()
        restored = Task.from_redis("t1", data)
        assert restored.status == "running"
        assert restored.run_count == 5
        assert restored.fail_count == 2
        assert restored.last_error == "timeout"
        assert restored.last_run_at == 1234567890.0


# ========== utils 测试 ==========


class TestUtils:
    def test_calc_shard_id(self):
        assert calc_shard_id(10001, 128) == 10001 % 128
        assert calc_shard_id(0, 128) == 0

    def test_calc_stable_jitter(self):
        j1 = calc_stable_jitter(10001, 60)
        j2 = calc_stable_jitter(10001, 60)
        assert j1 == j2  # 稳定
        assert 0 <= j1 < 60
        assert calc_stable_jitter(100, 0) == 0

    def test_calc_next_fire(self):
        base = 1715234400.0  # 固定时间
        nf = calc_next_fire("* * * * *", base)
        assert nf > base
        assert nf <= base + 61  # 每分钟触发


# ========== shard 测试 (需 fakeredis) ==========


class TestShardManager:
    @pytest.mark.asyncio
    async def test_acquire_and_renew(self, fake_redis):
        """获取锁后应能续约。"""
        from redis_cron.shard import ShardManager

        mgr = ShardManager(fake_redis, "node-1", shard_count=4, lock_ttl=15)

        token = await mgr.try_acquire(0)
        assert token is not None
        assert 0 in mgr.my_shards

        ok = await mgr.renew(0)
        assert ok is True

    @pytest.mark.asyncio
    async def test_acquire_conflict(self, fake_redis):
        """已有锁时其他节点无法获取。"""
        from redis_cron.shard import ShardManager

        mgr1 = ShardManager(fake_redis, "node-1", shard_count=4)
        mgr2 = ShardManager(fake_redis, "node-2", shard_count=4)

        token1 = await mgr1.try_acquire(0)
        assert token1 is not None

        token2 = await mgr2.try_acquire(0)
        assert token2 is None

    @pytest.mark.asyncio
    async def test_heartbeat_renews(self, fake_redis):
        """心跳应续约所有锁。"""
        from redis_cron.shard import ShardManager

        mgr = ShardManager(fake_redis, "node-1", shard_count=4)
        await mgr.try_acquire(0)
        await mgr.try_acquire(1)

        lost = await mgr.heartbeat()
        assert lost == []
        assert len(mgr.my_shards) == 2


# ========== Lua 脚本测试 (需 fakeredis[lua]) ==========


class TestLuaScripts:
    @pytest.mark.asyncio
    async def test_fetch_due_tasks(self, fake_redis):
        """Lua 原子取任务脚本应正确工作。"""
        from redis_cron import lua_scripts

        shard_id = 0
        # 设置锁
        await fake_redis.set(f"shard_lock:{shard_id}", "node-1:1")

        # 添加到期任务
        now = time.time()
        await fake_redis.zadd(f"trigger:shard_{shard_id}", {"task_a": now - 10, "task_b": now - 5})

        result_raw = await fake_redis.eval(
            lua_scripts.FETCH_DUE_TASKS, 3,
            f"shard_lock:{shard_id}",
            f"trigger:shard_{shard_id}",
            f"processing:shard_{shard_id}",
            "node-1:1", str(now), "10",
        )
        result = json.loads(result_raw)
        tasks = result["tasks"]
        assert len(tasks) == 2

        # 验证已从 ZSET 移除
        remaining = await fake_redis.zcard(f"trigger:shard_{shard_id}")
        assert remaining == 0

        # 验证已加入 processing
        proc = await fake_redis.hgetall(f"processing:shard_{shard_id}")
        assert len(proc) == 2

    @pytest.mark.asyncio
    async def test_fetch_not_owner(self, fake_redis):
        """非所有者调用应返回 NOT_OWNER。"""
        from redis_cron import lua_scripts

        await fake_redis.set("shard_lock:0", "node-1:1")

        result_raw = await fake_redis.eval(
            lua_scripts.FETCH_DUE_TASKS, 3,
            "shard_lock:0", "trigger:shard_0", "processing:shard_0",
            "node-2:2", str(time.time()), "10",
        )
        result = json.loads(result_raw)
        assert result["error"] == "NOT_OWNER"

    @pytest.mark.asyncio
    async def test_ack_task_with_next_fire(self, fake_redis):
        """ACK 应从 processing 移除并放回触发队列。"""
        from redis_cron import lua_scripts

        await fake_redis.hset("processing:shard_0", "task_a", str(time.time()))

        next_fire = time.time() + 3600
        await fake_redis.eval(
            lua_scripts.ACK_TASK, 2,
            "processing:shard_0", "trigger:shard_0",
            "task_a", str(next_fire),
        )

        # processing 中应已移除
        exists = await fake_redis.hexists("processing:shard_0", "task_a")
        assert not exists

        # 应在触发队列中
        score = await fake_redis.zscore("trigger:shard_0", "task_a")
        assert score is not None

    @pytest.mark.asyncio
    async def test_ack_task_no_next(self, fake_redis):
        """ACK 且 next_fire=0 时不应放回触发队列。"""
        from redis_cron import lua_scripts

        await fake_redis.hset("processing:shard_0", "task_b", str(time.time()))

        await fake_redis.eval(
            lua_scripts.ACK_TASK, 2,
            "processing:shard_0", "trigger:shard_0",
            "task_b", "0",
        )

        exists = await fake_redis.hexists("processing:shard_0", "task_b")
        assert not exists

        score = await fake_redis.zscore("trigger:shard_0", "task_b")
        assert score is None


# ========== Worker 测试 ==========


class TestWorker:
    @pytest.mark.asyncio
    async def test_execute_task_dedup(self, fake_redis):
        """同一任务同一触发时间只执行一次。"""
        from redis_cron.worker import Worker

        call_count = 0

        async def handler(task_id: str, payload: dict):
            nonlocal call_count
            call_count += 1

        worker = Worker(fake_redis, shard_count=4)
        worker.register("test_type", handler)

        # 创建任务
        await fake_redis.hset("task:t1", mapping={
            "task_type": "test_type",
            "payload": "{}",
            "user_id": "0",
            "shard_id": "0",
            "fire_time": "0",
            "created_at": "0",
            "max_jitter": "0",
            "is_cron": "0",
            "status": "active",
            "max_retries": "0",
            "retry_count": "0",
            "retry_delay": "60",
            "last_run_at": "0",
            "run_count": "0",
            "fail_count": "0",
            "last_error": "",
        })
        await fake_redis.hset("processing:shard_0", "t1", str(time.time()))

        fire_time = time.time()
        ok1 = await worker.execute_task("t1", 0, fire_time)
        assert ok1 is True

        ok2 = await worker.execute_task("t1", 0, fire_time)
        assert ok2 is False  # 去重

        assert call_count == 1

    @pytest.mark.asyncio
    async def test_execute_task_status_transitions(self, fake_redis):
        """执行任务应正确更新状态。"""
        from redis_cron.worker import Worker

        async def handler(task_id: str, payload: dict):
            # 检查执行期间状态是 running
            status = await fake_redis.hget("task:t_status", "status")
            s = status.decode() if isinstance(status, bytes) else status
            assert s == "running"

        worker = Worker(fake_redis, shard_count=4)
        worker.register("test_type", handler)

        await fake_redis.hset("task:t_status", mapping={
            "task_type": "test_type",
            "payload": "{}",
            "user_id": "0",
            "shard_id": "0",
            "fire_time": "0",
            "created_at": "0",
            "max_jitter": "0",
            "is_cron": "0",
            "status": "active",
            "max_retries": "0",
            "retry_count": "0",
            "retry_delay": "60",
            "last_run_at": "0",
            "run_count": "0",
            "fail_count": "0",
            "last_error": "",
        })
        await fake_redis.hset("processing:shard_0", "t_status", str(time.time()))

        ok = await worker.execute_task("t_status", 0, time.time())
        assert ok is True

        # 执行完成后应为 completed（非 cron 任务）
        status = await fake_redis.hget("task:t_status", "status")
        s = status.decode() if isinstance(status, bytes) else status
        assert s == "completed"

        run_count = await fake_redis.hget("task:t_status", "run_count")
        assert int(run_count) == 1

    @pytest.mark.asyncio
    async def test_execute_task_failure_and_retry(self, fake_redis):
        """失败任务应重试并更新状态。"""
        from redis_cron.worker import Worker

        call_count = 0

        async def failing_handler(task_id: str, payload: dict):
            nonlocal call_count
            call_count += 1
            raise ValueError("模拟失败")

        worker = Worker(fake_redis, shard_count=4)
        worker.register("fail_type", failing_handler)

        await fake_redis.hset("task:t_retry", mapping={
            "task_type": "fail_type",
            "payload": "{}",
            "user_id": "0",
            "shard_id": "0",
            "fire_time": "0",
            "created_at": "0",
            "max_jitter": "0",
            "is_cron": "0",
            "status": "active",
            "max_retries": "2",
            "retry_count": "0",
            "retry_delay": "10",
            "last_run_at": "0",
            "run_count": "0",
            "fail_count": "0",
            "last_error": "",
        })
        await fake_redis.hset("processing:shard_0", "t_retry", str(time.time()))

        # 第一次失败 — 应设置重试
        ok = await worker.execute_task("t_retry", 0, time.time())
        assert ok is False
        assert call_count == 1

        # 检查重试计数
        retry_count = await fake_redis.hget("task:t_retry", "retry_count")
        assert int(retry_count) == 1

        # 任务应在 ZSET 中（准备重试）
        score = await fake_redis.zscore("trigger:shard_0", "t_retry")
        assert score is not None

        # 状态应为 active（等待重试）
        status = await fake_redis.hget("task:t_retry", "status")
        s = status.decode() if isinstance(status, bytes) else status
        assert s == "active"

    @pytest.mark.asyncio
    async def test_execute_task_max_retries_exceeded(self, fake_redis):
        """超过最大重试次数应标记为 failed。"""
        from redis_cron.worker import Worker

        async def failing_handler(task_id: str, payload: dict):
            raise ValueError("always fails")

        worker = Worker(fake_redis, shard_count=4)
        worker.register("fail_type", failing_handler)

        await fake_redis.hset("task:t_maxretry", mapping={
            "task_type": "fail_type",
            "payload": "{}",
            "user_id": "0",
            "shard_id": "0",
            "fire_time": "0",
            "created_at": "0",
            "max_jitter": "0",
            "is_cron": "0",
            "status": "active",
            "max_retries": "1",
            "retry_count": "1",  # 已重试过一次
            "retry_delay": "10",
            "last_run_at": "0",
            "run_count": "1",
            "fail_count": "1",
            "last_error": "",
        })
        await fake_redis.hset("processing:shard_0", "t_maxretry", str(time.time()))

        ok = await worker.execute_task("t_maxretry", 0, time.time())
        assert ok is False

        status = await fake_redis.hget("task:t_maxretry", "status")
        s = status.decode() if isinstance(status, bytes) else status
        assert s == "failed"

    @pytest.mark.asyncio
    async def test_execution_history(self, fake_redis):
        """执行后应记录历史。"""
        from redis_cron.worker import Worker

        async def handler(task_id: str, payload: dict):
            pass

        worker = Worker(fake_redis, shard_count=4)
        worker.register("hist_type", handler)

        await fake_redis.hset("task:t_hist", mapping={
            "task_type": "hist_type",
            "payload": "{}",
            "user_id": "0",
            "shard_id": "0",
            "fire_time": "0",
            "created_at": "0",
            "max_jitter": "0",
            "is_cron": "0",
            "status": "active",
            "max_retries": "0",
            "retry_count": "0",
            "retry_delay": "60",
            "last_run_at": "0",
            "run_count": "0",
            "fail_count": "0",
            "last_error": "",
        })
        await fake_redis.hset("processing:shard_0", "t_hist", str(time.time()))

        await worker.execute_task("t_hist", 0, time.time())

        # 检查历史记录
        entries = await fake_redis.lrange("task_history:t_hist", 0, -1)
        assert len(entries) == 1
        entry = json.loads(entries[0])
        assert entry["status"] == "success"
        assert "duration_ms" in entry


# ========== Scheduler 高级功能测试 ==========


class TestSchedulerAdvanced:
    """测试 RedisScheduler 的高级功能（trigger, history, bulk, list_by_user）。"""

    async def _make_scheduler(self, fake_redis):
        """创建一个使用 fakeredis 的调度器。"""
        from redis_cron.scheduler import RedisScheduler

        s = RedisScheduler(redis_url="redis://fake", shard_count=4)
        s._redis = fake_redis
        return s

    @pytest.mark.asyncio
    async def test_trigger_task(self, fake_redis):
        """手动触发应将任务 score 设为 0。"""
        s = await self._make_scheduler(fake_redis)

        tid = await s.create_delayed_task("test", delay_seconds=9999, user_id=1, task_id="trig1")
        assert tid == "trig1"

        # 原始 score 应该很大
        shard_id = calc_shard_id(1, 4)
        score_before = await fake_redis.zscore(f"trigger:shard_{shard_id}", "trig1")
        assert score_before > 1000

        ok = await s.trigger_task("trig1")
        assert ok is True

        score_after = await fake_redis.zscore(f"trigger:shard_{shard_id}", "trig1")
        assert score_after == 0

    @pytest.mark.asyncio
    async def test_trigger_paused_task(self, fake_redis):
        """暂停的任务不能手动触发。"""
        s = await self._make_scheduler(fake_redis)

        tid = await s.create_delayed_task("test", delay_seconds=100, user_id=2, task_id="trig_p")
        await s.pause_task(tid)

        ok = await s.trigger_task(tid)
        assert ok is False

    @pytest.mark.asyncio
    async def test_trigger_nonexistent(self, fake_redis):
        """触发不存在的任务应返回 False。"""
        s = await self._make_scheduler(fake_redis)
        ok = await s.trigger_task("no_exist")
        assert ok is False

    @pytest.mark.asyncio
    async def test_pause_resume_status(self, fake_redis):
        """暂停和恢复应更新 status 字段。"""
        s = await self._make_scheduler(fake_redis)

        tid = await s.create_delayed_task("test", delay_seconds=100, user_id=3, task_id="pr1")

        # 暂停
        await s.pause_task(tid)
        task = await s.get_task(tid)
        assert task.status == "paused"

        # 恢复
        await s.resume_task(tid)
        task = await s.get_task(tid)
        assert task.status == "active"

    @pytest.mark.asyncio
    async def test_get_task_history(self, fake_redis):
        """get_task_history 应返回历史记录。"""
        s = await self._make_scheduler(fake_redis)

        # 手动插入历史
        for i in range(5):
            entry = json.dumps({"fire_time": i, "status": "success", "duration_ms": 10, "error": None})
            await fake_redis.lpush("task_history:hist_test", entry)

        history = await s.get_task_history("hist_test", limit=3)
        assert len(history) == 3
        assert history[0]["fire_time"] == 4  # 最新的在前

    @pytest.mark.asyncio
    async def test_list_tasks_by_user(self, fake_redis):
        """按用户查询应返回该用户的所有任务。"""
        s = await self._make_scheduler(fake_redis)

        await s.create_delayed_task("type_a", delay_seconds=100, user_id=42, task_id="u42_1")
        await s.create_delayed_task("type_b", delay_seconds=100, user_id=42, task_id="u42_2")
        await s.create_delayed_task("type_a", delay_seconds=100, user_id=99, task_id="u99_1")

        tasks = await s.list_tasks_by_user(42)
        assert len(tasks) == 2
        task_ids = {t.task_id for t in tasks}
        assert task_ids == {"u42_1", "u42_2"}

        # 按类型过滤
        tasks_a = await s.list_tasks_by_user(42, task_type="type_a")
        assert len(tasks_a) == 1
        assert tasks_a[0].task_id == "u42_1"

    @pytest.mark.asyncio
    async def test_list_tasks_by_user_includes_paused(self, fake_redis):
        """按用户查询应包含暂停的任务。"""
        s = await self._make_scheduler(fake_redis)

        await s.create_delayed_task("test", delay_seconds=100, user_id=50, task_id="u50_1")
        await s.create_delayed_task("test", delay_seconds=100, user_id=50, task_id="u50_2")
        await s.pause_task("u50_2")

        tasks = await s.list_tasks_by_user(50)
        assert len(tasks) == 2

    @pytest.mark.asyncio
    async def test_bulk_delete(self, fake_redis):
        """批量删除应删除多个任务。"""
        s = await self._make_scheduler(fake_redis)

        await s.create_delayed_task("test", delay_seconds=100, user_id=1, task_id="bd1")
        await s.create_delayed_task("test", delay_seconds=100, user_id=1, task_id="bd2")
        await s.create_delayed_task("test", delay_seconds=100, user_id=1, task_id="bd3")

        count = await s.bulk_delete_tasks(["bd1", "bd2", "nonexist"])
        assert count == 2

        assert await s.get_task("bd1") is None
        assert await s.get_task("bd2") is None
        assert await s.get_task("bd3") is not None

    @pytest.mark.asyncio
    async def test_bulk_pause_resume(self, fake_redis):
        """批量暂停和恢复。"""
        s = await self._make_scheduler(fake_redis)

        await s.create_delayed_task("test", delay_seconds=100, user_id=1, task_id="bp1")
        await s.create_delayed_task("test", delay_seconds=100, user_id=1, task_id="bp2")

        paused = await s.bulk_pause_tasks(["bp1", "bp2"])
        assert paused == 2

        t1 = await s.get_task("bp1")
        assert t1.status == "paused"

        resumed = await s.bulk_resume_tasks(["bp1", "bp2"])
        assert resumed == 2

        t1 = await s.get_task("bp1")
        assert t1.status == "active"

    @pytest.mark.asyncio
    async def test_create_with_retry_params(self, fake_redis):
        """创建任务时应支持重试参数。"""
        s = await self._make_scheduler(fake_redis)

        tid = await s.create_cron_task(
            "test", "* * * * *", user_id=1, task_id="retry_task",
            max_retries=3, retry_delay=30,
        )
        task = await s.get_task(tid)
        assert task.max_retries == 3
        assert task.retry_delay == 30
        assert task.status == "active"

    @pytest.mark.asyncio
    async def test_user_tasks_index_on_create(self, fake_redis):
        """创建任务时应添加到 user_tasks 索引。"""
        s = await self._make_scheduler(fake_redis)

        await s.create_cron_task("test", "* * * * *", user_id=7, task_id="idx1")
        await s.create_delayed_task("test", delay_seconds=100, user_id=7, task_id="idx2")

        members = await fake_redis.smembers("user_tasks:7")
        ids = {m.decode() if isinstance(m, bytes) else m for m in members}
        assert ids == {"idx1", "idx2"}

    @pytest.mark.asyncio
    async def test_delete_cleans_user_index(self, fake_redis):
        """删除任务应从 user_tasks 索引移除。"""
        s = await self._make_scheduler(fake_redis)

        await s.create_delayed_task("test", delay_seconds=100, user_id=8, task_id="del_idx")
        await s.delete_task("del_idx")

        members = await fake_redis.smembers("user_tasks:8")
        assert len(members) == 0
