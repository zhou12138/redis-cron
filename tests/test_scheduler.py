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

        restored = Task.from_redis("abc123", data)
        assert restored.task_id == "abc123"
        assert restored.task_type == "send_email"
        assert restored.payload == {"to": "test@example.com"}
        assert restored.user_id == 10001

    def test_cron_task_roundtrip(self):
        """CronTask 序列化和反序列化应保持一致。"""
        t = CronTask(
            task_id="cron001",
            task_type="daily_report",
            cron="0 8 * * *",
            user_id=10001,
            shard_id=17,
            payload={"subject": "报表"},
        )
        data = t.to_redis()
        assert data["is_cron"] == "1"
        assert data["cron"] == "0 8 * * *"

        restored = CronTask.from_redis("cron001", data)
        assert restored.cron == "0 8 * * *"
        assert restored.task_type == "daily_report"


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
        })
        await fake_redis.hset("processing:shard_0", "t1", str(time.time()))

        fire_time = time.time()
        ok1 = await worker.execute_task("t1", 0, fire_time)
        assert ok1 is True

        ok2 = await worker.execute_task("t1", 0, fire_time)
        assert ok2 is False  # 去重

        assert call_count == 1
