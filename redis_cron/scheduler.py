"""SchedulerNode：分片管理、扫描循环、心跳、补偿扫描。"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from typing import Any, Callable, Coroutine

import redis.asyncio as aioredis

from . import lua_scripts
from .models import CronTask, Task
from .shard import ShardManager
from .utils import calc_next_fire, calc_shard_id, calc_stable_jitter
from .worker import TaskHandler, Worker

logger = logging.getLogger(__name__)


class RedisScheduler:
    """分布式定时任务调度器，同时支持 Scheduler 和 Worker 角色。"""

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        shard_count: int = 128,
        node_id: str | None = None,
        lock_ttl: int = 15,
        batch_size: int = 200,
        scan_interval: float = 0.1,
        heartbeat_interval: float = 5.0,
        scavenge_interval: float = 10.0,
        recover_interval: float = 60.0,
        processing_timeout: float = 60.0,
        task_timeout: int = 60,
        dedup_ttl: int = 3600,
    ):
        """初始化调度器。

        Args:
            redis_url: Redis 连接地址
            shard_count: 分片总数
            node_id: 节点 ID，不提供则自动生成 UUID
            lock_ttl: 分片锁 TTL（秒）
            batch_size: 每次取任务的批量大小
            scan_interval: 无任务时的扫描间隔（秒）
            heartbeat_interval: 心跳间隔（秒）
            scavenge_interval: 扫描无主 shard 间隔（秒）
            recover_interval: 补偿扫描间隔（秒）
            processing_timeout: processing 中任务的超时时间（秒）
            task_timeout: 单个任务执行超时（秒）
            dedup_ttl: 去重 key 过期时间（秒）
        """
        self._redis_url = redis_url
        self._shard_count = shard_count
        self._node_id = node_id or uuid.uuid4().hex[:12]
        self._batch_size = batch_size
        self._scan_interval = scan_interval
        self._heartbeat_interval = heartbeat_interval
        self._scavenge_interval = scavenge_interval
        self._recover_interval = recover_interval
        self._processing_timeout = processing_timeout
        self._running = False
        self._redis: aioredis.Redis | None = None
        self._shard_mgr: ShardManager | None = None
        self._worker: Worker | None = None
        self._lock_ttl = lock_ttl
        self._task_timeout = task_timeout
        self._dedup_ttl = dedup_ttl
        self._max_history = 100
        self._shard_tasks: dict[int, asyncio.Task] = {}

    # ========== 任务处理器注册 ==========

    def task(self, task_type: str) -> Callable:
        """装饰器：注册任务处理器。

        用法::

            @scheduler.task("send_email")
            async def send_email(task_id: str, payload: dict):
                ...
        """
        def decorator(func: TaskHandler) -> TaskHandler:
            if self._worker is None:
                # 延迟注册，先存到临时字典
                if not hasattr(self, "_pending_handlers"):
                    self._pending_handlers: dict[str, TaskHandler] = {}
                self._pending_handlers[task_type] = func
            else:
                self._worker.register(task_type, func)
            return func
        return decorator

    # ========== 连接管理 ==========

    async def _ensure_redis(self) -> aioredis.Redis:
        """确保 Redis 连接可用。"""
        if self._redis is None:
            self._redis = aioredis.from_url(self._redis_url, decode_responses=False)
        return self._redis

    # ========== 任务创建 ==========

    async def create_cron_task(
        self,
        task_type: str,
        cron: str,
        user_id: int = 0,
        payload: dict[str, Any] | None = None,
        max_jitter: int = 0,
        task_id: str | None = None,
        max_retries: int = 0,
        retry_delay: int = 60,
    ) -> str:
        """创建周期性 Cron 任务。

        Args:
            task_type: 任务类型
            cron: Cron 表达式
            user_id: 用户 ID（用于分片和 jitter）
            payload: 任务载荷
            max_jitter: 最大打散秒数
            task_id: 自定义任务 ID，不提供则自动生成
            max_retries: 最大重试次数，默认 0 不重试
            retry_delay: 重试间隔基数（秒），默认 60

        Returns:
            任务 ID
        """
        r = await self._ensure_redis()
        shard_id = calc_shard_id(user_id, self._shard_count)
        tid = task_id or uuid.uuid4().hex

        next_fire = calc_next_fire(cron)
        jitter = calc_stable_jitter(user_id, max_jitter)
        fire_time = next_fire + jitter

        task = CronTask(
            task_id=tid,
            task_type=task_type,
            cron=cron,
            user_id=user_id,
            shard_id=shard_id,
            payload=payload or {},
            fire_time=fire_time,
            max_jitter=max_jitter,
            status="active",
            max_retries=max_retries,
            retry_delay=retry_delay,
        )

        pipe = r.pipeline()
        pipe.hset(f"task:{tid}", mapping=task.to_redis())
        pipe.zadd(f"trigger:shard_{shard_id}", {tid: fire_time})
        pipe.sadd(f"user_tasks:{user_id}", tid)
        await pipe.execute()

        logger.info("创建 Cron 任务 %s, shard=%d, next_fire=%.0f", tid, shard_id, fire_time)
        return tid

    async def create_delayed_task(
        self,
        task_type: str,
        delay_seconds: float,
        user_id: int = 0,
        payload: dict[str, Any] | None = None,
        task_id: str | None = None,
        max_retries: int = 0,
        retry_delay: int = 60,
    ) -> str:
        """创建一次性延迟任务。

        Args:
            task_type: 任务类型
            delay_seconds: 延迟秒数
            user_id: 用户 ID
            payload: 任务载荷
            task_id: 自定义任务 ID
            max_retries: 最大重试次数，默认 0 不重试
            retry_delay: 重试间隔基数（秒），默认 60

        Returns:
            任务 ID
        """
        r = await self._ensure_redis()
        shard_id = calc_shard_id(user_id, self._shard_count)
        tid = task_id or uuid.uuid4().hex
        fire_time = time.time() + delay_seconds

        task = Task(
            task_id=tid,
            task_type=task_type,
            user_id=user_id,
            shard_id=shard_id,
            payload=payload or {},
            fire_time=fire_time,
            status="active",
            max_retries=max_retries,
            retry_delay=retry_delay,
        )

        pipe = r.pipeline()
        pipe.hset(f"task:{tid}", mapping=task.to_redis())
        pipe.zadd(f"trigger:shard_{shard_id}", {tid: fire_time})
        pipe.sadd(f"user_tasks:{user_id}", tid)
        await pipe.execute()

        logger.info("创建延迟任务 %s, shard=%d, fire_time=%.0f", tid, shard_id, fire_time)
        return tid

    # ========== 任务 CRUD ==========

    async def get_task(self, task_id: str) -> Task | CronTask | None:
        """获取任务详情。

        Args:
            task_id: 任务 ID

        Returns:
            Task 或 CronTask 对象，不存在返回 None
        """
        r = await self._ensure_redis()
        data = await r.hgetall(f"task:{task_id}")
        if not data:
            return None

        is_cron = self._get_field(data, "is_cron") == "1"
        if is_cron:
            return CronTask.from_redis(task_id, data)
        return Task.from_redis(task_id, data)

    async def delete_task(self, task_id: str) -> bool:
        """删除任务。

        从 ZSET 触发队列、processing 集合和任务详情中彻底移除。

        Args:
            task_id: 任务 ID

        Returns:
            True 表示任务存在并已删除，False 表示任务不存在
        """
        r = await self._ensure_redis()
        data = await r.hgetall(f"task:{task_id}")
        if not data:
            return False

        shard_id = int(self._get_field(data, "shard_id") or "0")
        user_id = int(self._get_field(data, "user_id") or "0")

        pipe = r.pipeline()
        pipe.delete(f"task:{task_id}")
        pipe.zrem(f"trigger:shard_{shard_id}", task_id)
        pipe.hdel(f"processing:shard_{shard_id}", task_id)
        pipe.srem(f"user_tasks:{user_id}", task_id)
        pipe.delete(f"task_history:{task_id}")
        await pipe.execute()

        logger.info("删除任务 %s, shard=%d", task_id, shard_id)
        return True

    async def update_task(
        self,
        task_id: str,
        *,
        cron: str | None = None,
        payload: dict[str, Any] | None = None,
        max_jitter: int | None = None,
    ) -> bool:
        """更新已有任务的属性。

        仅支持更新 cron 表达式、payload 和 max_jitter。
        更新 cron 会自动重新计算下次触发时间。

        Args:
            task_id: 任务 ID
            cron: 新的 Cron 表达式（仅 CronTask 有效）
            payload: 新的任务载荷
            max_jitter: 新的最大打散秒数

        Returns:
            True 表示更新成功，False 表示任务不存在
        """
        r = await self._ensure_redis()
        data = await r.hgetall(f"task:{task_id}")
        if not data:
            return False

        shard_id = int(self._get_field(data, "shard_id") or "0")
        user_id = int(self._get_field(data, "user_id") or "0")
        is_cron = self._get_field(data, "is_cron") == "1"

        updates: dict[str, str] = {}
        new_fire_time: float | None = None

        if payload is not None:
            updates["payload"] = json.dumps(payload, ensure_ascii=False)

        if max_jitter is not None:
            updates["max_jitter"] = str(max_jitter)

        if cron is not None and is_cron:
            updates["cron"] = cron
            jitter_val = max_jitter if max_jitter is not None else int(self._get_field(data, "max_jitter") or "0")
            next_fire = calc_next_fire(cron)
            jitter = calc_stable_jitter(user_id, jitter_val)
            new_fire_time = next_fire + jitter
            updates["fire_time"] = str(new_fire_time)

        if not updates:
            return True  # 没有需要更新的字段

        pipe = r.pipeline()
        pipe.hset(f"task:{task_id}", mapping=updates)
        if new_fire_time is not None:
            pipe.zadd(f"trigger:shard_{shard_id}", {task_id: new_fire_time})
        await pipe.execute()

        logger.info("更新任务 %s, fields=%s", task_id, list(updates.keys()))
        return True

    async def pause_task(self, task_id: str) -> bool:
        """暂停任务（从触发队列移除，但保留任务数据）。

        Args:
            task_id: 任务 ID

        Returns:
            True 表示暂停成功，False 表示任务不存在
        """
        r = await self._ensure_redis()
        data = await r.hgetall(f"task:{task_id}")
        if not data:
            return False

        shard_id = int(self._get_field(data, "shard_id") or "0")

        pipe = r.pipeline()
        pipe.hset(f"task:{task_id}", mapping={"paused": "1", "status": "paused"})
        pipe.zrem(f"trigger:shard_{shard_id}", task_id)
        await pipe.execute()

        logger.info("暂停任务 %s", task_id)
        return True

    async def resume_task(self, task_id: str) -> bool:
        """恢复已暂停的任务（重新放入触发队列）。

        Args:
            task_id: 任务 ID

        Returns:
            True 表示恢复成功，False 表示任务不存在
        """
        r = await self._ensure_redis()
        data = await r.hgetall(f"task:{task_id}")
        if not data:
            return False

        shard_id = int(self._get_field(data, "shard_id") or "0")
        is_cron = self._get_field(data, "is_cron") == "1"

        if is_cron:
            cron_expr = self._get_field(data, "cron")
            user_id = int(self._get_field(data, "user_id") or "0")
            max_jitter_val = int(self._get_field(data, "max_jitter") or "0")
            next_fire = calc_next_fire(cron_expr)
            jitter = calc_stable_jitter(user_id, max_jitter_val)
            fire_time = next_fire + jitter
        else:
            fire_time = float(self._get_field(data, "fire_time") or "0")

        pipe = r.pipeline()
        pipe.hset(f"task:{task_id}", mapping={"paused": "0", "status": "active"})
        pipe.zadd(f"trigger:shard_{shard_id}", {task_id: fire_time})
        await pipe.execute()

        logger.info("恢复任务 %s", task_id)
        return True

    async def list_tasks(
        self,
        shard_id: int | None = None,
        task_type: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Task | CronTask]:
        """列出任务。

        Args:
            shard_id: 按分片过滤（不传则扫描所有分片）
            task_type: 按任务类型过滤
            limit: 返回数量上限
            offset: 跳过前 N 条

        Returns:
            任务列表
        """
        r = await self._ensure_redis()
        shards = [shard_id] if shard_id is not None else range(self._shard_count)

        results: list[Task | CronTask] = []
        skipped = 0

        for sid in shards:
            task_ids = await r.zrange(f"trigger:shard_{sid}", 0, -1)
            for raw_tid in task_ids:
                tid = raw_tid.decode() if isinstance(raw_tid, bytes) else raw_tid
                data = await r.hgetall(f"task:{tid}")
                if not data:
                    continue

                is_cron = self._get_field(data, "is_cron") == "1"
                task = CronTask.from_redis(tid, data) if is_cron else Task.from_redis(tid, data)

                if task_type and task.task_type != task_type:
                    continue

                if skipped < offset:
                    skipped += 1
                    continue

                results.append(task)
                if len(results) >= limit:
                    return results

        return results

    async def count_tasks(self, shard_id: int | None = None) -> int:
        """统计任务数量。

        Args:
            shard_id: 按分片统计（不传则统计所有分片）

        Returns:
            任务总数
        """
        r = await self._ensure_redis()
        shards = [shard_id] if shard_id is not None else range(self._shard_count)
        total = 0
        for sid in shards:
            total += await r.zcard(f"trigger:shard_{sid}")
        return total

    # ========== 手动触发 ==========

    async def trigger_task(self, task_id: str) -> bool:
        """手动立即触发任务，无视其调度时间。

        Args:
            task_id: 任务 ID

        Returns:
            True 表示触发成功，False 表示任务不存在
        """
        r = await self._ensure_redis()
        data = await r.hgetall(f"task:{task_id}")
        if not data:
            return False

        shard_id = int(self._get_field(data, "shard_id") or "0")
        status = self._get_field(data, "status") or "active"

        if status == "paused":
            return False

        # score=0 表示立即触发
        await r.zadd(f"trigger:shard_{shard_id}", {task_id: 0})
        logger.info("手动触发任务 %s", task_id)
        return True

    # ========== 执行历史 ==========

    async def get_task_history(self, task_id: str, limit: int = 10) -> list[dict]:
        """获取任务执行历史。

        Args:
            task_id: 任务 ID
            limit: 返回记录数上限，默认 10

        Returns:
            执行历史记录列表，每条包含 fire_time, status, duration_ms, error
        """
        r = await self._ensure_redis()
        raw_entries = await r.lrange(f"task_history:{task_id}", 0, limit - 1)
        results = []
        for entry in raw_entries:
            s = entry.decode() if isinstance(entry, bytes) else entry
            results.append(json.loads(s))
        return results

    # ========== 按用户查询 ==========

    async def list_tasks_by_user(
        self,
        user_id: int,
        task_type: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Task | CronTask]:
        """列出指定用户的所有任务。

        使用 user_tasks:{user_id} 二级索引，包含已暂停的任务。

        Args:
            user_id: 用户 ID
            task_type: 按任务类型过滤
            limit: 返回数量上限
            offset: 跳过前 N 条

        Returns:
            任务列表
        """
        r = await self._ensure_redis()
        task_ids = await r.smembers(f"user_tasks:{user_id}")

        results: list[Task | CronTask] = []
        skipped = 0

        for raw_tid in task_ids:
            tid = raw_tid.decode() if isinstance(raw_tid, bytes) else raw_tid
            data = await r.hgetall(f"task:{tid}")
            if not data:
                continue

            is_cron = self._get_field(data, "is_cron") == "1"
            task = CronTask.from_redis(tid, data) if is_cron else Task.from_redis(tid, data)

            if task_type and task.task_type != task_type:
                continue

            if skipped < offset:
                skipped += 1
                continue

            results.append(task)
            if len(results) >= limit:
                return results

        return results

    # ========== 批量操作 ==========

    async def bulk_delete_tasks(self, task_ids: list[str]) -> int:
        """批量删除任务。

        Args:
            task_ids: 任务 ID 列表

        Returns:
            成功删除的任务数
        """
        r = await self._ensure_redis()
        deleted = 0
        for task_id in task_ids:
            ok = await self.delete_task(task_id)
            if ok:
                deleted += 1
        return deleted

    async def bulk_pause_tasks(self, task_ids: list[str]) -> int:
        """批量暂停任务。

        Args:
            task_ids: 任务 ID 列表

        Returns:
            成功暂停的任务数
        """
        r = await self._ensure_redis()
        paused = 0
        for task_id in task_ids:
            ok = await self.pause_task(task_id)
            if ok:
                paused += 1
        return paused

    async def bulk_resume_tasks(self, task_ids: list[str]) -> int:
        """批量恢复任务。

        Args:
            task_ids: 任务 ID 列表

        Returns:
            成功恢复的任务数
        """
        r = await self._ensure_redis()
        resumed = 0
        for task_id in task_ids:
            ok = await self.resume_task(task_id)
            if ok:
                resumed += 1
        return resumed

    @staticmethod
    def _get_field(data: dict, key: str) -> str:
        """从 Redis HGETALL 结果中取字符串值。"""
        val = data.get(key.encode(), data.get(key, b""))
        return val.decode() if isinstance(val, bytes) else val

    # ========== 启动与停止 ==========

    async def start(self, roles: list[str] | None = None) -> None:
        """启动调度器。

        Args:
            roles: 角色列表，可选 "scheduler" 和/或 "worker"。
                   默认 None 表示同时启动两个角色。
        """
        if roles is None:
            roles = ["scheduler", "worker"]

        r = await self._ensure_redis()
        self._running = True

        self._shard_mgr = ShardManager(r, self._node_id, self._shard_count, self._lock_ttl)
        self._worker = Worker(r, self._shard_count, self._dedup_ttl, self._task_timeout, self._max_history)

        # 注册延迟的处理器
        if hasattr(self, "_pending_handlers"):
            for tt, handler in self._pending_handlers.items():
                self._worker.register(tt, handler)
            del self._pending_handlers

        coroutines: list[Coroutine] = []

        if "scheduler" in roles:
            coroutines.extend([
                self._heartbeat_loop(),
                self._scavenge_loop(),
                self._schedule_loop(),
                self._recover_loop(),
            ])

        if "worker" in roles and "scheduler" not in roles:
            # 纯 worker 模式：监听所有 shard 的 processing 做补偿
            # 实际上 worker 在 scheduler 模式下已经内嵌执行
            coroutines.append(self._standalone_worker_loop())

        await asyncio.gather(*coroutines)

    async def stop(self) -> None:
        """停止调度器。"""
        self._running = False
        # 取消所有 shard 调度任务
        for t in self._shard_tasks.values():
            t.cancel()
        self._shard_tasks.clear()

        # 释放锁
        if self._shard_mgr:
            for sid in list(self._shard_mgr.my_shards.keys()):
                await self._shard_mgr.release(sid)

        if self._redis:
            await self._redis.aclose()
            self._redis = None

    # ========== 内部循环 ==========

    async def _heartbeat_loop(self) -> None:
        """心跳循环：续约节点注册和 shard 锁。"""
        while self._running:
            try:
                assert self._shard_mgr is not None
                lost = await self._shard_mgr.heartbeat()
                for sid in lost:
                    task = self._shard_tasks.pop(sid, None)
                    if task:
                        task.cancel()
            except Exception:
                logger.exception("心跳异常")
            await asyncio.sleep(self._heartbeat_interval)

    async def _scavenge_loop(self) -> None:
        """扫描无主 shard 并接管。"""
        while self._running:
            try:
                assert self._shard_mgr is not None
                await self._shard_mgr.scan_orphan_shards()
            except Exception:
                logger.exception("扫描孤儿 shard 异常")
            await asyncio.sleep(self._scavenge_interval)

    async def _schedule_loop(self) -> None:
        """管理各 shard 的调度协程。"""
        assert self._shard_mgr is not None
        while self._running:
            # 启动新 shard
            for sid in list(self._shard_mgr.my_shards.keys()):
                if sid not in self._shard_tasks:
                    self._shard_tasks[sid] = asyncio.create_task(self._schedule_shard(sid))

            # 清理丢失的 shard
            for sid in list(self._shard_tasks.keys()):
                if sid not in self._shard_mgr.my_shards:
                    self._shard_tasks[sid].cancel()
                    del self._shard_tasks[sid]

            await asyncio.sleep(1)

    async def _schedule_shard(self, shard_id: int) -> None:
        """单个 shard 的调度循环：取到期任务并执行。"""
        assert self._shard_mgr is not None
        assert self._worker is not None
        r = await self._ensure_redis()

        while self._running and shard_id in self._shard_mgr.my_shards:
            try:
                ov = self._shard_mgr.owner_val(shard_id)
                if ov is None:
                    return

                result_raw = await r.eval(
                    lua_scripts.FETCH_DUE_TASKS, 3,
                    f"shard_lock:{shard_id}",
                    f"trigger:shard_{shard_id}",
                    f"processing:shard_{shard_id}",
                    ov, str(time.time()), str(self._batch_size),
                )
                result = json.loads(result_raw)

                if result.get("error") == "NOT_OWNER":
                    logger.warning("shard %d 所有权校验失败，停止调度", shard_id)
                    self._shard_mgr.my_shards.pop(shard_id, None)
                    return

                tasks = result.get("tasks", [])
                fire_time = time.time()

                for task_id in tasks:
                    tid = task_id.decode() if isinstance(task_id, bytes) else task_id
                    # 在当前协程中执行（简化版，生产环境可改为投递到队列）
                    asyncio.create_task(
                        self._worker.execute_task(tid, shard_id, fire_time)
                    )

                if not tasks:
                    await asyncio.sleep(self._scan_interval)
                elif len(tasks) == self._batch_size:
                    await asyncio.sleep(0.01)  # 还有更多，短暂等待防止打满

            except asyncio.CancelledError:
                return
            except Exception:
                logger.exception("shard %d 调度异常", shard_id)
                await asyncio.sleep(1)

    async def _recover_loop(self) -> None:
        """补偿扫描：定期检查 processing 集合中超时的任务，放回 ZSET。"""
        assert self._shard_mgr is not None
        r = await self._ensure_redis()

        while self._running:
            await asyncio.sleep(self._recover_interval)
            try:
                for shard_id in list(self._shard_mgr.my_shards.keys()):
                    await self._recover_stuck_tasks(r, shard_id)
            except Exception:
                logger.exception("补偿扫描异常")

    async def _recover_stuck_tasks(self, r: aioredis.Redis, shard_id: int) -> None:
        """扫描 processing 中超时的任务，放回触发队列。"""
        stuck = await r.hgetall(f"processing:shard_{shard_id}")
        now = time.time()
        recovered = 0

        for raw_tid, raw_ts in stuck.items():
            task_id = raw_tid.decode() if isinstance(raw_tid, bytes) else raw_tid
            ts = float(raw_ts.decode() if isinstance(raw_ts, bytes) else raw_ts)

            if now - ts > self._processing_timeout:
                pipe = r.pipeline()
                pipe.zadd(f"trigger:shard_{shard_id}", {task_id: 0})  # score=0 立即触发
                pipe.hdel(f"processing:shard_{shard_id}", task_id)
                await pipe.execute()
                recovered += 1

        if recovered:
            logger.info("shard %d: 恢复 %d 个卡住的任务", shard_id, recovered)

    async def _standalone_worker_loop(self) -> None:
        """纯 Worker 模式：定期执行补偿扫描。"""
        # 在纯 worker 模式下，不持有 shard 锁，只做补偿
        # 实际生产中这里应该接入 MQ 消费
        while self._running:
            await asyncio.sleep(self._recover_interval)
