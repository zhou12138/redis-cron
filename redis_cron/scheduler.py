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
    ) -> str:
        """创建周期性 Cron 任务。

        Args:
            task_type: 任务类型
            cron: Cron 表达式
            user_id: 用户 ID（用于分片和 jitter）
            payload: 任务载荷
            max_jitter: 最大打散秒数
            task_id: 自定义任务 ID，不提供则自动生成

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
        )

        pipe = r.pipeline()
        pipe.hset(f"task:{tid}", mapping=task.to_redis())
        pipe.zadd(f"trigger:shard_{shard_id}", {tid: fire_time})
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
    ) -> str:
        """创建一次性延迟任务。

        Args:
            task_type: 任务类型
            delay_seconds: 延迟秒数
            user_id: 用户 ID
            payload: 任务载荷
            task_id: 自定义任务 ID

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
        )

        pipe = r.pipeline()
        pipe.hset(f"task:{tid}", mapping=task.to_redis())
        pipe.zadd(f"trigger:shard_{shard_id}", {tid: fire_time})
        await pipe.execute()

        logger.info("创建延迟任务 %s, shard=%d, fire_time=%.0f", tid, shard_id, fire_time)
        return tid

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
        self._worker = Worker(r, self._shard_count, self._dedup_ttl, self._task_timeout)

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
