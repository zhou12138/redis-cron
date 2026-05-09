"""Worker：消费任务、幂等执行、回调、重试、执行历史。"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, Callable, Coroutine

import redis.asyncio as aioredis

from . import lua_scripts
from .models import CronTask, Task
from .utils import calc_next_fire, calc_stable_jitter

logger = logging.getLogger(__name__)

# 任务处理器类型：async def handler(task_id: str, payload: dict) -> None
TaskHandler = Callable[[str, dict[str, Any]], Coroutine[Any, Any, None]]


class Worker:
    """任务消费者，从 processing 集合取出任务并幂等执行。"""

    def __init__(
        self,
        redis_client: aioredis.Redis,
        shard_count: int,
        dedup_ttl: int = 3600,
        task_timeout: int = 60,
        max_history: int = 100,
    ):
        self._redis = redis_client
        self._shard_count = shard_count
        self._dedup_ttl = dedup_ttl
        self._task_timeout = task_timeout
        self._max_history = max_history
        self._handlers: dict[str, TaskHandler] = {}
        self._running = False

    def register(self, task_type: str, handler: TaskHandler) -> None:
        """注册任务处理器。"""
        self._handlers[task_type] = handler

    async def execute_task(self, task_id: str, shard_id: int, fire_time: float) -> bool:
        """幂等执行单个任务。

        Args:
            task_id: 任务 ID
            shard_id: 分片 ID
            fire_time: 触发时间戳

        Returns:
            是否成功执行（False 表示去重跳过或失败）
        """
        # 幂等去重
        dedup_key = f"dedup:{task_id}:{int(fire_time)}"
        acquired = await self._redis.set(dedup_key, "1", nx=True, ex=self._dedup_ttl)
        if not acquired:
            logger.debug("跳过重复任务 %s", task_id)
            return False

        start_time = time.time()
        error_msg = ""

        try:
            # 读取任务详情
            data = await self._redis.hgetall(f"task:{task_id}")
            if not data:
                logger.warning("任务 %s 不存在", task_id)
                return False

            # 判断是否 cron 任务
            is_cron = self._get_str(data, "is_cron") == "1"
            if is_cron:
                task = CronTask.from_redis(task_id, data)
            else:
                task = Task.from_redis(task_id, data)

            # 查找处理器
            handler = self._handlers.get(task.task_type)
            if handler is None:
                logger.error("未注册任务类型 %s 的处理器", task.task_type)
                await self._redis.delete(dedup_key)
                return False

            # 设置状态为 running
            await self._redis.hset(f"task:{task_id}", "status", "running")

            # 执行
            await asyncio.wait_for(handler(task_id, task.payload), timeout=self._task_timeout)

            # 执行成功
            duration_ms = int((time.time() - start_time) * 1000)

            # ACK：从 processing 移除，cron 任务计算下次触发放回 ZSET
            next_fire = 0.0
            new_status = "completed"
            if is_cron and isinstance(task, CronTask) and task.cron:
                next_fire = calc_next_fire(task.cron, fire_time)
                jitter = calc_stable_jitter(task.user_id, task.max_jitter)
                next_fire += jitter
                # 检查 end_at：如果下次触发超过 end_at，不再调度
                if task.end_at > 0 and next_fire > task.end_at:
                    next_fire = 0.0
                    new_status = "completed"
                else:
                    new_status = "active"

            await self._redis.eval(
                lua_scripts.ACK_TASK, 2,
                f"processing:shard_{shard_id}",
                f"trigger:shard_{shard_id}",
                task_id, str(next_fire),
            )

            # 更新任务统计和状态
            now = time.time()
            pipe = self._redis.pipeline()
            pipe.hset(f"task:{task_id}", mapping={
                "status": new_status,
                "last_run_at": str(now),
                "run_count": str(task.run_count + 1),
                "retry_count": "0",  # 成功后重置重试计数
                "last_error": "",
            })
            # 记录执行历史
            history_entry = json.dumps({
                "fire_time": fire_time,
                "status": "success",
                "duration_ms": duration_ms,
                "error": None,
            }, ensure_ascii=False)
            history_key = f"task_history:{task_id}"
            pipe.lpush(history_key, history_entry)
            pipe.ltrim(history_key, 0, self._max_history - 1)
            await pipe.execute()

            logger.info("任务 %s 执行成功", task_id)
            return True

        except (asyncio.TimeoutError, Exception) as exc:
            if isinstance(exc, asyncio.TimeoutError):
                error_msg = "执行超时"
                logger.error("任务 %s 执行超时", task_id)
            else:
                error_msg = str(exc)
                logger.exception("任务 %s 执行失败", task_id)

            duration_ms = int((time.time() - start_time) * 1000)
            await self._redis.delete(dedup_key)

            # 读取当前任务数据用于重试判断
            data = await self._redis.hgetall(f"task:{task_id}")
            if data:
                is_cron = self._get_str(data, "is_cron") == "1"
                max_retries = int(self._get_str(data, "max_retries") or "0")
                retry_count = int(self._get_str(data, "retry_count") or "0")
                retry_delay = int(self._get_str(data, "retry_delay") or "60")
                run_count = int(self._get_str(data, "run_count") or "0")
                fail_count = int(self._get_str(data, "fail_count") or "0")

                new_retry_count = retry_count + 1
                should_retry = max_retries > 0 and new_retry_count <= max_retries

                if should_retry:
                    # 计算下次重试时间（指数退避）
                    next_retry_time = time.time() + retry_delay * new_retry_count
                    new_status = "active"
                    # 放回 ZSET
                    pipe = self._redis.pipeline()
                    pipe.zadd(f"trigger:shard_{shard_id}", {task_id: next_retry_time})
                    pipe.hdel(f"processing:shard_{shard_id}", task_id)
                    pipe.hset(f"task:{task_id}", mapping={
                        "status": new_status,
                        "retry_count": str(new_retry_count),
                        "last_run_at": str(time.time()),
                        "run_count": str(run_count + 1),
                        "fail_count": str(fail_count + 1),
                        "last_error": error_msg,
                    })
                    history_entry = json.dumps({
                        "fire_time": fire_time,
                        "status": "retry",
                        "duration_ms": duration_ms,
                        "error": error_msg,
                    }, ensure_ascii=False)
                    history_key = f"task_history:{task_id}"
                    pipe.lpush(history_key, history_entry)
                    pipe.ltrim(history_key, 0, self._max_history - 1)
                    await pipe.execute()
                    logger.info("任务 %s 将在 %.0f 秒后重试 (%d/%d)", task_id, retry_delay * new_retry_count, new_retry_count, max_retries)
                else:
                    # 不再重试，标记为 failed
                    pipe = self._redis.pipeline()
                    pipe.hdel(f"processing:shard_{shard_id}", task_id)
                    pipe.hset(f"task:{task_id}", mapping={
                        "status": "failed",
                        "retry_count": str(new_retry_count),
                        "last_run_at": str(time.time()),
                        "run_count": str(run_count + 1),
                        "fail_count": str(fail_count + 1),
                        "last_error": error_msg,
                    })
                    history_entry = json.dumps({
                        "fire_time": fire_time,
                        "status": "failed",
                        "duration_ms": duration_ms,
                        "error": error_msg,
                    }, ensure_ascii=False)
                    history_key = f"task_history:{task_id}"
                    pipe.lpush(history_key, history_entry)
                    pipe.ltrim(history_key, 0, self._max_history - 1)
                    await pipe.execute()

            return False

    @staticmethod
    def _get_str(data: dict, key: str) -> str:
        """从 Redis HGETALL 结果中取字符串值。"""
        val = data.get(key.encode(), data.get(key, b""))
        return val.decode() if isinstance(val, bytes) else val
