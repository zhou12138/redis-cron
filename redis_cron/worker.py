"""Worker：消费任务、幂等执行、回调。"""

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
    ):
        self._redis = redis_client
        self._shard_count = shard_count
        self._dedup_ttl = dedup_ttl
        self._task_timeout = task_timeout
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

            # 执行
            await asyncio.wait_for(handler(task_id, task.payload), timeout=self._task_timeout)

            # ACK：从 processing 移除，cron 任务计算下次触发放回 ZSET
            next_fire = 0.0
            if is_cron and isinstance(task, CronTask) and task.cron:
                next_fire = calc_next_fire(task.cron, fire_time)
                jitter = calc_stable_jitter(task.user_id, task.max_jitter)
                next_fire += jitter

            await self._redis.eval(
                lua_scripts.ACK_TASK, 2,
                f"processing:shard_{shard_id}",
                f"trigger:shard_{shard_id}",
                task_id, str(next_fire),
            )

            logger.info("任务 %s 执行成功", task_id)
            return True

        except asyncio.TimeoutError:
            logger.error("任务 %s 执行超时", task_id)
            await self._redis.delete(dedup_key)
            return False
        except Exception:
            logger.exception("任务 %s 执行失败", task_id)
            await self._redis.delete(dedup_key)
            return False

    @staticmethod
    def _get_str(data: dict, key: str) -> str:
        """从 Redis HGETALL 结果中取字符串值。"""
        val = data.get(key.encode(), data.get(key, b""))
        return val.decode() if isinstance(val, bytes) else val
