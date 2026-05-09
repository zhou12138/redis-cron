"""任务数据模型。"""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass, field
from typing import Any


@dataclass
class Task:
    """一次性延迟任务。"""

    task_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    task_type: str = ""
    payload: dict[str, Any] = field(default_factory=dict)
    user_id: int = 0
    shard_id: int = 0
    fire_time: float = 0.0
    created_at: float = field(default_factory=time.time)
    max_jitter: int = 0
    # 状态字段
    status: str = "active"
    # 重试配置
    max_retries: int = 0
    retry_count: int = 0
    retry_delay: int = 60
    # 执行统计
    last_run_at: float = 0.0
    run_count: int = 0
    fail_count: int = 0
    last_error: str = ""

    def to_redis(self) -> dict[str, str]:
        """序列化为 Redis HSET 字段映射。"""
        return {
            "task_type": self.task_type,
            "payload": json.dumps(self.payload, ensure_ascii=False),
            "user_id": str(self.user_id),
            "shard_id": str(self.shard_id),
            "fire_time": str(self.fire_time),
            "created_at": str(self.created_at),
            "max_jitter": str(self.max_jitter),
            "is_cron": "0",
            "status": self.status,
            "max_retries": str(self.max_retries),
            "retry_count": str(self.retry_count),
            "retry_delay": str(self.retry_delay),
            "last_run_at": str(self.last_run_at),
            "run_count": str(self.run_count),
            "fail_count": str(self.fail_count),
            "last_error": self.last_error,
        }

    @classmethod
    def from_redis(cls, task_id: str, data: dict[bytes | str, bytes | str]) -> Task:
        """从 Redis HGETALL 结果反序列化。"""
        def _s(v: bytes | str) -> str:
            return v.decode() if isinstance(v, bytes) else v

        def _g(key: str, default: str = "") -> str:
            return _s(data.get(key.encode(), data.get(key, default.encode())))

        return cls(
            task_id=task_id,
            task_type=_g("task_type"),
            payload=json.loads(_g("payload", "{}")),
            user_id=int(_g("user_id", "0")),
            shard_id=int(_g("shard_id", "0")),
            fire_time=float(_g("fire_time", "0")),
            created_at=float(_g("created_at", "0")),
            max_jitter=int(_g("max_jitter", "0")),
            status=_g("status", "active") or "active",
            max_retries=int(_g("max_retries", "0")),
            retry_count=int(_g("retry_count", "0")),
            retry_delay=int(_g("retry_delay", "60") or "60"),
            last_run_at=float(_g("last_run_at", "0")),
            run_count=int(_g("run_count", "0")),
            fail_count=int(_g("fail_count", "0")),
            last_error=_g("last_error"),
        )


@dataclass
class CronTask(Task):
    """周期性 Cron 任务。"""

    cron: str = ""

    def to_redis(self) -> dict[str, str]:
        """序列化为 Redis HSET 字段映射。"""
        d = super().to_redis()
        d["cron"] = self.cron
        d["is_cron"] = "1"
        return d

    @classmethod
    def from_redis(cls, task_id: str, data: dict[bytes | str, bytes | str]) -> CronTask:
        """从 Redis HGETALL 结果反序列化。"""
        def _s(v: bytes | str) -> str:
            return v.decode() if isinstance(v, bytes) else v

        def _g(key: str, default: str = "") -> str:
            return _s(data.get(key.encode(), data.get(key, default.encode())))

        return cls(
            task_id=task_id,
            task_type=_g("task_type"),
            payload=json.loads(_g("payload", "{}")),
            user_id=int(_g("user_id", "0")),
            shard_id=int(_g("shard_id", "0")),
            fire_time=float(_g("fire_time", "0")),
            created_at=float(_g("created_at", "0")),
            max_jitter=int(_g("max_jitter", "0")),
            status=_g("status", "active") or "active",
            max_retries=int(_g("max_retries", "0")),
            retry_count=int(_g("retry_count", "0")),
            retry_delay=int(_g("retry_delay", "60") or "60"),
            last_run_at=float(_g("last_run_at", "0")),
            run_count=int(_g("run_count", "0")),
            fail_count=int(_g("fail_count", "0")),
            last_error=_g("last_error"),
            cron=_g("cron"),
        )
