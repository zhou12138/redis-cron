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
        }

    @classmethod
    def from_redis(cls, task_id: str, data: dict[bytes | str, bytes | str]) -> Task:
        """从 Redis HGETALL 结果反序列化。"""
        def _s(v: bytes | str) -> str:
            return v.decode() if isinstance(v, bytes) else v

        return cls(
            task_id=task_id,
            task_type=_s(data.get(b"task_type", data.get("task_type", b""))),
            payload=json.loads(_s(data.get(b"payload", data.get("payload", b"{}")))),
            user_id=int(_s(data.get(b"user_id", data.get("user_id", b"0")))),
            shard_id=int(_s(data.get(b"shard_id", data.get("shard_id", b"0")))),
            fire_time=float(_s(data.get(b"fire_time", data.get("fire_time", b"0")))),
            created_at=float(_s(data.get(b"created_at", data.get("created_at", b"0")))),
            max_jitter=int(_s(data.get(b"max_jitter", data.get("max_jitter", b"0")))),
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

        return cls(
            task_id=task_id,
            task_type=_s(data.get(b"task_type", data.get("task_type", b""))),
            payload=json.loads(_s(data.get(b"payload", data.get("payload", b"{}")))),
            user_id=int(_s(data.get(b"user_id", data.get("user_id", b"0")))),
            shard_id=int(_s(data.get(b"shard_id", data.get("shard_id", b"0")))),
            fire_time=float(_s(data.get(b"fire_time", data.get("fire_time", b"0")))),
            created_at=float(_s(data.get(b"created_at", data.get("created_at", b"0")))),
            max_jitter=int(_s(data.get(b"max_jitter", data.get("max_jitter", b"0")))),
            cron=_s(data.get(b"cron", data.get("cron", b""))),
        )
