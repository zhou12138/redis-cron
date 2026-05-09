"""分片锁管理，基于 Fencing Token 防脑裂。"""

from __future__ import annotations

import logging
from typing import Any

import redis.asyncio as aioredis

from . import lua_scripts

logger = logging.getLogger(__name__)


class ShardManager:
    """管理分片锁的获取、续约和释放。"""

    def __init__(self, redis_client: aioredis.Redis, node_id: str, shard_count: int, lock_ttl: int = 15):
        self._redis = redis_client
        self._node_id = node_id
        self._shard_count = shard_count
        self._lock_ttl = lock_ttl
        # shard_id → fencing_token
        self.my_shards: dict[int, int] = {}

    def owner_val(self, shard_id: int) -> str | None:
        """获取指定 shard 的 owner 标识字符串。"""
        token = self.my_shards.get(shard_id)
        if token is None:
            return None
        return f"{self._node_id}:{token}"

    async def try_acquire(self, shard_id: int) -> int | None:
        """尝试获取 shard 锁，成功返回 fencing token，失败返回 None。"""
        token: Any = await self._redis.eval(
            lua_scripts.ACQUIRE_SHARD_LOCK, 2,
            f"shard_lock:{shard_id}",
            f"shard_fence:{shard_id}",
            self._node_id, str(self._lock_ttl),
        )
        if isinstance(token, int) and token > 0:
            self.my_shards[shard_id] = token
            logger.info("获取 shard %d 锁, token=%d", shard_id, token)
            return token
        return None

    async def renew(self, shard_id: int) -> bool:
        """续约 shard 锁，成功返回 True。"""
        ov = self.owner_val(shard_id)
        if ov is None:
            return False
        ok: Any = await self._redis.eval(
            lua_scripts.RENEW_SHARD_LOCK, 1,
            f"shard_lock:{shard_id}",
            ov, str(self._lock_ttl),
        )
        return bool(ok)

    async def release(self, shard_id: int) -> None:
        """主动释放 shard 锁。"""
        ov = self.owner_val(shard_id)
        if ov is None:
            return
        # 只删除自己持有的锁
        current = await self._redis.get(f"shard_lock:{shard_id}")
        if current is not None:
            val = current.decode() if isinstance(current, bytes) else current
            if val == ov:
                await self._redis.delete(f"shard_lock:{shard_id}")
        self.my_shards.pop(shard_id, None)

    async def heartbeat(self) -> list[int]:
        """续约所有持有的 shard 锁，返回丢失的 shard 列表。"""
        # 续约节点注册
        await self._redis.set(f"node:{self._node_id}", "alive", ex=self._lock_ttl)

        lost: list[int] = []
        for shard_id in list(self.my_shards.keys()):
            ok = await self.renew(shard_id)
            if not ok:
                lost.append(shard_id)
                logger.warning("丢失 shard %d 所有权", shard_id)

        for sid in lost:
            self.my_shards.pop(sid, None)
        return lost

    async def scan_orphan_shards(self) -> list[int]:
        """扫描并接管无主 shard，返回新接管的 shard 列表。"""
        acquired: list[int] = []
        for shard_id in range(self._shard_count):
            if shard_id in self.my_shards:
                continue
            owner = await self._redis.get(f"shard_lock:{shard_id}")
            if owner is None:
                token = await self.try_acquire(shard_id)
                if token is not None:
                    acquired.append(shard_id)
                    logger.info("接管孤儿 shard %d, token=%d", shard_id, token)
        return acquired
