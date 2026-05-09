"""分片锁管理，基于 Fencing Token 防脑裂 + 均衡接管 + 主动 Rebalance。"""

from __future__ import annotations

import logging
import math
import random
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

    async def _count_alive_nodes(self) -> int:
        """统计存活节点数。"""
        alive = 0
        cursor = 0
        while True:
            cursor, keys = await self._redis.scan(cursor, match="node:*", count=100)
            alive += len(keys)
            if cursor == 0:
                break
        return max(alive, 1)

    def _fair_share(self, alive_nodes: int) -> int:
        """计算每个节点应持有的 shard 上限。"""
        return math.ceil(self._shard_count / alive_nodes)

    async def scan_orphan_shards(self) -> list[int]:
        """均衡接管无主 shard。

        根据存活节点数计算公平配额，只接管不超过配额的 shard。
        随机打散扫描顺序，避免多节点同时从 shard_0 开始竞争。
        """
        alive_nodes = await self._count_alive_nodes()
        fair = self._fair_share(alive_nodes)
        quota = fair - len(self.my_shards)

        if quota <= 0:
            return []

        # 随机打散，避免热点竞争
        shard_ids = list(range(self._shard_count))
        random.shuffle(shard_ids)

        acquired: list[int] = []
        for shard_id in shard_ids:
            if len(acquired) >= quota:
                break
            if shard_id in self.my_shards:
                continue
            owner = await self._redis.get(f"shard_lock:{shard_id}")
            if owner is None:
                token = await self.try_acquire(shard_id)
                if token is not None:
                    acquired.append(shard_id)
                    logger.info("均衡接管孤儿 shard %d, token=%d (quota=%d/%d)", shard_id, token, len(acquired), quota)
        return acquired

    async def rebalance(self) -> list[int]:
        """主动 Rebalance：当本节点持有过多 shard 时，主动释放多余的。

        典型场景：新节点加入后，老节点调用 rebalance 释放多余 shard，
        新节点通过 scan_orphan_shards 接管。

        Returns:
            释放的 shard ID 列表
        """
        alive_nodes = await self._count_alive_nodes()
        fair = self._fair_share(alive_nodes)
        excess = len(self.my_shards) - fair

        if excess <= 0:
            return []

        # 释放最后接管的 shard（LIFO 策略，保留最早持有的）
        to_release = list(self.my_shards.keys())[-excess:]
        released: list[int] = []

        for shard_id in to_release:
            await self.release(shard_id)
            released.append(shard_id)
            logger.info("Rebalance 释放 shard %d (持有 %d, 公平值 %d)", shard_id, len(self.my_shards), fair)

        return released
