"""集成测试（需要真实 Redis）。

运行方式：
    REDIS_URL=redis://localhost:6379 pytest tests/test_integration.py -v
"""

from __future__ import annotations

import asyncio
import os
import time

import pytest
import pytest_asyncio
import redis.asyncio as aioredis

from redis_cron import RedisScheduler

REDIS_URL = os.environ.get("REDIS_URL", "")


@pytest.fixture(scope="module")
def redis_url():
    if not REDIS_URL:
        pytest.skip("需要设置 REDIS_URL 环境变量运行集成测试")
    return REDIS_URL


@pytest_asyncio.fixture
async def scheduler(redis_url):
    s = RedisScheduler(
        redis_url=redis_url,
        shard_count=4,
        lock_ttl=10,
        scan_interval=0.05,
        heartbeat_interval=2.0,
        scavenge_interval=2.0,
        recover_interval=5.0,
        processing_timeout=5.0,
    )
    yield s
    await s.stop()


@pytest.mark.asyncio
async def test_create_and_schedule_delayed_task(scheduler):
    """创建延迟任务后应能被调度执行。"""
    executed = asyncio.Event()

    @scheduler.task("integration_test")
    async def handler(task_id: str, payload: dict):
        assert payload["msg"] == "hello"
        executed.set()

    tid = await scheduler.create_delayed_task(
        task_type="integration_test",
        delay_seconds=0.5,
        payload={"msg": "hello"},
    )
    assert tid

    # 启动调度器（后台运行）
    task = asyncio.create_task(scheduler.start())

    try:
        await asyncio.wait_for(executed.wait(), timeout=10)
    finally:
        await scheduler.stop()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
