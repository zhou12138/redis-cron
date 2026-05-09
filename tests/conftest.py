"""测试 fixtures。"""

import asyncio
from typing import AsyncGenerator

import pytest
import pytest_asyncio


@pytest.fixture(scope="session")
def event_loop():
    """创建全局事件循环。"""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture
async def fake_redis():
    """提供 fakeredis 异步客户端。"""
    try:
        import fakeredis.aioredis
    except ImportError:
        pytest.skip("需要安装 fakeredis[lua]")

    server = fakeredis.aioredis.FakeServer()
    r = fakeredis.aioredis.FakeRedis(server=server, decode_responses=False)
    yield r
    await r.aclose()
