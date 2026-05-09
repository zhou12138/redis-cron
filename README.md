# redis-cron — 基于 Redis ZSET 的分布式定时任务调度库

一个轻量级的 Python 分布式定时任务调度库，核心依赖仅 Redis。

## 特性

- 🕐 **秒级精度**：基于 Redis ZSET 的时间排序，P99 延迟 < 3 秒
- 🔒 **分布式协调**：纯 Redis 分布式锁 + Fencing Token 防脑裂
- 📦 **分片机制**：按 `user_id % shard_count` 分片，水平扩展
- ♻️ **两阶段提交**：processing 集合保证任务不丢失
- 🛡️ **幂等防重**：`task_id + fire_time` 去重 key
- 🌊 **整点打散**：基于 user_id 的稳定 jitter，削峰填谷
- 🔄 **补偿扫描**：定期恢复 processing 中超时的任务
- 🏗️ **灵活部署**：Scheduler / Worker 可同进程或分角色部署

## 安装

```bash
pip install -e .
```

## 快速开始

```python
import asyncio
from redis_cron import RedisScheduler

scheduler = RedisScheduler(
    redis_url="redis://localhost:6379",
    shard_count=128,
    node_id="node-1",  # 可选，自动生成 UUID
)

# 注册任务处理器
@scheduler.task("send_email")
async def send_email(task_id: str, payload: dict):
    print(f"发送邮件给 {payload['to']}: {payload['subject']}")

async def main():
    # 创建周期性 Cron 任务
    await scheduler.create_cron_task(
        task_type="send_email",
        cron="0 8 * * *",
        user_id=10001,
        payload={"to": "user@example.com", "subject": "每日报表"},
        max_jitter=60,  # 秒级打散，避免整点风暴
    )

    # 创建一次性延迟任务
    await scheduler.create_delayed_task(
        task_type="send_email",
        delay_seconds=300,
        payload={"to": "user@example.com", "subject": "提醒"},
    )

    # 启动调度器（同时作为 Scheduler + Worker）
    await scheduler.start()

asyncio.run(main())
```

## 分角色部署

```python
# 节点 A：只调度
await scheduler.start(roles=["scheduler"])

# 节点 B：只执行
await scheduler.start(roles=["worker"])
```

## 架构概览

```
Redis Cluster
├── ZSET: trigger:shard_{0..N}     ← score = 触发时间戳
├── HASH: task:{id}                ← 任务详情
├── HASH: processing:shard_{0..N}  ← 执行中任务（两阶段提交）
├── STRING: shard_lock:{N}         ← 分片锁
└── STRING: shard_fence:{N}        ← Fencing Token

Scheduler 节点
├── 心跳循环：每 5s 续约锁
├── 扫描循环：接管无主 shard
├── 调度循环：取到期任务并执行
└── 补偿扫描：恢复 processing 中超时任务
```

## 配置参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `redis_url` | `redis://localhost:6379` | Redis 连接地址 |
| `shard_count` | `128` | 分片总数 |
| `node_id` | 自动生成 | 节点唯一标识 |
| `lock_ttl` | `15` | 分片锁 TTL（秒） |
| `batch_size` | `200` | 每次取任务的批量大小 |
| `scan_interval` | `0.1` | 无任务时扫描间隔（秒） |
| `heartbeat_interval` | `5.0` | 心跳间隔（秒） |
| `processing_timeout` | `60.0` | processing 超时时间（秒） |
| `task_timeout` | `60` | 单任务执行超时（秒） |

## 运行测试

```bash
# 安装开发依赖
pip install -e ".[dev]"

# 单元测试（使用 fakeredis，无需真实 Redis）
pytest tests/test_scheduler.py -v

# 集成测试（需要真实 Redis）
REDIS_URL=redis://localhost:6379 pytest tests/test_integration.py -v
```

## 依赖

- Python 3.10+
- redis[hiredis] >= 5.0.0
- croniter >= 1.3.0

## 许可证

MIT
