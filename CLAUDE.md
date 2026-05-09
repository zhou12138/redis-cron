# redis-cron — 分布式定时任务调度库

## 项目目标
基于 Redis ZSET 实现一个 Python 分布式定时任务调度库，核心依赖仅 Redis。

## 设计文档
完整架构设计在 `DESIGN.md` 中，包含：
- ZSET 触发队列 + HSET 任务详情的数据模型
- 分片（shard）机制，按 user_id % shard_count 分配
- 纯 Redis 分布式锁（无 etcd/ZK），Fencing Token 防脑裂
- Lua 原子脚本：取任务 + 所有权校验
- processing 集合做两阶段提交
- 整点风暴打散（jitter）
- 幂等防重（dedup key）
- 补偿扫描（scavenge）

## 技术规范
- Python 3.10+，纯 asyncio
- 核心依赖：redis[hiredis]、croniter
- 包名：`redis_cron`
- 使用 pyproject.toml（不要 setup.py）

## 代码结构
```
redis_cron/
  __init__.py        # 公开 API：RedisScheduler, Task, CronTask
  scheduler.py       # SchedulerNode：分片管理、扫描循环、心跳
  models.py          # Task / CronTask 数据模型
  lua_scripts.py     # 所有 Lua 脚本常量
  shard.py           # 分片锁管理、Fencing Token
  worker.py          # Worker：消费任务、幂等执行、回调
  utils.py           # jitter 计算、cron 解析
tests/
  test_scheduler.py  # 单元测试（用 fakeredis）
  test_integration.py # 集成测试（需要真实 Redis）
  conftest.py
pyproject.toml
README.md
```

## API 设计（目标接口）
```python
from redis_cron import RedisScheduler

scheduler = RedisScheduler(
    redis_url="redis://localhost:6379",
    shard_count=128,
    node_id="node-1",  # 可选，自动生成 UUID
)

# 注册任务处理器
@scheduler.task("send_email")
async def send_email(task_id: str, payload: dict):
    await do_send(payload["to"], payload["subject"])

# 创建定时任务
await scheduler.create_cron_task(
    task_type="send_email",
    cron="0 8 * * *",
    user_id=10001,
    payload={"to": "user@example.com", "subject": "Daily Report"},
    max_jitter=60,  # 秒级打散
)

# 创建一次性延迟任务
await scheduler.create_delayed_task(
    task_type="send_email",
    delay_seconds=300,
    payload={...},
)

# 启动（同时作为 Scheduler + Worker）
await scheduler.start()

# 或者分角色启动
await scheduler.start(roles=["scheduler"])  # 只调度
await scheduler.start(roles=["worker"])     # 只执行
```

## 质量要求
- 所有 Lua 脚本从 DESIGN.md 中提取，保持原子性
- 完整的类型注解（type hints）
- docstring 写中文
- 写完整的 README.md（中文），包含使用示例
- 单元测试覆盖核心逻辑
