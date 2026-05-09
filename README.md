# redis-cron

基于 Redis ZSET 的分布式定时任务调度库，纯 Python asyncio 实现。

## 特性

- 🕐 **秒级精度** — Redis ZSET 时间排序，P99 延迟 < 3s
- 🔒 **分布式协调** — Lua 原子操作 + Fencing Token 防脑裂
- 📦 **分片机制** — `user_id % shard_count` 水平扩展，互不干扰
- ♻️ **两阶段提交** — processing 集合保证任务不丢失
- 🛡️ **幂等防重** — `task_id + fire_time` 去重 key
- 🌊 **整点打散** — 基于 user_id 的稳定 jitter，削峰填谷
- 🔄 **故障转移** — 节点宕机后存活节点自动接管孤儿 shard
- 🏗️ **灵活部署** — Scheduler / Worker 可同进程或分角色部署
- 🔁 **自动重试** — 可配置重试次数和指数退避
- 📊 **执行历史** — 每个任务保留最近 N 次执行记录
- ⚡ **手动触发** — 支持立即触发任务，跳过调度等待
- 📦 **批量操作** — 批量暂停/恢复/删除

## 安装

```bash
pip install git+https://github.com/zhou12138/redis-cron.git

# 或本地安装
git clone https://github.com/zhou12138/redis-cron.git
cd redis-cron
pip install -e .
```

### 依赖

- Python 3.10+
- redis[hiredis] >= 5.0.0
- croniter >= 1.3.0

---

## 快速开始

```python
import asyncio
from redis_cron import RedisScheduler

scheduler = RedisScheduler(redis_url="redis://localhost:6379")

@scheduler.task("send_email")
async def send_email(task_id: str, payload: dict):
    print(f"发送邮件给 {payload['to']}: {payload['subject']}")

async def main():
    # 创建周期性任务（带重试）
    await scheduler.create_cron_task(
        task_type="send_email",
        cron="0 8 * * *",
        user_id=10001,
        payload={"to": "user@example.com", "subject": "每日报表"},
        max_jitter=60,
        max_retries=3,
        retry_delay=30,
    )

    # 创建一次性延迟任务
    await scheduler.create_delayed_task(
        task_type="send_email",
        delay_seconds=300,
        payload={"to": "user@example.com", "subject": "5分钟后提醒"},
    )

    # 手动触发
    await scheduler.trigger_task("task-id-xxx")

    # 查看执行历史
    history = await scheduler.get_task_history("task-id-xxx", limit=10)

    # 按用户查询
    tasks = await scheduler.list_tasks_by_user(user_id=10001)

    # 批量操作
    await scheduler.bulk_pause_tasks(["id1", "id2", "id3"])
    await scheduler.bulk_resume_tasks(["id1", "id2", "id3"])
    await scheduler.bulk_delete_tasks(["id1", "id2"])

    # 启动（Ctrl+C 停止）
    await scheduler.start()

asyncio.run(main())
```

---

## 任务状态机

```
                 create
                   │
                   ▼
              ┌─────────┐
         ┌───▶│  active  │◀──────────────┐
         │    └─────────┘               │
         │         │                    │
    resume │    execute              success
         │         │                (cron task)
         │         ▼                    │
         │    ┌─────────┐              │
         │    │ running  │──────────────┘
         │    └─────────┘
         │     │       │
         │  failure  success
         │  (retry)  (one-shot)
         │     │       │
         │     ▼       ▼
         │  ┌──────┐ ┌───────────┐
         │  │active│ │ completed │
         │  └──────┘ └───────────┘
         │     │
         │  failure
         │  (no retry)
         │     │
         │     ▼
         │  ┌────────┐
         │  │ failed  │
         │  └────────┘
         │
    ┌─────────┐
    │ paused  │
    └─────────┘
```

| 状态 | 说明 |
|------|------|
| `active` | 正常调度中，在 ZSET 触发队列中 |
| `paused` | 已暂停，从 ZSET 移除但数据保留 |
| `running` | 正在执行中 |
| `completed` | 一次性任务执行完成 |
| `failed` | 最后一次执行失败（超过最大重试次数） |

---

## API 参考

### `RedisScheduler` — 核心调度器

#### 构造函数

```python
RedisScheduler(
    redis_url="redis://localhost:6379",
    shard_count=128,
    node_id=None,
    lock_ttl=15,
    batch_size=200,
    scan_interval=0.1,
    heartbeat_interval=5.0,
    scavenge_interval=10.0,
    recover_interval=60.0,
    processing_timeout=60.0,
    task_timeout=60,
    dedup_ttl=3600,
)
```

---

#### `@scheduler.task(task_type)` — 注册任务处理器

```python
@scheduler.task("send_email")
async def send_email(task_id: str, payload: dict) -> None:
    ...
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `task_type` | `str` | 任务类型标识 |

---

#### `await scheduler.create_cron_task(...)` — 创建周期性 Cron 任务

```python
task_id = await scheduler.create_cron_task(
    task_type="send_email",
    cron="0 8 * * *",
    user_id=10001,
    payload={"to": "user@example.com"},
    max_jitter=60,
    task_id="custom-id",
    max_retries=3,
    retry_delay=30,
)
```

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `task_type` | `str` | *必填* | 任务类型 |
| `cron` | `str` | *必填* | 标准 5 位 Cron 表达式 |
| `user_id` | `int` | `0` | 用户 ID，用于分片和 jitter |
| `payload` | `dict \| None` | `None` | 自定义任务数据 |
| `max_jitter` | `int` | `0` | 最大打散秒数 |
| `task_id` | `str \| None` | 自动生成 | 自定义任务 ID |
| `max_retries` | `int` | `0` | 最大重试次数，0 表示不重试 |
| `retry_delay` | `int` | `60` | 重试间隔基数（秒），实际间隔 = retry_delay × retry_count |

**返回值**：`str` — 任务 ID

---

#### `await scheduler.create_delayed_task(...)` — 创建一次性延迟任务

```python
task_id = await scheduler.create_delayed_task(
    task_type="send_notification",
    delay_seconds=300,
    user_id=20002,
    payload={"message": "订单即将超时"},
    max_retries=2,
    retry_delay=30,
)
```

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `task_type` | `str` | *必填* | 任务类型 |
| `delay_seconds` | `float` | *必填* | 延迟秒数 |
| `user_id` | `int` | `0` | 用户 ID |
| `payload` | `dict \| None` | `None` | 自定义任务数据 |
| `task_id` | `str \| None` | 自动生成 | 自定义任务 ID |
| `max_retries` | `int` | `0` | 最大重试次数 |
| `retry_delay` | `int` | `60` | 重试间隔基数（秒） |

**返回值**：`str` — 任务 ID

---

#### `await scheduler.trigger_task(task_id)` — 手动立即触发

```python
ok = await scheduler.trigger_task("task-id-xxx")
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `task_id` | `str` | 任务 ID |

**返回值**：`bool` — True 表示触发成功，False 表示任务不存在或已暂停

将任务在 ZSET 中的 score 设为 0，调度器下一轮扫描即会立即执行。已暂停的任务不可触发。

---

#### `await scheduler.get_task(task_id)` — 获取任务详情

**返回值**：`Task | CronTask | None`

---

#### `await scheduler.pause_task(task_id)` / `resume_task(task_id)` — 暂停/恢复

暂停时将任务从 ZSET 移除并设置 `status="paused"`。恢复时重新计算触发时间并放回 ZSET，设置 `status="active"`。

**返回值**：`bool`

---

#### `await scheduler.delete_task(task_id)` — 删除任务

同时清理 ZSET、processing、task hash、user_tasks 索引和执行历史。

**返回值**：`bool`

---

#### `await scheduler.get_task_history(task_id, limit=10)` — 获取执行历史

```python
history = await scheduler.get_task_history("task-id", limit=20)
# [{"fire_time": 1715234400.0, "status": "success", "duration_ms": 42, "error": null}, ...]
```

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `task_id` | `str` | *必填* | 任务 ID |
| `limit` | `int` | `10` | 返回记录数上限 |

**返回值**：`list[dict]` — 执行记录列表（最新在前），每条包含：
- `fire_time` — 触发时间戳
- `status` — `"success"` / `"retry"` / `"failed"`
- `duration_ms` — 执行耗时（毫秒）
- `error` — 错误信息（成功时为 null）

---

#### `await scheduler.list_tasks_by_user(user_id, ...)` — 按用户查询任务

```python
tasks = await scheduler.list_tasks_by_user(
    user_id=10001,
    task_type="send_email",
    limit=50,
    offset=0,
)
```

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `user_id` | `int` | *必填* | 用户 ID |
| `task_type` | `str \| None` | `None` | 按任务类型过滤 |
| `limit` | `int` | `100` | 返回数量上限 |
| `offset` | `int` | `0` | 跳过前 N 条 |

**返回值**：`list[Task | CronTask]` — 包含已暂停的任务（通过 `user_tasks:{user_id}` 二级索引查询）

---

#### `await scheduler.bulk_delete_tasks(task_ids)` — 批量删除

```python
deleted_count = await scheduler.bulk_delete_tasks(["id1", "id2", "id3"])
```

**返回值**：`int` — 成功删除的任务数

---

#### `await scheduler.bulk_pause_tasks(task_ids)` — 批量暂停

**返回值**：`int` — 成功暂停的任务数

---

#### `await scheduler.bulk_resume_tasks(task_ids)` — 批量恢复

**返回值**：`int` — 成功恢复的任务数

---

### `Task` — 一次性任务数据模型

| 字段 | 类型 | 说明 |
|------|------|------|
| `task_id` | `str` | 唯一标识 |
| `task_type` | `str` | 任务类型 |
| `payload` | `dict[str, Any]` | 自定义数据 |
| `user_id` | `int` | 所属用户 ID |
| `shard_id` | `int` | 所在分片 |
| `fire_time` | `float` | 触发时间戳 |
| `created_at` | `float` | 创建时间戳 |
| `max_jitter` | `int` | 最大打散秒数 |
| `status` | `str` | 状态：active/paused/running/completed/failed |
| `max_retries` | `int` | 最大重试次数（默认 0） |
| `retry_count` | `int` | 当前重试次数 |
| `retry_delay` | `int` | 重试间隔基数（秒，默认 60） |
| `last_run_at` | `float` | 最后执行时间戳 |
| `run_count` | `int` | 总执行次数 |
| `fail_count` | `int` | 失败次数 |
| `last_error` | `str` | 最后一次错误信息 |

### `CronTask(Task)` — 周期性任务数据模型

继承 `Task`，额外字段：

| 字段 | 类型 | 说明 |
|------|------|------|
| `cron` | `str` | Cron 表达式 |

---

## Redis 数据结构

```
Redis
├── ZSET   trigger:shard_{0..N}       score = 触发时间戳, member = task_id
├── HASH   task:{task_id}             任务详情 (task_type, payload, cron, status, ...)
├── HASH   processing:shard_{N}       执行中任务 (task_id → 开始时间戳)
├── LIST   task_history:{task_id}     执行历史 (最新在前，最多保留 100 条)
├── SET    user_tasks:{user_id}       用户任务二级索引
├── STRING shard_lock:{N}             分片锁
├── STRING shard_fence:{N}            Fencing Token 计数器
├── STRING node:{node_id}             节点注册
└── STRING dedup:{task_id}:{fire_ts}  幂等去重 key
```

---

## 重试机制

设置 `max_retries > 0` 启用自动重试：

```python
await scheduler.create_cron_task(
    task_type="flaky_api",
    cron="*/10 * * * *",
    max_retries=3,    # 最多重试 3 次
    retry_delay=30,   # 基础间隔 30 秒
)
```

重试间隔采用线性退避：`retry_delay × retry_count`
- 第 1 次重试：30 秒后
- 第 2 次重试：60 秒后
- 第 3 次重试：90 秒后

超过最大重试次数后，任务状态变为 `failed`。成功执行会重置 `retry_count` 为 0。

---

## 运行测试

```bash
pip install -e ".[dev]"

# 单元测试（fakeredis，无需真实 Redis）
pytest tests/test_scheduler.py -v

# 集成测试（需要真实 Redis）
REDIS_URL=redis://localhost:6379 pytest tests/test_integration.py -v
```

---

## 许可证

MIT
