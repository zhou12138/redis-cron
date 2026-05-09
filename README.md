# redis-cron

基于 Redis ZSET 的分布式定时任务调度库，纯 Python asyncio 实现。

## 特性

- 🕐 **秒级精度** — Redis ZSET 时间排序，P99 延迟 < 3s
- 🔒 **分布式协调** — Lua 原子操作 + Fencing Token 防脑裂
- 📦 **分片机制** — `user_id % shard_count` 水平扩展，互不干扰
- ♻️ **两阶段提交** — processing 集合保证任务不丢失
- 🛡️ **幂等防重** — `task_id + fire_time` 去重 key
- 🌊 **整点打散** — 基于 user_id 的稳定 jitter，削峰填谷
- 🔄 **故障转移** — 节点宕机后存活节点均衡接管孤儿 shard（配额制 + 随机打散）
- ⚖️ **主动 Rebalance** — 新节点加入时老节点自动释放多余 shard
- ⏰ **时间窗口** — start_at / end_at 控制任务有效执行区间
- 🌍 **时区支持** — 按目标时区解释 Cron 表达式，自动处理夏令时
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
        start_at=1715234400.0,   # 指定生效时间
        end_at=1717826400.0,     # 指定过期时间
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
| `start_at` | `float` | `0` | 任务生效时间戳，0 表示立即生效。若 > now，首次触发推迟到 start_at |
| `end_at` | `float` | `0` | 任务过期时间戳，0 表示永不过期。过期后自动标记 completed |
| `timezone` | `str` | `"UTC"` | 时区名称（如 `"Asia/Shanghai"`），Cron 表达式按此时区解释 |

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
| `start_at` | `float` | `0` | 任务生效时间戳 |
| `end_at` | `float` | `0` | 任务过期时间戳 |
| `timezone` | `str` | `"UTC"` | 时区名称 |

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
| `start_at` | `float` | 任务生效时间戳（0 = 立即生效） |
| `end_at` | `float` | 任务过期时间戳（0 = 永不过期） |
| `timezone` | `str` | 时区名称（默认 `"UTC"`），Cron 表达式按此时区解释 |

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

## 时间窗口（start_at / end_at）

控制任务的有效执行区间：

```python
import time

now = time.time()
await scheduler.create_cron_task(
    task_type="campaign_email",
    cron="0 10 * * *",
    user_id=10001,
    payload={"campaign": "summer-sale"},
    start_at=now + 86400,       # 明天开始
    end_at=now + 86400 * 30,    # 30 天后过期
)
```

**行为规则：**

| 场景 | 行为 |
|------|------|
| `start_at > now` | 首次触发时间推迟到 start_at，之后按 cron 正常调度 |
| `end_at > 0` 且创建时已过期 | 任务直接标记 `completed`，不入调度队列 |
| cron 执行后 `next_fire_time > end_at` | 任务自动标记 `completed`，不再调度 |
| `start_at = 0` | 立即生效（默认） |
| `end_at = 0` | 永不过期（默认） |

**典型场景：** 营销活动定时推送、试用期任务、限时提醒。

---

## 时区支持

默认情况下，Cron 表达式按 UTC 解释。通过 `timezone` 参数可以指定目标时区：

```python
# 每天北京时间 9:00 触发（= UTC 01:00）
await scheduler.create_cron_task(
    task_type="daily_report",
    cron="0 9 * * *",
    user_id=10001,
    payload={"region": "cn"},
    timezone="Asia/Shanghai",
)

# 每天纽约时间 8:00 触发
await scheduler.create_cron_task(
    task_type="morning_alert",
    cron="0 8 * * *",
    timezone="America/New_York",
)
```

**时区名称**使用 IANA 标准格式（如 `Asia/Shanghai`、`America/New_York`、`Europe/London`），基于 Python 标准库 `zoneinfo`，零额外依赖。

### DST（夏令时）处理

对于有夏令时切换的时区（如 `America/New_York`），调度器会自动处理：

| 场景 | 行为 |
|------|------|
| **春天跳过**（如 2:00 AM → 3:00 AM） | 如果 Cron 触发时间落在不存在的时段内（如 2:30 AM），该次触发跳过，下一次正常调度 |
| **秋天重复**（如 2:00 AM → 1:00 AM） | 触发时间落在重复时段内时，按第一次出现计算 |
| **无 DST 时区**（如 `Asia/Shanghai`） | 行为始终稳定，无需特殊处理 |

**建议：** 对于关键业务任务，优先使用无 DST 的时区（如 UTC 或 Asia/Shanghai），避免夏令时切换带来的不确定性。

---

## 均衡故障转移与 Rebalance

### 均衡接管孤儿 Shard

节点宕机后，存活节点通过 `scan_orphan_shards()` 均衡接管无主 shard：

- 配额制：`ceil(总 shard 数 / 存活节点数) - 已持有数`
- 随机打散扫描顺序，避免多节点热点竞争
- 超过配额不再接管，保证各节点负载均衡

### 主动 Rebalance

新节点加入后，老节点调用 `rebalance()` 主动释放多余 shard：

- LIFO 策略：优先释放最近才接管的 shard
- 释放的 shard 由新节点通过 `scan_orphan_shards()` 接管

**典型流程：**
```
3 节点 × 16 shard → 每节点 ~5-6 个
1 节点宕机 → 2 节点均衡接管 → 每节点 8 个
新节点加入 → 老节点 rebalance → 新节点接管 → 每节点 ~5-6 个
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
