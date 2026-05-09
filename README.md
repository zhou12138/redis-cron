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

## 安装

```bash
# 从 GitHub 安装
pip install git+https://github.com/zhou12138/redis-cron.git

# 或克隆后本地安装
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
    # 创建周期性任务
    await scheduler.create_cron_task(
        task_type="send_email",
        cron="0 8 * * *",
        user_id=10001,
        payload={"to": "user@example.com", "subject": "每日报表"},
        max_jitter=60,
    )

    # 创建一次性延迟任务
    await scheduler.create_delayed_task(
        task_type="send_email",
        delay_seconds=300,
        payload={"to": "user@example.com", "subject": "5分钟后提醒"},
    )

    # 启动（Ctrl+C 停止）
    await scheduler.start()

asyncio.run(main())
```

---

## API 参考

### `RedisScheduler` — 核心调度器

#### 构造函数

```python
RedisScheduler(
    redis_url: str = "redis://localhost:6379",
    shard_count: int = 128,
    node_id: str | None = None,
    lock_ttl: int = 15,
    batch_size: int = 200,
    scan_interval: float = 0.1,
    heartbeat_interval: float = 5.0,
    scavenge_interval: float = 10.0,
    recover_interval: float = 60.0,
    processing_timeout: float = 60.0,
    dedup_ttl: int = 3600,
    task_timeout: int = 60,
)
```

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `redis_url` | `str` | `"redis://localhost:6379"` | Redis 连接地址，支持 `redis://`、`rediss://`（TLS）、`redis+sentinel://` |
| `shard_count` | `int` | `128` | 分片总数。任务按 `user_id % shard_count` 分配到对应分片。建议设为 2 的幂次（64/128/256），一旦上线不可更改 |
| `node_id` | `str \| None` | 自动生成 UUID | 节点唯一标识。多实例部署时**必须不同**。不传则自动生成 `uuid4().hex[:8]` |
| `lock_ttl` | `int` | `15` | 分片锁 TTL（秒）。心跳间隔应 < lock_ttl / 3，否则锁可能意外过期 |
| `batch_size` | `int` | `200` | 每次从 ZSET 取出的最大任务数。增大可提高吞吐但增加单次延迟 |
| `scan_interval` | `float` | `0.1` | 无到期任务时的轮询间隔（秒）。降低可减少延迟但增加 Redis 压力 |
| `heartbeat_interval` | `float` | `5.0` | 心跳间隔（秒）。续约节点注册和所有 shard 锁 |
| `scavenge_interval` | `float` | `10.0` | 孤儿 shard 扫描间隔（秒）。节点宕机后，存活节点在此间隔内发现并接管 |
| `recover_interval` | `float` | `60.0` | 补偿扫描间隔（秒）。检查 processing 集合中超时未完成的任务 |
| `processing_timeout` | `float` | `60.0` | 任务在 processing 集合中的最大停留时间（秒），超时后被补偿扫描放回触发队列 |
| `dedup_ttl` | `int` | `3600` | 幂等去重 key 的 TTL（秒）。在此时间内同一 `task_id + fire_time` 不会重复执行 |
| `task_timeout` | `int` | `60` | 单个任务处理器的执行超时（秒），超时后 `asyncio.wait_for` 抛出 `TimeoutError` |

---

#### `scheduler.task(task_type: str)` — 注册任务处理器（装饰器）

```python
@scheduler.task("send_email")
async def send_email(task_id: str, payload: dict) -> None:
    ...
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `task_type` | `str` | 任务类型标识，与 `create_cron_task` / `create_delayed_task` 的 `task_type` 对应 |

**处理器签名**：`async def handler(task_id: str, payload: dict[str, Any]) -> None`

- `task_id`：当前执行的任务 ID
- `payload`：创建任务时传入的自定义数据

---

#### `await scheduler.create_cron_task(...)` — 创建周期性 Cron 任务

```python
task_id = await scheduler.create_cron_task(
    task_type="send_email",
    cron="0 8 * * *",
    user_id=10001,
    payload={"to": "user@example.com", "subject": "每日报表"},
    max_jitter=60,
    task_id="custom-id-001",
)
```

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `task_type` | `str` | *必填* | 任务类型，需提前通过 `@scheduler.task()` 注册对应处理器 |
| `cron` | `str` | *必填* | 标准 5 位 Cron 表达式：`分 时 日 月 周`。示例：`"0 8 * * *"`（每天 8 点）、`"*/5 * * * *"`（每 5 分钟）、`"0 0 1 * *"`（每月 1 号） |
| `user_id` | `int` | `0` | 用户 ID，用于分片（`user_id % shard_count`）和 jitter 计算 |
| `payload` | `dict \| None` | `None` | 自定义任务数据，JSON 可序列化，处理器执行时会收到 |
| `max_jitter` | `int` | `0` | 最大打散秒数。设为 60 表示在触发时间基础上增加 0~59 秒的稳定偏移（同一 user_id 每次偏移相同），用于整点削峰 |
| `task_id` | `str \| None` | 自动生成 | 自定义任务 ID。不传则自动生成 `uuid4().hex`。相同 task_id 会覆盖已有任务 |

**返回值**：`str` — 任务 ID

---

#### `await scheduler.create_delayed_task(...)` — 创建一次性延迟任务

```python
task_id = await scheduler.create_delayed_task(
    task_type="send_notification",
    delay_seconds=300,
    user_id=20002,
    payload={"message": "你的订单即将超时"},
    task_id="order-timeout-12345",
)
```

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `task_type` | `str` | *必填* | 任务类型 |
| `delay_seconds` | `float` | *必填* | 延迟秒数（从当前时间算起） |
| `user_id` | `int` | `0` | 用户 ID，用于分片 |
| `payload` | `dict \| None` | `None` | 自定义任务数据 |
| `task_id` | `str \| None` | 自动生成 | 自定义任务 ID |

**返回值**：`str` — 任务 ID

> 一次性任务执行完成后**自动清理**，不会重复触发。

---

#### `await scheduler.start(roles=None)` — 启动调度器

```python
# 同时作为 Scheduler + Worker（默认）
await scheduler.start()

# 只调度，不执行
await scheduler.start(roles=["scheduler"])

# 只执行，不调度
await scheduler.start(roles=["worker"])
```

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `roles` | `list[str] \| None` | `None` | 启动角色。`None` = `["scheduler", "worker"]`。可选值：`"scheduler"`（分片管理 + 任务调度）、`"worker"`（消费执行） |

**行为**：

- `scheduler` 角色启动 4 个后台协程：心跳续约、孤儿扫描、调度循环、补偿扫描
- `worker` 角色在 scheduler 模式下内嵌执行；纯 worker 模式下做补偿扫描
- **此方法会阻塞**直到调用 `stop()` 或进程退出

---

#### `await scheduler.stop()` — 停止调度器

```python
await scheduler.stop()
```

- 取消所有 shard 调度任务
- 主动释放所有 shard 锁（其他节点可立即接管）
- 关闭 Redis 连接

---

### `Task` — 一次性任务数据模型

```python
from redis_cron import Task
```

| 字段 | 类型 | 说明 |
|------|------|------|
| `task_id` | `str` | 唯一标识，默认 `uuid4().hex` |
| `task_type` | `str` | 任务类型 |
| `payload` | `dict[str, Any]` | 自定义数据 |
| `user_id` | `int` | 所属用户 ID |
| `shard_id` | `int` | 所在分片 |
| `fire_time` | `float` | 触发时间戳 |
| `created_at` | `float` | 创建时间戳 |
| `max_jitter` | `int` | 最大打散秒数 |

**方法**：

| 方法 | 说明 |
|------|------|
| `task.to_redis() → dict[str, str]` | 序列化为 Redis HSET 字段映射 |
| `Task.from_redis(task_id, data) → Task` | 从 Redis HGETALL 结果反序列化 |

---

### `CronTask(Task)` — 周期性任务数据模型

继承 `Task`，额外字段：

| 字段 | 类型 | 说明 |
|------|------|------|
| `cron` | `str` | Cron 表达式 |

---

## Redis 数据结构

```
Redis
├── ZSET   trigger:shard_{0..N}      score = 触发时间戳, member = task_id
├── HASH   task:{task_id}            任务详情 (task_type, payload, cron, user_id, ...)
├── HASH   processing:shard_{N}      执行中任务 (task_id → 开始时间戳)
├── STRING shard_lock:{N}            分片锁 (value = "node_id:fence_token", TTL = lock_ttl)
├── STRING shard_fence:{N}           Fencing Token 计数器 (INCR)
├── STRING node:{node_id}            节点注册 (value = "alive", TTL = lock_ttl)
└── STRING dedup:{task_id}:{fire_ts}  幂等去重 key (TTL = dedup_ttl)
```

---

## 架构概览

```
                    ┌──────────────────────┐
                    │     Your App         │
                    │  create_cron_task()  │
                    │  create_delayed_task()│
                    └──────────┬───────────┘
                               │
              ┌────────────────▼────────────────┐
              │         Redis (ZSET + HASH)      │
              │  trigger:shard_0  ...  shard_127 │
              └────────────────┬────────────────┘
                               │
         ┌─────────────────────┼─────────────────────┐
         │                     │                     │
   ┌─────▼─────┐        ┌─────▼─────┐        ┌─────▼─────┐
   │ Node A     │        │ Node B     │        │ Node C     │
   │ shard 0~42 │        │ shard 43~84│        │ shard 85~127│
   │            │        │            │        │            │
   │ ┌────────┐ │        │ ┌────────┐ │        │ ┌────────┐ │
   │ │心跳续约│ │        │ │心跳续约│ │        │ │心跳续约│ │
   │ │孤儿扫描│ │        │ │孤儿扫描│ │        │ │孤儿扫描│ │
   │ │调度循环│ │        │ │调度循环│ │        │ │调度循环│ │
   │ │补偿扫描│ │        │ │补偿扫描│ │        │ │补偿扫描│ │
   │ └────────┘ │        │ └────────┘ │        │ └────────┘ │
   └────────────┘        └────────────┘        └────────────┘
```

### 故障转移流程

```
Node B 宕机
  ↓ lock_ttl (15s) 后 shard_lock:43~84 过期
  ↓
Node A/C 的孤儿扫描发现无主 shard
  ↓ try_acquire 抢锁 + 新 Fencing Token
  ↓
Node A 接管 shard 43~63, Node C 接管 shard 64~84
  ↓
补偿扫描恢复 Node B 执行中的卡住任务
```

---

## 分角色部署

```python
# ============ 节点 A：纯 Scheduler ============
scheduler_a = RedisScheduler(
    redis_url="redis://redis-cluster:6379",
    node_id="scheduler-a",
)
await scheduler_a.start(roles=["scheduler"])

# ============ 节点 B：纯 Worker ============
scheduler_b = RedisScheduler(
    redis_url="redis://redis-cluster:6379",
    node_id="worker-b",
)

@scheduler_b.task("send_email")
async def send_email(task_id: str, payload: dict):
    ...

await scheduler_b.start(roles=["worker"])
```

---

## 配置建议

| 场景 | shard_count | batch_size | scan_interval | 说明 |
|------|-------------|------------|---------------|------|
| 开发/测试 | 4~16 | 50 | 0.5 | 低资源消耗 |
| 中小规模 (<100万任务) | 64~128 | 200 | 0.1 | 默认配置 |
| 大规模 (千万级任务) | 256~1024 | 500 | 0.05 | 需要多节点 |

### 参数调优关系

```
lock_ttl > heartbeat_interval × 3     （防止心跳抖动导致锁丢失）
processing_timeout > task_timeout       （让任务有机会自然完成）
dedup_ttl ≥ cron 最小间隔               （防止同一轮次重复执行）
scavenge_interval < lock_ttl            （及时发现宕机节点）
```

---

## 运行测试

```bash
# 安装开发依赖
pip install -e ".[dev]"

# 单元测试（fakeredis，无需真实 Redis）
pytest tests/test_scheduler.py -v

# 集成测试（需要真实 Redis）
REDIS_URL=redis://localhost:6379 pytest tests/test_integration.py -v
```

---

## 压测数据（ZhouTest4, 2 CPU / 8GB, Redis 7.4.7）

| 规模 | 写入速度 | 读取速度 | 内存占用 |
|------|---------|---------|---------|
| 1,000 任务 | 25,262 tasks/s | 7,118 tasks/s | 5.75 MB |
| 10,000 任务 | 24,970 tasks/s | 62,738 tasks/s | 7.90 MB |
| 100,000 任务 | 24,638 tasks/s | 122,646 tasks/s | 34.30 MB |
| 500,000 任务 | 24,044 tasks/s | 158,869 tasks/s | 146.32 MB |

**并发调度**（4 Scheduler 节点 × 100,000 任务）：45,949 tasks/s，锁冲突仅 384 次

---

## 许可证

MIT
