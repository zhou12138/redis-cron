# 01 - 整体架构

## 设计目标

| 指标 | 目标 |
|------|------|
| 任务总量 | 5000万+（千万用户 × 5任务/人） |
| 触发精度 | 秒级（P99 < 3s） |
| 可用性 | 99.99%（最多延迟，绝不丢失） |
| 扩展性 | 水平扩展，线性增长 |

## 架构全貌

```
┌─────────────────────────────────────────────────────┐
│                    API Gateway                       │
│               (任务 CRUD / 用户接口)                   │
└───────────────────────┬─────────────────────────────┘
                        │
             ┌──────────▼──────────┐
             │    任务元数据存储      │
             │  (MySQL 分库分表 /    │
             │   TiDB / MongoDB)    │
             │   兜底数据源           │
             └──────────┬──────────┘
                        │ 同步
         ┌──────────────▼──────────────┐
         │        Redis Cluster         │
         │                              │
         │  ZSET: trigger:shard_{0~N}   │  ← score = 触发时间戳
         │  HSET: task:{id} → 详情       │  ← 任务元数据
         │  STRING: shard_lock:{N}      │  ← 分片锁
         └──────────────┬──────────────┘
                        │
      ┌─────────────────┼─────────────────┐
      ▼                 ▼                 ▼
┌───────────┐    ┌───────────┐     ┌───────────┐
│ Scheduler │    │ Scheduler │     │ Scheduler │
│  Node 0   │    │  Node 1   │     │  Node N   │
│ shard 0~9 │    │shard 10~19│     │shard 120~ │
│           │    │           │     │           │
│ ┌───────┐ │    │ ┌───────┐ │     │ ┌───────┐ │
│ │扫描循环│ │    │ │扫描循环│ │     │ │扫描循环│ │
│ └───┬───┘ │    │ └───┬───┘ │     │ └───┬───┘ │
└─────┼─────┘    └─────┼─────┘     └─────┼─────┘
      │                │                 │
      ▼                ▼                 ▼
┌─────────────────────────────────────────────┐
│         消息队列 (Kafka / Redis Stream)       │
│        按优先级/类型分 Topic                   │
│        削峰填谷 + 重试保证                     │
└──────────────────────┬──────────────────────┘
                       │
      ┌────────────────┼────────────────┐
      ▼                ▼                ▼
┌──────────┐    ┌──────────┐     ┌──────────┐
│ Worker 0 │    │ Worker 1 │     │ Worker N │
│ 无状态    │    │ 无状态    │     │ 无状态    │
│ 幂等执行  │    │ 幂等执行  │     │ 幂等执行  │
└────┬─────┘    └────┬─────┘     └────┬─────┘
     │               │               │
     ▼               ▼               ▼
┌──────────────────────────────────────────┐
│      状态通知（HTTP回调/WebSocket/推送）    │
└──────────────────────────────────────────┘
```

## 各层职责

### API Gateway
- 任务 CRUD 接口
- 参数校验、Cron 表达式解析
- 写 DB + 同步写 Redis ZSET

### Redis Cluster（调度核心）
- ZSET 按时间排序，O(logN) 查询到期任务
- HSET 存任务详情，O(1) 读取
- 分布式锁协调 Scheduler 节点

### Scheduler 集群
- 每个节点负责若干 shard
- 周期扫描 ZSET，取出到期任务
- 投递到 MQ，计算 next_fire_time 放回 ZSET

### 消息队列
- 削峰填谷（整点百万任务不打爆 Worker）
- 消费失败自动重试 → 死信队列
- 按任务类型分 Topic

### Worker 集群
- 无状态，水平扩容
- 幂等设计：`task_id + fire_time` 去重
- 执行结果回写 DB + 触发状态通知

## 数据流

```
用户创建任务
     │
     ▼
  API Server
     │
     ├──► MySQL（持久化）
     │
     └──► Redis ZADD trigger:shard_{N} (score=next_fire_ts)
           Redis HSET task:{id} (详情)
                │
                ▼
         Scheduler 扫描
         ZRANGEBYSCORE + ZREM（Lua 原子）
                │
                ▼
           投递到 MQ
                │
                ▼
         Worker 消费执行
                │
                ├──► 回调通知用户
                ├──► 写执行日志到 DB
                └──► 计算 next_fire_time → ZADD 放回 ZSET
```
# 02 - Redis 数据模型

## 数据模型总览

```
Redis 数据结构：

┌─────────────────────────────────────────────────────┐
│                   Redis Cluster                      │
│                                                      │
│  ┌─────────────────────────────────────────┐         │
│  │  ZSET: trigger:shard_{0..127}           │         │
│  │  ┌──────────┬───────────────────┐       │         │
│  │  │  Score    │  Member           │       │         │
│  │  ├──────────┼───────────────────┤       │         │
│  │  │ 17152344 │  task_a1b2c3      │       │  触发队列│
│  │  │ 17152345 │  task_d4e5f6      │       │         │
│  │  │ 17152400 │  task_g7h8i9      │       │         │
│  │  └──────────┴───────────────────┘       │         │
│  └─────────────────────────────────────────┘         │
│                                                      │
│  ┌─────────────────────────────────────────┐         │
│  │  HASH: task:{task_id}                   │         │
│  │  ┌──────────┬───────────────────┐       │         │
│  │  │  Field   │  Value             │       │         │
│  │  ├──────────┼───────────────────┤       │  任务详情│
│  │  │ user_id  │  10001             │       │         │
│  │  │ cron     │  0 8 * * *         │       │         │
│  │  │ callback │  https://api/notify│       │         │
│  │  │ payload  │  {"key":"val"}     │       │         │
│  │  └──────────┴───────────────────┘       │         │
│  └─────────────────────────────────────────┘         │
│                                                      │
│  ┌─────────────────────────────────────────┐         │
│  │  HASH: processing:shard_{0..127}        │         │
│  │  ┌──────────┬───────────────────┐       │         │
│  │  │  Field   │  Value             │       │  执行中  │
│  │  │ task_id  │  取出时间戳          │       │         │
│  │  └──────────┴───────────────────┘       │         │
│  └─────────────────────────────────────────┘         │
│                                                      │
│  STRING: shard_lock:{N}   → "node_id:token"   分片锁  │
│  STRING: shard_fence:{N}  → 单调递增计数器     栅栏令牌 │
│                                                      │
└─────────────────────────────────────────────────────┘
```

## 写入任务

```python
import redis
import json

r = redis.Redis(cluster=True)

SHARD_COUNT = 128

def create_task(task_id: str, user_id: int, cron: str, callback: str, payload: dict):
    shard_id = user_id % SHARD_COUNT
    next_fire = calc_next_fire(cron)

    pipe = r.pipeline()
    # 任务详情
    pipe.hset(f"task:{task_id}", mapping={
        "user_id": str(user_id),
        "cron": cron,
        "callback": callback,
        "payload": json.dumps(payload),
        "shard_id": str(shard_id),
    })
    # 触发队列
    pipe.zadd(f"trigger:shard_{shard_id}", {task_id: next_fire})
    pipe.execute()
```

## 核心 Lua 脚本

### 原子取任务（带所有权校验）

```lua
-- KEYS[1] = shard_lock:{N}
-- KEYS[2] = trigger:shard_{N}
-- KEYS[3] = processing:shard_{N}
-- ARGV[1] = owner_val (node_id:token)
-- ARGV[2] = now (时间戳)
-- ARGV[3] = batch_size

-- 1. 校验调用者是否持有 shard 锁
local lock_val = redis.call('GET', KEYS[1])
if lock_val ~= ARGV[1] then
    return cjson.encode({error = "NOT_OWNER"})
end

-- 2. 取到期任务
local tasks = redis.call('ZRANGEBYSCORE', KEYS[2],
    '-inf', ARGV[2], 'LIMIT', 0, tonumber(ARGV[3]))

if #tasks == 0 then
    return cjson.encode({tasks = {}})
end

-- 3. 从触发队列移除
redis.call('ZREM', KEYS[2], unpack(tasks))

-- 4. 放入 processing 集合（两阶段提交）
for _, task_id in ipairs(tasks) do
    redis.call('HSET', KEYS[3], task_id, ARGV[2])
end

return cjson.encode({tasks = tasks})
```

### Python 调用

```python
FETCH_SCRIPT = """
local lock_val = redis.call('GET', KEYS[1])
if lock_val ~= ARGV[1] then
    return '{"error":"NOT_OWNER"}'
end
local tasks = redis.call('ZRANGEBYSCORE', KEYS[2],
    '-inf', ARGV[2], 'LIMIT', 0, tonumber(ARGV[3]))
if #tasks == 0 then
    return '{"tasks":[]}'
end
redis.call('ZREM', KEYS[2], unpack(tasks))
for _, task_id in ipairs(tasks) do
    redis.call('HSET', KEYS[3], task_id, ARGV[2])
end
return cjson.encode({tasks = tasks})
"""

# 注册脚本（避免每次传输）
fetch_sha = r.script_load(FETCH_SCRIPT)

def fetch_due_tasks(shard_id: int, owner_val: str, batch_size=200):
    now = time.time()
    result = r.evalsha(
        fetch_sha, 3,
        f"shard_lock:{shard_id}",
        f"trigger:shard_{shard_id}",
        f"processing:shard_{shard_id}",
        owner_val, str(now), str(batch_size)
    )
    return json.loads(result)
```

### 任务确认（ACK）

```lua
-- KEYS[1] = processing:shard_{N}
-- KEYS[2] = trigger:shard_{N}
-- ARGV[1] = task_id
-- ARGV[2] = next_fire_time (0 表示不再触发)

-- 从 processing 移除
redis.call('HDEL', KEYS[1], ARGV[1])

-- 如果有下次触发时间，放回触发队列
local next_fire = tonumber(ARGV[2])
if next_fire > 0 then
    redis.call('ZADD', KEYS[2], next_fire, ARGV[1])
end

return 1
```

## 容量估算

```
千万用户 × 5 任务/人 = 5000万任务

┌──────────────────────────────────────────────────┐
│  数据结构         │  单条大小   │  总量          │
├──────────────────┼───────────┼───────────────┤
│  ZSET (trigger)   │  ~80 B    │  50M × 80B    │
│  128 shards       │  member+  │  ≈ 4 GB       │
│                   │  score    │               │
├──────────────────┼───────────┼───────────────┤
│  HSET (task:*)    │  ~300 B   │  50M × 300B   │
│  任务详情          │  5 fields │  ≈ 15 GB      │
├──────────────────┼───────────┼───────────────┤
│  HSET (process)   │  ~60 B    │  峰值 ~1M     │
│  执行中任务        │  task_id+ │  ≈ 60 MB      │
│                   │  timestamp│               │
├──────────────────┼───────────┼───────────────┤
│  STRING (locks)   │  ~50 B    │  128 × 50B    │
│  分片锁           │           │  ≈ 6 KB       │
├──────────────────┴───────────┴───────────────┤
│  总计 ≈ 19 GB                                 │
│  Redis Cluster 3主3从（每主 ~7GB）绰绰有余     │
└──────────────────────────────────────────────┘
```

### QPS 估算

```
最坏情况：整点风暴
├── 20% 任务集中在整点 = 1000万任务/分钟
├── = ~167,000 / 秒
├── 128 shard → 每 shard ~1,300/秒
├── ZRANGEBYSCORE 复杂度 O(logN + M)
└── Redis 单节点轻松支撑 10万+ QPS → ✅ 无压力

正常情况：
├── 任务均匀分布
├── 5000万 / 86400秒 ≈ 580/秒
└── 几乎无压力
```

## Key 设计注意事项

### Redis Cluster 槽位

```
问题：trigger:shard_0 和 task:xxx 可能不在同一个 slot
     → Lua 脚本只能操作同一 slot 的 key

方案1：用 hash tag 强制同 slot
  trigger:{shard_0}
  processing:{shard_0}
  shard_lock:{shard_0}
  → 三个 key 通过 {shard_0} 落在同一 slot

方案2：分开查询（推荐）
  Lua 只操作同 shard 的 trigger + processing + lock
  task:{id} 详情单独查（可以在任意 slot）
```
# 03 - 分片与节点管理

## 分片策略

```
┌─────────────────────────────────────────────┐
│            用户 → Shard 映射                  │
│                                              │
│   user_id = 10001                            │
│   shard_id = 10001 % 128 = 17               │
│                                              │
│   该用户的所有任务都在 trigger:shard_17       │
│   由负责 shard_17 的 Scheduler 节点调度       │
└─────────────────────────────────────────────┘

分片数量选择：
┌──────────┬────────────────┬──────────────────┐
│ 分片数    │ 每 shard 任务数 │ 适用场景          │
├──────────┼────────────────┼──────────────────┤
│ 64       │ ~78万           │ < 1000万任务      │
│ 128      │ ~39万           │ 千万级（推荐）     │
│ 256      │ ~20万           │ 亿级              │
│ 1024     │ ~5万            │ 超大规模           │
└──────────┴────────────────┴──────────────────┘
```

## 纯 Redis 节点管理（无 etcd/ZK）

### 整体流程

```
┌──────────────────────────────────────────────────┐
│                 纯 Redis 方案                      │
│                                                    │
│  Node 启动                                         │
│    │                                               │
│    ├─► 1. 注册自己: SET node:{id} alive EX 15      │
│    │                                               │
│    ├─► 2. 扫描无主 shard，尝试抢锁                   │
│    │   for shard in range(128):                    │
│    │     SET shard_lock:{shard} node:token NX EX 15│
│    │                                               │
│    ├─► 3. 启动调度循环（负责抢到的 shard）            │
│    │                                               │
│    └─► 4. 后台心跳：每 5s 续约锁 + 捡无主 shard      │
│                                                    │
│  Node 宕机                                         │
│    │                                               │
│    └─► 15s 后锁过期 → 其他 Node 自动接管             │
└──────────────────────────────────────────────────┘
```

### 完整实现

```python
import asyncio
import time
import json
import redis.asyncio as redis

class SchedulerNode:
    def __init__(self, node_id: str, redis_url: str, shard_count=128):
        self.node_id = node_id
        self.redis = redis.from_url(redis_url)
        self.shard_count = shard_count
        self.my_shards: dict[int, int] = {}  # shard_id → fencing_token
        self.running = True

    async def start(self):
        """启动节点"""
        # 注册自己
        await self.redis.set(f"node:{self.node_id}", "alive", ex=15)

        # 并行启动三个循环
        await asyncio.gather(
            self.heartbeat_loop(),
            self.scavenge_loop(),
            self.schedule_all_shards(),
        )

    # ==========================================
    # 抢锁与续约
    # ==========================================

    ACQUIRE_SCRIPT = """
    local current = redis.call('GET', KEYS[1])
    if current == false then
        local token = redis.call('INCR', KEYS[2])
        redis.call('SET', KEYS[1], ARGV[1] .. ':' .. token, 'EX', tonumber(ARGV[2]))
        return token
    end
    return -1
    """

    async def try_acquire(self, shard_id: int, ttl=15) -> int | None:
        """尝试获取 shard 锁，返回 fencing token"""
        token = await self.redis.eval(
            self.ACQUIRE_SCRIPT, 2,
            f"shard_lock:{shard_id}",
            f"shard_fence:{shard_id}",
            self.node_id, str(ttl)
        )
        if token > 0:
            self.my_shards[shard_id] = token
            return token
        return None

    RENEW_SCRIPT = """
    local val = redis.call('GET', KEYS[1])
    if val == ARGV[1] then
        redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
        return 1
    end
    return 0
    """

    async def renew(self, shard_id: int, ttl=15) -> bool:
        """续约 shard 锁"""
        token = self.my_shards.get(shard_id)
        if token is None:
            return False
        owner_val = f"{self.node_id}:{token}"
        ok = await self.redis.eval(
            self.RENEW_SCRIPT, 1,
            f"shard_lock:{shard_id}",
            owner_val, str(ttl)
        )
        return bool(ok)

    # ==========================================
    # 心跳循环
    # ==========================================

    async def heartbeat_loop(self):
        """
        每 5s：
        1. 续约自己的注册
        2. 续约所有持有的 shard 锁
        3. 失去的 shard 立即停止调度
        """
        while self.running:
            # 续约节点注册
            await self.redis.set(f"node:{self.node_id}", "alive", ex=15)

            # 续约每个 shard
            lost = []
            for shard_id in list(self.my_shards.keys()):
                ok = await self.renew(shard_id)
                if not ok:
                    lost.append(shard_id)
                    print(f"⚠️ 丢失 shard {shard_id} 所有权")

            for shard_id in lost:
                self.my_shards.pop(shard_id, None)

            await asyncio.sleep(5)

    # ==========================================
    # 捡无主 shard
    # ==========================================

    async def scavenge_loop(self):
        """每 10s 扫描无主 shard 并尝试接管"""
        while self.running:
            for shard_id in range(self.shard_count):
                if shard_id in self.my_shards:
                    continue
                owner = await self.redis.get(f"shard_lock:{shard_id}")
                if owner is None:
                    token = await self.try_acquire(shard_id)
                    if token:
                        print(f"✅ 接管孤儿 shard {shard_id}, token={token}")

            await asyncio.sleep(10)

    # ==========================================
    # 调度循环
    # ==========================================

    async def schedule_all_shards(self):
        """为每个持有的 shard 启动调度协程"""
        tasks = {}
        while self.running:
            # 启动新 shard 的调度
            for shard_id in list(self.my_shards.keys()):
                if shard_id not in tasks:
                    tasks[shard_id] = asyncio.create_task(
                        self.schedule_shard(shard_id)
                    )

            # 清理失去的 shard
            for shard_id in list(tasks.keys()):
                if shard_id not in self.my_shards:
                    tasks[shard_id].cancel()
                    del tasks[shard_id]

            await asyncio.sleep(1)

    async def schedule_shard(self, shard_id: int):
        """单个 shard 的调度循环"""
        while shard_id in self.my_shards:
            token = self.my_shards[shard_id]
            owner_val = f"{self.node_id}:{token}"

            result = await self.fetch_due_tasks(shard_id, owner_val)

            if result.get("error") == "NOT_OWNER":
                print(f"🛑 shard {shard_id} 所有权校验失败，停止")
                self.my_shards.pop(shard_id, None)
                return

            for task_id in result.get("tasks", []):
                await self.dispatch_to_mq(task_id, shard_id)

            if not result.get("tasks"):
                await asyncio.sleep(0.1)  # 100ms 精度
```

## 节点扩缩容

### 扩容（加节点）

```
Before:  Node-A [shard 0~63],  Node-B [shard 64~127]
                                         │
Add Node-C ──────────────────────────────┘

过程（自动完成）：
1. Node-C 启动，扫描所有 shard
2. 所有 shard 都有主 → 无法抢到
3. 等待 Node-A/B 某些 shard 锁过期（手动释放或自然过期）
4. Node-C 接管部分 shard

推荐做法：主动释放
  Node-A 收到"缩减"信号 → 停止续约 shard 32~63
  → 15s 后 Node-C 自动接管
```

### 缩容（减节点）

```
停掉 Node-C：
1. Node-C 进程退出
2. 15s 后 shard 锁过期
3. Node-A/B scavenge_loop 发现无主 shard
4. 抢锁接管
5. 全程自动，最大延迟 ≈ 25s（15s 过期 + 10s 扫描间隔）
```

### 均衡分配算法

```python
async def rebalance(self, all_nodes: list[str]):
    """
    目标：每个节点持有的 shard 数量差不超过 1

    128 shards / 3 nodes:
    Node-A: 43, Node-B: 43, Node-C: 42
    """
    target = self.shard_count // len(all_nodes)
    excess = self.shard_count % len(all_nodes)

    my_index = all_nodes.index(self.node_id)
    my_target = target + (1 if my_index < excess else 0)

    current = len(self.my_shards)
    if current > my_target:
        # 释放多余的 shard
        to_release = current - my_target
        for shard_id in list(self.my_shards.keys())[:to_release]:
            await self.redis.delete(f"shard_lock:{shard_id}")
            self.my_shards.pop(shard_id)
            print(f"📤 释放 shard {shard_id}")
```

## Shard 分配可视化

```
3 节点 × 128 shards 的稳态分配：

Node-A  ████████████████████████████████████████████  shard 0~42   (43个)
Node-B  ████████████████████████████████████████████  shard 43~85  (43个)
Node-C  ██████████████████████████████████████████    shard 86~127 (42个)

Node-B 宕机后：

Node-A  ████████████████████████████████████████████████████████████████  shard 0~63   (64个)
Node-B  ✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗✗  DEAD
Node-C  ████████████████████████████████████████████████████████████████  shard 64~127 (64个)
```
# 04 - 故障恢复与高可用

## 故障类型与应对

```
┌──────────────────────────────────────────────────────┐
│                   故障分类                             │
│                                                       │
│  ┌─────────────┐  ┌─────────────┐  ┌──────────────┐  │
│  │ Scheduler   │  │ Redis       │  │ MQ / Worker  │  │
│  │ 节点宕机     │  │ 节点故障     │  │ 消费失败      │  │
│  │             │  │             │  │              │  │
│  │ → shard 锁  │  │ → 主从切换   │  │ → 重试 +     │  │
│  │   自动过期   │  │ → Sentinel  │  │   死信队列    │  │
│  │ → 其他节点   │  │ → Cluster   │  │ → 补偿扫描   │  │
│  │   接管       │  │   failover  │  │              │  │
│  └─────────────┘  └─────────────┘  └──────────────┘  │
└──────────────────────────────────────────────────────┘
```

## Scheduler 节点宕机

### 宕机时间线

```
T+0s     Node-B 进程崩溃（OOM / 硬件故障 / 网络断开）
         │
         │  锁还在 Redis，其他节点看不到异常
         │
T+15s    shard_lock 过期（TTL=15s）
         │
         │  Node-A 和 Node-C 的 scavenge_loop 发现无主 shard
         │
T+25s    最迟：Node-A/C 接管所有孤儿 shard（scavenge 间隔 10s）
         │
         │  开始执行 ZRANGEBYSCORE，补执行 T+0 ~ T+25 积压的任务
         │
T+26s    恢复正常调度

最大影响窗口：~25 秒（任务延迟，不丢失）
```

### ZSET 天然兜底

```
为什么任务不会丢？

  ZSET 里的任务只有被 ZREM 才会消失
  Node-B 挂了 → 没人执行 ZREM → 任务一直在 ZSET 里
  新 owner 接管后 ZRANGEBYSCORE('-inf', now) → 把积压的全取出来

  ┌───────────────────────────────────┐
  │  trigger:shard_17                 │
  │                                   │
  │  Score(时间)     Task              │
  │  ─────────────  ──────            │
  │  T+2s           task_001  ← 积压  │
  │  T+5s           task_002  ← 积压  │
  │  T+10s          task_003  ← 积压  │
  │  T+20s          task_004  ← 积压  │
  │  T+30s          task_005  ← 正常  │
  │                                   │
  │  新 owner 在 T+25s 接管           │
  │  一次取出 task_001~004，全部补执行  │
  └───────────────────────────────────┘
```

## Fencing Token 防脑裂

### 问题场景

```
T+0s    Node-A 获取 shard-5 锁，token=42
T+12s   Node-A 网络抖动，续约失败（但进程还活着）
T+15s   锁过期
T+16s   Node-B 获取 shard-5 锁，token=43
T+17s   Node-A 网络恢复，继续执行调度
        ↓
        两个节点同时操作 shard-5 → 任务重复触发！
```

### 解决方案：Lua 原子校验

```
┌────────────────────────────────────────────────────┐
│              Fencing Token 机制                      │
│                                                     │
│  shard_fence:{N} → 单调递增计数器                     │
│                                                     │
│  获取锁时：                                          │
│    token = INCR shard_fence:{N}                     │
│    SET shard_lock:{N} "node_id:token" EX 15         │
│                                                     │
│  操作任务时（Lua 原子脚本内）：                        │
│    lock_val = GET shard_lock:{N}                    │
│    if lock_val ≠ "my_node:my_token" → 拒绝操作       │
│                                                     │
│  ┌──────────┐     ┌──────────┐                      │
│  │  Node-A  │     │  Node-B  │                      │
│  │ token=42 │     │ token=43 │                      │
│  └────┬─────┘     └────┬─────┘                      │
│       │                │                            │
│       ▼                ▼                            │
│  fetch_tasks()    fetch_tasks()                     │
│  Lua: GET lock    Lua: GET lock                     │
│  值="B:43"        值="B:43"                          │
│  ≠ "A:42"         = "B:43"                          │
│  → NOT_OWNER ❌   → 执行 ✅                          │
│                                                     │
│  Node-A 感知到丢失，立即停止 shard-5 调度              │
└────────────────────────────────────────────────────┘
```

### 完整实现

```python
FETCH_AND_FIRE = """
-- 校验所有权（fencing）
local lock_val = redis.call('GET', KEYS[1])
if lock_val ~= ARGV[1] then
    return '{"error":"NOT_OWNER"}'
end

-- 原子取任务 + 移到 processing
local tasks = redis.call('ZRANGEBYSCORE', KEYS[2],
    '-inf', ARGV[2], 'LIMIT', 0, tonumber(ARGV[3]))

if #tasks == 0 then
    return '{"tasks":[]}'
end

redis.call('ZREM', KEYS[2], unpack(tasks))
for _, task_id in ipairs(tasks) do
    redis.call('HSET', KEYS[3], task_id, ARGV[2])
end

return cjson.encode({tasks = tasks})
"""

async def schedule_shard(self, shard_id: int):
    while shard_id in self.my_shards:
        token = self.my_shards[shard_id]
        owner_val = f"{self.node_id}:{token}"

        result = json.loads(await self.redis.eval(
            FETCH_AND_FIRE, 3,
            f"shard_lock:{shard_id}",
            f"trigger:shard_{shard_id}",
            f"processing:shard_{shard_id}",
            owner_val, str(time.time()), "200"
        ))

        if result.get("error") == "NOT_OWNER":
            # 立即感知丢失，停止调度
            self.my_shards.pop(shard_id, None)
            print(f"🛑 shard {shard_id} 被抢占，停止调度")
            return

        for task_id in result.get("tasks", []):
            await self.dispatch_to_mq(task_id, shard_id)

        if not result.get("tasks"):
            await asyncio.sleep(0.1)
```

## 两阶段提交防任务丢失

### 问题场景

```
T+0s    Node-A: ZREM task_100（从触发队列移除）
T+1s    Node-A: 准备投递到 MQ...
T+2s    Node-A 崩溃 💥
        ↓
        task_100 既不在 ZSET，也没到 MQ → 丢了！
```

### 解决方案：processing 中间态

```
┌──────────────────────────────────────────────────┐
│              两阶段提交                            │
│                                                   │
│  阶段1: ZSET → processing（Lua 原子操作）          │
│                                                   │
│  trigger:shard_17          processing:shard_17    │
│  ┌──────────────┐          ┌──────────────┐      │
│  │ task_100 ──────────────►│ task_100     │      │
│  │ task_101     │  ZREM +  │ ts=1715234   │      │
│  │ task_102     │  HSET    │              │      │
│  └──────────────┘          └──────────────┘      │
│                                                   │
│  阶段2: 投递 MQ 成功后 → 清理 processing           │
│                                                   │
│  processing:shard_17                              │
│  ┌──────────────┐                                 │
│  │ (已清理)      │  ← HDEL task_100               │
│  └──────────────┘                                 │
│                                                   │
│  如果阶段2之前崩溃 → processing 里还有 task_100    │
│  → 补偿扫描发现 → 重新投递                          │
└──────────────────────────────────────────────────┘
```

### 补偿扫描

```python
async def recover_stuck_tasks(self, shard_id: int):
    """
    每 60s 执行一次
    扫描 processing 中超过 60s 未确认的任务 → 放回 ZSET 重新触发
    """
    stuck = await self.redis.hgetall(f"processing:shard_{shard_id}")
    now = time.time()
    recovered = 0

    for task_id, ts in stuck.items():
        task_id = task_id.decode()
        if now - float(ts) > 60:
            # 放回触发队列（score=0 表示立即触发）
            pipe = self.redis.pipeline()
            pipe.zadd(f"trigger:shard_{shard_id}", {task_id: 0})
            pipe.hdel(f"processing:shard_{shard_id}", task_id)
            await pipe.execute()
            recovered += 1

    if recovered:
        print(f"🔄 shard {shard_id}: 恢复 {recovered} 个卡住的任务")
```

## 三道防线总结

```
┌──────────────────────────────────────────────────────┐
│                                                       │
│  防线 1：ZSET 天然堆积                                 │
│  ├── 没被 ZREM 的任务不会丢                             │
│  └── 新 owner 接管后自动补执行                           │
│                                                       │
│  防线 2：processing 集合 + 补偿扫描                      │
│  ├── ZREM 后 MQ 投递前崩溃 → processing 里有记录        │
│  ├── 60s 补偿扫描 → 放回 ZSET 重触发                    │
│  └── Worker 幂等 → 重复触发不会重复执行                   │
│                                                       │
│  防线 3：DB 全量对账（终极保障）                          │
│  ├── 每 10 分钟 MySQL 扫描                              │
│  │   WHERE next_fire_time < NOW() - 5min               │
│  │     AND last_executed < next_fire_time               │
│  └── 发现遗漏 → 写回 Redis ZSET 补触发                  │
│                                                       │
│  结果：最多延迟，绝不丢失                                │
└──────────────────────────────────────────────────────┘
```

## Worker 幂等设计

```python
async def execute_task(task_id: str, fire_time: float):
    """
    幂等 key: task_id + fire_time
    同一任务同一触发时间只执行一次
    """
    dedup_key = f"dedup:{task_id}:{int(fire_time)}"

    # SET NX: 只有第一个 Worker 能设置成功
    acquired = await redis.set(dedup_key, "1", nx=True, ex=3600)
    if not acquired:
        print(f"⏭️ 跳过重复任务 {task_id}")
        return

    # 执行任务逻辑
    try:
        task_info = await redis.hgetall(f"task:{task_id}")
        result = await do_callback(task_info)

        # ACK: 从 processing 移除 + 计算下次触发
        next_fire = calc_next_fire(task_info["cron"], fire_time)
        await ack_task(task_id, shard_id, next_fire)

    except Exception as e:
        # 执行失败 → 删除去重 key，允许重试
        await redis.delete(dedup_key)
        raise
```
# 06 - 整点风暴与性能优化

## 整点风暴问题

```
用户行为分布：

00:00  ████████████████████████████████  32%   ← 日终结算/清理
06:00  ██████████                         10%
08:00  ████████████████████████           24%   ← 早间推送/报表
09:00  ██████████████████                 18%
12:00  ████████                            8%
18:00  ████████                            8%

整点 1 秒内可能触发 100万+ 任务
不做处理 → MQ 打满 → Worker 雪崩 → 用户无响应
```

## 打散策略

### 秒级随机打散

```python
import random
from croniter import croniter

def create_task_with_jitter(user_id: int, cron_expr: str, max_jitter=60):
    """
    用户设置 "每天8点" → 实际触发 8:00:00 ~ 8:00:59 随机

    ┌──────────────────────────────────────────┐
    │  原始分布（无打散）                        │
    │                                           │
    │  08:00:00  ████████████████  100万任务     │
    │  08:00:01                                 │
    │  08:00:02                                 │
    │  ...                                      │
    │                                           │
    │  打散后分布                                │
    │                                           │
    │  08:00:00  ██  ~1.7万                     │
    │  08:00:01  ██  ~1.7万                     │
    │  08:00:02  ██  ~1.7万                     │
    │  ...                                      │
    │  08:00:59  ██  ~1.7万                     │
    │                                           │
    │  峰值从 100万/s 降到 1.7万/s（60x 削峰）   │
    └──────────────────────────────────────────┘
    """
    base_time = croniter(cron_expr).get_next(float)
    jitter = random.randint(0, max_jitter)
    actual_fire_time = base_time + jitter

    # jitter 固定到用户，同一用户每天同一秒触发
    # 避免用户感知到随机性
    stable_jitter = user_id % max_jitter
    actual_fire_time = base_time + stable_jitter

    return actual_fire_time
```

### Scheduler 分批投递

```python
async def schedule_shard_with_throttle(shard_id: int, owner_val: str):
    """
    即使同一秒有大量到期任务，也分批投递到 MQ
    每批 200 个，批间 10ms 间隔
    """
    while True:
        result = await fetch_due_tasks(shard_id, owner_val, batch_size=200)
        tasks = result.get("tasks", [])

        if not tasks:
            await asyncio.sleep(0.1)
            continue

        # 分批投递
        for task_id in tasks:
            await mq.send("task-execute", {
                "task_id": task_id,
                "shard_id": shard_id,
                "fire_time": time.time()
            })

        # 如果取满了（200个），说明还有更多，短暂 sleep 后继续
        if len(tasks) == 200:
            await asyncio.sleep(0.01)  # 10ms 间隔，防止打满 MQ
```

## 幂等防重

```
┌──────────────────────────────────────────────────────┐
│                 幂等防重机制                           │
│                                                       │
│  场景：同一任务因补偿/重试被投递多次                      │
│                                                       │
│  ┌─────────┐   ┌─────────┐   ┌─────────┐             │
│  │ Worker A │   │ Worker B │   │ Worker C │             │
│  │ task_100 │   │ task_100 │   │ task_100 │             │
│  └────┬─────┘   └────┬─────┘   └────┬─────┘             │
│       │              │              │                    │
│       ▼              ▼              ▼                    │
│  Redis SET NX:  dedup:task_100:1715234400                │
│       │              │              │                    │
│    成功 ✅         失败 ❌        失败 ❌                  │
│    执行任务        跳过            跳过                    │
│                                                       │
└──────────────────────────────────────────────────────┘
```

```python
async def execute_with_dedup(task_id: str, fire_time: float):
    dedup_key = f"dedup:{task_id}:{int(fire_time)}"

    # NX: 只有一个 Worker 能设置成功
    # EX: 自动过期，避免永久占用
    if not await redis.set(dedup_key, "1", nx=True, ex=3600):
        return  # 已有其他 Worker 在执行

    try:
        await do_execute(task_id)
    except Exception:
        # 失败 → 删除 dedup key，允许重试
        await redis.delete(dedup_key)
        raise
```

## MQ 削峰

```
┌──────────────────────────────────────────────────────┐
│                                                       │
│  无 MQ（直接执行）:                                    │
│                                                       │
│  Scheduler ──► Worker                                │
│  100万任务/s   最多处理 1万/s                          │
│  → Worker 崩溃                                       │
│                                                       │
│  ─────────────────────────────────────────────       │
│                                                       │
│  有 MQ（缓冲）:                                       │
│                                                       │
│  Scheduler ──► MQ ──► Worker                         │
│  100万任务/s   缓冲    1万/s 稳定消费                  │
│               堆积     自动反压                        │
│                                                       │
│  ┌────────────────────────────────────┐               │
│  │  MQ 积压量                         │               │
│  │                                    │               │
│  │  ████                              │               │
│  │  ████████                          │  生产峰值      │
│  │  ████████████                      │               │
│  │  ████████████████                  │               │
│  │  ████████████████                  │  ← 峰值       │
│  │  ██████████████                    │               │
│  │  ████████████                      │  消费追赶      │
│  │  ████████                          │               │
│  │  ████                              │               │
│  │  ██                                │               │
│  │  ▏                                 │  ← 消化完毕    │
│  └────────────────────────────────────┘               │
│                                                       │
│  MQ 选型：                                            │
│  ├── Kafka:     超高吞吐，适合大规模                    │
│  ├── Redis Stream: 轻量，纯 Redis 方案首选             │
│  └── RocketMQ:  延迟消息原生支持                       │
└──────────────────────────────────────────────────────┘
```

### Redis Stream 作为轻量 MQ

```python
# 生产者（Scheduler）
async def dispatch_to_stream(task_id: str, shard_id: int):
    await redis.xadd("task_stream", {
        "task_id": task_id,
        "shard_id": str(shard_id),
        "fire_time": str(time.time())
    }, maxlen=1000000)  # 保留最近 100万条

# 消费者（Worker）
async def consume_tasks(worker_id: str, group="workers"):
    # 创建消费者组
    try:
        await redis.xgroup_create("task_stream", group, id="0", mkstream=True)
    except Exception:
        pass  # 组已存在

    while True:
        # 读取待处理消息
        messages = await redis.xreadgroup(
            group, worker_id,
            {"task_stream": ">"},
            count=10, block=1000  # 每次取 10 条，阻塞 1s
        )

        for stream, msgs in messages:
            for msg_id, data in msgs:
                try:
                    await execute_with_dedup(data["task_id"], float(data["fire_time"]))
                    await redis.xack("task_stream", group, msg_id)
                except Exception as e:
                    print(f"执行失败，等待重试: {e}")
                    # 不 ACK → 自动重投（pending 状态）
```

## 监控指标体系

```
┌──────────────────────────────────────────────────────┐
│                 核心监控大盘                           │
│                                                       │
│  ┌─────────────────────────────────────────────┐     │
│  │  调度延迟 (Scheduling Latency)               │     │
│  │                                              │     │
│  │  P50: 0.1s  P90: 0.5s  P99: 2s  Max: 5s    │     │
│  │  目标: P99 < 3s                              │     │
│  │                                              │     │
│  │  计算: 实际触发时间 - 预期触发时间              │     │
│  └─────────────────────────────────────────────┘     │
│                                                       │
│  ┌─────────────────────────────────────────────┐     │
│  │  任务成功率 (Success Rate)                   │     │
│  │                                              │     │
│  │  总触发: 1,234,567   成功: 1,233,333         │     │
│  │  失败: 1,234   成功率: 99.9%                 │     │
│  │  目标: > 99.9%                               │     │
│  └─────────────────────────────────────────────┘     │
│                                                       │
│  ┌─────────────────────────────────────────────┐     │
│  │  队列积压 (Queue Backlog)                    │     │
│  │                                              │     │
│  │  当前积压: 12,345 条                          │     │
│  │  消费速率: 5,000 条/s                         │     │
│  │  预计消化时间: 2.5s                           │     │
│  │  告警阈值: > 10万 条                          │     │
│  └─────────────────────────────────────────────┘     │
│                                                       │
│  ┌─────────────────────────────────────────────┐     │
│  │  分片负载均衡 (Shard Balance)                 │     │
│  │                                              │     │
│  │  Node-A: 43 shards  ████████████████████     │     │
│  │  Node-B: 43 shards  ████████████████████     │     │
│  │  Node-C: 42 shards  ███████████████████      │     │
│  │  偏差率: 2.3%  目标: < 10%                    │     │
│  └─────────────────────────────────────────────┘     │
└──────────────────────────────────────────────────────┘
```

### 监控实现

```python
import time
from dataclasses import dataclass, field
from collections import defaultdict

@dataclass
class Metrics:
    # 调度延迟
    latencies: list[float] = field(default_factory=list)

    # 任务计数
    fired: int = 0
    success: int = 0
    failed: int = 0

    # 分片统计
    shard_task_count: dict[int, int] = field(default_factory=lambda: defaultdict(int))

    def record_fire(self, expected_time: float, actual_time: float):
        self.fired += 1
        self.latencies.append(actual_time - expected_time)

    def record_result(self, success: bool):
        if success:
            self.success += 1
        else:
            self.failed += 1

    def report(self) -> dict:
        latencies = sorted(self.latencies)
        n = len(latencies)
        return {
            "latency_p50": latencies[int(n * 0.5)] if n else 0,
            "latency_p90": latencies[int(n * 0.9)] if n else 0,
            "latency_p99": latencies[int(n * 0.99)] if n else 0,
            "success_rate": self.success / max(self.fired, 1),
            "total_fired": self.fired,
            "total_failed": self.failed,
        }

# 每分钟上报到 Redis（供 Grafana 读取）
async def report_metrics(metrics: Metrics):
    report = metrics.report()
    await redis.hset("metrics:scheduler", mapping={
        k: str(v) for k, v in report.items()
    })
    await redis.expire("metrics:scheduler", 300)
```

### 告警规则

```
┌──────────────────┬──────────────┬──────────────┐
│ 指标              │ 告警阈值      │ 级别          │
├──────────────────┼──────────────┼──────────────┤
│ 调度延迟 P99     │ > 5s          │ ⚠️ Warning   │
│ 调度延迟 P99     │ > 30s         │ 🔴 Critical  │
│ 任务成功率       │ < 99.5%       │ ⚠️ Warning   │
│ 任务成功率       │ < 99%         │ 🔴 Critical  │
│ MQ 积压量        │ > 10万        │ ⚠️ Warning   │
│ MQ 积压量        │ > 100万       │ 🔴 Critical  │
│ Shard 无主时长   │ > 30s         │ 🔴 Critical  │
│ processing 卡住  │ > 5min        │ 🔴 Critical  │
│ Redis 内存使用   │ > 80%         │ ⚠️ Warning   │
│ Node 数量变化    │ any           │ ℹ️ Info      │
└──────────────────┴──────────────┴──────────────┘
```
