#!/usr/bin/env python3
"""redis-cron v0.2.0 全功能压测脚本 (带审计日志)

需要真实 Redis 实例。用法:
    python3 bench.py [redis_url]

默认连接 redis://localhost:6379

输出审计文件: bench_audit.md
"""

import asyncio
import json
import os
import sys
import time
import statistics
from datetime import datetime
from dataclasses import dataclass, field

import redis.asyncio as aioredis

# 加到 path
sys.path.insert(0, os.path.dirname(__file__))

from redis_cron.scheduler import RedisScheduler
from redis_cron.models import Task, CronTask
from redis_cron.utils import calc_shard_id

REDIS_URL = sys.argv[1] if len(sys.argv) > 1 else "redis://localhost:6379"
AUDIT_FILE = os.path.join(os.path.dirname(__file__), "bench_audit.md")

# ============================================================
# Audit Logger
# ============================================================

class AuditLogger:
    def __init__(self, path: str):
        self.path = path
        self.lines: list[str] = []
        self.start_time = time.time()

    def header(self, text: str):
        self.lines.append(f"\n## {text}\n")

    def subheader(self, text: str):
        self.lines.append(f"\n### {text}\n")

    def log(self, msg: str):
        elapsed = time.time() - self.start_time
        ts = f"[{elapsed:8.3f}s]"
        self.lines.append(f"- {ts} {msg}")

    def table(self, headers: list[str], rows: list[list[str]]):
        sep = " | ".join(["---"] * len(headers))
        hdr = " | ".join(headers)
        self.lines.append(f"\n| {hdr} |")
        self.lines.append(f"| {sep} |")
        for row in rows:
            self.lines.append(f"| {' | '.join(row)} |")
        self.lines.append("")

    def result(self, label: str, value: str):
        self.lines.append(f"  - **{label}**: {value}")

    def save(self):
        preamble = [
            f"# redis-cron v0.2.0 压测审计报告",
            f"",
            f"- **生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"- **Redis**: `{REDIS_URL}`",
            f"- **总耗时**: {time.time() - self.start_time:.1f}s",
            f"",
        ]
        with open(self.path, "w") as f:
            f.write("\n".join(preamble + self.lines) + "\n")


audit = AuditLogger(AUDIT_FILE)


# ============================================================
# Helper
# ============================================================

async def cleanup(r: aioredis.Redis, prefix: str = "bench_"):
    """清理所有压测数据"""
    cursor = 0
    deleted = 0
    while True:
        cursor, keys = await r.scan(cursor, match=f"*{prefix}*", count=1000)
        if keys:
            await r.delete(*keys)
            deleted += len(keys)
        if cursor == 0:
            break
    # 也清理 trigger/processing/user_tasks/task_history/dedup
    for pattern in ["trigger:shard_*", "processing:shard_*", "user_tasks:*", "task_history:*", "dedup:*", "task:*"]:
        cursor = 0
        while True:
            cursor, keys = await r.scan(cursor, match=pattern, count=1000)
            if keys:
                await r.delete(*keys)
                deleted += len(keys)
            if cursor == 0:
                break
    return deleted


async def timed(label: str, coro):
    """执行并计时"""
    t0 = time.time()
    result = await coro
    dt = time.time() - t0
    return result, dt


# ============================================================
# Benchmark Suites
# ============================================================

async def bench_create_tasks(scheduler: RedisScheduler, r: aioredis.Redis):
    """测试 1: 批量创建任务性能"""
    audit.header("1. 批量创建任务")

    rows = []
    for count in [1_000, 5_000, 10_000]:
        await cleanup(r)

        t0 = time.time()
        for i in range(count):
            await scheduler.create_cron_task(
                task_type="bench_email",
                cron="*/5 * * * *",
                user_id=i % 10000,
                payload={"idx": i},
                task_id=f"bench_cron_{i}",
                max_retries=3,
                retry_delay=30,
            )
        dt = time.time() - t0
        rate = count / dt
        audit.log(f"创建 {count:,} 个 cron 任务: {dt:.2f}s ({rate:,.0f} tasks/s)")
        rows.append([f"{count:,}", f"{dt:.2f}s", f"{rate:,.0f}"])

    audit.table(["任务数", "耗时", "TPS"], rows)


async def bench_create_delayed(scheduler: RedisScheduler, r: aioredis.Redis):
    """测试 2: 创建延迟任务"""
    audit.subheader("1b. 延迟任务创建")

    await cleanup(r)
    count = 10_000
    t0 = time.time()
    for i in range(count):
        await scheduler.create_delayed_task(
            task_type="bench_notify",
            delay_seconds=60 + i % 3600,
            user_id=i % 5000,
            payload={"msg": f"hello_{i}"},
            task_id=f"bench_delayed_{i}",
            max_retries=2,
            retry_delay=10,
        )
    dt = time.time() - t0
    audit.log(f"创建 {count:,} 个延迟任务: {dt:.2f}s ({count/dt:,.0f} tasks/s)")


async def bench_read_ops(scheduler: RedisScheduler, r: aioredis.Redis):
    """测试 3: 读取操作性能"""
    audit.header("2. 读取操作")

    # 先创建 10K 任务
    await cleanup(r)
    n = 10_000
    audit.log(f"准备 {n:,} 个任务...")
    for i in range(n):
        await scheduler.create_cron_task(
            task_type="bench_report" if i % 2 == 0 else "bench_cleanup",
            cron="0 */2 * * *",
            user_id=i % 1000,
            payload={"idx": i},
            task_id=f"bench_read_{i}",
        )

    # get_task
    sample_ids = [f"bench_read_{i}" for i in range(0, n, n // 100)]
    t0 = time.time()
    for tid in sample_ids:
        task = await scheduler.get_task(tid)
        assert task is not None
    dt = time.time() - t0
    audit.log(f"get_task x {len(sample_ids)}: {dt:.3f}s ({len(sample_ids)/dt:,.0f} ops/s)")

    # list_tasks (first 100)
    t0 = time.time()
    tasks = await scheduler.list_tasks(limit=100)
    dt = time.time() - t0
    audit.log(f"list_tasks(limit=100): {dt:.3f}s, 返回 {len(tasks)} 条")

    # list_tasks with type filter
    t0 = time.time()
    tasks = await scheduler.list_tasks(task_type="bench_report", limit=50)
    dt = time.time() - t0
    audit.log(f"list_tasks(type=bench_report, limit=50): {dt:.3f}s, 返回 {len(tasks)} 条")

    # count_tasks
    t0 = time.time()
    total = await scheduler.count_tasks()
    dt = time.time() - t0
    audit.log(f"count_tasks(): {dt:.3f}s, 总计 {total:,} 条")

    # list_tasks_by_user
    t0 = time.time()
    user_tasks = await scheduler.list_tasks_by_user(user_id=42)
    dt = time.time() - t0
    audit.log(f"list_tasks_by_user(42): {dt:.3f}s, 返回 {len(user_tasks)} 条")

    # list_tasks_by_user with type filter
    t0 = time.time()
    user_tasks = await scheduler.list_tasks_by_user(user_id=42, task_type="bench_report")
    dt = time.time() - t0
    audit.log(f"list_tasks_by_user(42, type=bench_report): {dt:.3f}s, 返回 {len(user_tasks)} 条")


async def bench_update_ops(scheduler: RedisScheduler, r: aioredis.Redis):
    """测试 4: 更新操作性能"""
    audit.header("3. 更新操作")

    # 准备
    await cleanup(r)
    n = 5_000
    for i in range(n):
        await scheduler.create_cron_task(
            task_type="bench_update",
            cron="*/10 * * * *",
            user_id=i % 500,
            payload={"v": 1},
            task_id=f"bench_upd_{i}",
        )

    # update_task (payload)
    sample = 500
    t0 = time.time()
    for i in range(sample):
        await scheduler.update_task(f"bench_upd_{i}", payload={"v": 2, "updated": True})
    dt = time.time() - t0
    audit.log(f"update_task(payload) x {sample}: {dt:.3f}s ({sample/dt:,.0f} ops/s)")

    # update_task (cron)
    t0 = time.time()
    for i in range(sample):
        await scheduler.update_task(f"bench_upd_{i}", cron="*/15 * * * *")
    dt = time.time() - t0
    audit.log(f"update_task(cron) x {sample}: {dt:.3f}s ({sample/dt:,.0f} ops/s)")

    # verify update
    task = await scheduler.get_task("bench_upd_0")
    assert isinstance(task, CronTask)
    assert task.cron == "*/15 * * * *"
    assert task.payload.get("updated") is True
    audit.log(f"验证 bench_upd_0: cron={task.cron}, payload={task.payload}")


async def bench_pause_resume(scheduler: RedisScheduler, r: aioredis.Redis):
    """测试 5: 暂停/恢复性能"""
    audit.header("4. 暂停/恢复")

    await cleanup(r)
    n = 5_000
    for i in range(n):
        await scheduler.create_cron_task(
            task_type="bench_pr",
            cron="0 * * * *",
            user_id=i,
            payload={},
            task_id=f"bench_pr_{i}",
        )

    sample = 1_000

    # pause
    t0 = time.time()
    for i in range(sample):
        await scheduler.pause_task(f"bench_pr_{i}")
    dt = time.time() - t0
    audit.log(f"pause_task x {sample}: {dt:.3f}s ({sample/dt:,.0f} ops/s)")

    # verify paused status
    task = await scheduler.get_task("bench_pr_0")
    assert task.status == "paused"
    audit.log(f"验证 bench_pr_0 status={task.status} ✓")

    # verify not in trigger ZSET
    count_after_pause = await scheduler.count_tasks()
    audit.log(f"暂停 {sample} 后 ZSET 任务数: {count_after_pause:,} (期望 {n - sample:,})")

    # resume
    t0 = time.time()
    for i in range(sample):
        await scheduler.resume_task(f"bench_pr_{i}")
    dt = time.time() - t0
    audit.log(f"resume_task x {sample}: {dt:.3f}s ({sample/dt:,.0f} ops/s)")

    # verify resumed
    task = await scheduler.get_task("bench_pr_0")
    assert task.status == "active"
    count_after_resume = await scheduler.count_tasks()
    audit.log(f"恢复后 ZSET 任务数: {count_after_resume:,} (期望 {n:,})")


async def bench_trigger(scheduler: RedisScheduler, r: aioredis.Redis):
    """测试 6: 手动触发"""
    audit.header("5. 手动触发")

    await cleanup(r)
    n = 1_000
    for i in range(n):
        await scheduler.create_cron_task(
            task_type="bench_trigger",
            cron="0 0 * * *",  # 每天零点
            user_id=i,
            payload={},
            task_id=f"bench_trig_{i}",
        )

    # 批量触发
    t0 = time.time()
    for i in range(n):
        result = await scheduler.trigger_task(f"bench_trig_{i}")
        assert result is True
    dt = time.time() - t0
    audit.log(f"trigger_task x {n}: {dt:.3f}s ({n/dt:,.0f} ops/s)")

    # 验证 score=0
    shard_id = calc_shard_id(0, scheduler._shard_count)
    score = await r.zscore(f"trigger:shard_{shard_id}", "bench_trig_0")
    audit.log(f"验证 bench_trig_0 score={score} (期望 0.0) {'✓' if score == 0.0 else '✗'}")

    # trigger paused task should fail
    await scheduler.pause_task("bench_trig_0")
    result = await scheduler.trigger_task("bench_trig_0")
    audit.log(f"触发已暂停任务: result={result} (期望 False) {'✓' if not result else '✗'}")

    # trigger nonexistent
    result = await scheduler.trigger_task("nonexistent_task_xyz")
    audit.log(f"触发不存在任务: result={result} (期望 False) {'✓' if not result else '✗'}")


async def bench_delete(scheduler: RedisScheduler, r: aioredis.Redis):
    """测试 7: 删除操作"""
    audit.header("6. 删除操作")

    await cleanup(r)
    n = 5_000
    for i in range(n):
        await scheduler.create_cron_task(
            task_type="bench_del",
            cron="*/5 * * * *",
            user_id=i % 500,
            payload={},
            task_id=f"bench_del_{i}",
        )

    # 单个删除
    sample = 500
    t0 = time.time()
    for i in range(sample):
        await scheduler.delete_task(f"bench_del_{i}")
    dt = time.time() - t0
    audit.log(f"delete_task x {sample}: {dt:.3f}s ({sample/dt:,.0f} ops/s)")

    # 验证已删除
    task = await scheduler.get_task("bench_del_0")
    audit.log(f"验证 bench_del_0 已删除: {task is None} ✓")

    # 批量删除
    batch = [f"bench_del_{i}" for i in range(sample, sample + 1000)]
    t0 = time.time()
    deleted = await scheduler.bulk_delete_tasks(batch)
    dt = time.time() - t0
    audit.log(f"bulk_delete_tasks x {len(batch)}: {dt:.3f}s, 删除 {deleted} 条 ({len(batch)/dt:,.0f} ops/s)")

    remaining = await scheduler.count_tasks()
    audit.log(f"剩余任务数: {remaining:,} (期望 {n - sample - 1000:,})")


async def bench_bulk_ops(scheduler: RedisScheduler, r: aioredis.Redis):
    """测试 8: 批量暂停/恢复"""
    audit.header("7. 批量暂停/恢复")

    await cleanup(r)
    n = 3_000
    for i in range(n):
        await scheduler.create_cron_task(
            task_type="bench_bulk",
            cron="*/30 * * * *",
            user_id=i,
            payload={},
            task_id=f"bench_bulk_{i}",
        )

    ids = [f"bench_bulk_{i}" for i in range(1000)]

    # bulk pause
    t0 = time.time()
    paused = await scheduler.bulk_pause_tasks(ids)
    dt = time.time() - t0
    audit.log(f"bulk_pause_tasks x {len(ids)}: {dt:.3f}s, 暂停 {paused} 条 ({len(ids)/dt:,.0f} ops/s)")

    active_count = await scheduler.count_tasks()
    audit.log(f"ZSET 活跃任务: {active_count:,} (期望 {n - 1000:,})")

    # bulk resume
    t0 = time.time()
    resumed = await scheduler.bulk_resume_tasks(ids)
    dt = time.time() - t0
    audit.log(f"bulk_resume_tasks x {len(ids)}: {dt:.3f}s, 恢复 {resumed} 条 ({len(ids)/dt:,.0f} ops/s)")

    active_count = await scheduler.count_tasks()
    audit.log(f"ZSET 活跃任务: {active_count:,} (期望 {n:,})")


async def bench_execution_history(scheduler: RedisScheduler, r: aioredis.Redis):
    """测试 9: 执行历史"""
    audit.header("8. 执行历史")

    await cleanup(r)

    # 模拟写入执行历史
    task_id = "bench_hist_0"
    await scheduler.create_cron_task(
        task_type="bench_hist",
        cron="* * * * *",
        user_id=1,
        task_id=task_id,
    )

    # 直接写入模拟历史
    pipe = r.pipeline()
    for i in range(50):
        entry = json.dumps({
            "fire_time": time.time() - (50 - i) * 60,
            "status": "success" if i % 5 != 0 else "failed",
            "duration_ms": 100 + i * 10,
            "error": None if i % 5 != 0 else "test error",
        })
        pipe.lpush(f"task_history:{task_id}", entry)
    pipe.ltrim(f"task_history:{task_id}", 0, 99)
    await pipe.execute()

    # 读取历史
    t0 = time.time()
    history = await scheduler.get_task_history(task_id, limit=10)
    dt = time.time() - t0
    audit.log(f"get_task_history(limit=10): {dt:.4f}s, 返回 {len(history)} 条")

    t0 = time.time()
    history_all = await scheduler.get_task_history(task_id, limit=50)
    dt = time.time() - t0
    audit.log(f"get_task_history(limit=50): {dt:.4f}s, 返回 {len(history_all)} 条")

    # 验证结构
    if history:
        sample = history[0]
        audit.log(f"历史样本: fire_time={sample.get('fire_time')}, status={sample.get('status')}, duration_ms={sample.get('duration_ms')}")

    # 批量读取历史 (模拟 100 个任务的历史查询)
    for i in range(1, 100):
        tid = f"bench_hist_{i}"
        await scheduler.create_cron_task(task_type="bench_hist", cron="* * * * *", user_id=i, task_id=tid)
        entry = json.dumps({"fire_time": time.time(), "status": "success", "duration_ms": 50, "error": None})
        await r.lpush(f"task_history:{tid}", entry)

    t0 = time.time()
    for i in range(100):
        await scheduler.get_task_history(f"bench_hist_{i}", limit=10)
    dt = time.time() - t0
    audit.log(f"get_task_history x 100: {dt:.3f}s ({100/dt:,.0f} ops/s)")


async def bench_status_model(scheduler: RedisScheduler, r: aioredis.Redis):
    """测试 10: 状态字段验证"""
    audit.header("9. 状态模型验证")

    await cleanup(r)

    # 创建任务，验证初始状态
    tid = "bench_status_0"
    await scheduler.create_cron_task(
        task_type="bench_status",
        cron="*/5 * * * *",
        user_id=1,
        payload={"test": True},
        task_id=tid,
        max_retries=3,
        retry_delay=30,
    )

    task = await scheduler.get_task(tid)
    assert task is not None
    audit.log(f"创建后 status={task.status} (期望 active) {'✓' if task.status == 'active' else '✗'}")
    audit.log(f"max_retries={task.max_retries} (期望 3) {'✓' if task.max_retries == 3 else '✗'}")
    audit.log(f"retry_delay={task.retry_delay} (期望 30) {'✓' if task.retry_delay == 30 else '✗'}")
    audit.log(f"retry_count={task.retry_count} (期望 0) {'✓' if task.retry_count == 0 else '✗'}")
    audit.log(f"run_count={task.run_count} (期望 0) {'✓' if task.run_count == 0 else '✗'}")
    audit.log(f"fail_count={task.fail_count} (期望 0) {'✓' if task.fail_count == 0 else '✗'}")

    # 暂停
    await scheduler.pause_task(tid)
    task = await scheduler.get_task(tid)
    audit.log(f"暂停后 status={task.status} (期望 paused) {'✓' if task.status == 'paused' else '✗'}")

    # 恢复
    await scheduler.resume_task(tid)
    task = await scheduler.get_task(tid)
    audit.log(f"恢复后 status={task.status} (期望 active) {'✓' if task.status == 'active' else '✗'}")

    # 模拟 running 状态
    await r.hset(f"task:{tid}", "status", "running")
    task = await scheduler.get_task(tid)
    audit.log(f"手动设 running 后 status={task.status} (期望 running) {'✓' if task.status == 'running' else '✗'}")

    # 延迟任务
    dtid = "bench_status_delayed"
    await scheduler.create_delayed_task(
        task_type="bench_delayed_status",
        delay_seconds=60,
        user_id=2,
        task_id=dtid,
        max_retries=1,
    )
    task = await scheduler.get_task(dtid)
    audit.log(f"延迟任务 status={task.status} (期望 active) {'✓' if task.status == 'active' else '✗'}")
    audit.log(f"延迟任务 is_cron=False {'✓' if not isinstance(task, CronTask) else '✗'}")


async def bench_user_index(scheduler: RedisScheduler, r: aioredis.Redis):
    """测试 11: 用户索引"""
    audit.header("10. 用户索引 (user_tasks)")

    await cleanup(r)

    # 创建多用户任务
    users = 100
    tasks_per_user = 50
    total = users * tasks_per_user

    t0 = time.time()
    for u in range(users):
        for t in range(tasks_per_user):
            await scheduler.create_cron_task(
                task_type="bench_idx" if t % 2 == 0 else "bench_idx_alt",
                cron="*/5 * * * *",
                user_id=u,
                payload={},
                task_id=f"bench_idx_{u}_{t}",
            )
    dt = time.time() - t0
    audit.log(f"创建 {total:,} 任务 ({users} 用户 x {tasks_per_user} 任务): {dt:.2f}s")

    # 按用户查询
    t0 = time.time()
    user_tasks = await scheduler.list_tasks_by_user(user_id=42)
    dt = time.time() - t0
    audit.log(f"list_tasks_by_user(42): {dt:.3f}s, 返回 {len(user_tasks)} 条 (期望 {tasks_per_user})")

    # 按用户+类型
    t0 = time.time()
    user_tasks = await scheduler.list_tasks_by_user(user_id=42, task_type="bench_idx")
    dt = time.time() - t0
    audit.log(f"list_tasks_by_user(42, type=bench_idx): {dt:.3f}s, 返回 {len(user_tasks)} 条 (期望 {tasks_per_user // 2})")

    # 暂停某些任务后仍能通过 user index 查到
    for t in range(5):
        await scheduler.pause_task(f"bench_idx_42_{t}")
    user_tasks = await scheduler.list_tasks_by_user(user_id=42)
    paused_count = sum(1 for t in user_tasks if t.status == "paused")
    audit.log(f"暂停 5 个后 list_tasks_by_user(42): {len(user_tasks)} 条, paused={paused_count} (期望 5)")

    # 删除任务后 user index 应清除
    await scheduler.delete_task("bench_idx_42_0")
    user_tasks = await scheduler.list_tasks_by_user(user_id=42)
    audit.log(f"删除 1 个后 list_tasks_by_user(42): {len(user_tasks)} 条 (期望 {tasks_per_user - 1})")

    # 批量用户查询性能
    t0 = time.time()
    for u in range(users):
        await scheduler.list_tasks_by_user(user_id=u, limit=10)
    dt = time.time() - t0
    audit.log(f"list_tasks_by_user x {users} 用户: {dt:.3f}s ({users/dt:,.0f} queries/s)")


async def bench_memory(scheduler: RedisScheduler, r: aioredis.Redis):
    """测试 12: 内存占用"""
    audit.header("11. 内存占用")

    for count in [10_000, 100_000, 500_000]:
        await cleanup(r)

        info_before = await r.info("memory")
        mem_before = info_before["used_memory"]

        for i in range(count):
            await scheduler.create_cron_task(
                task_type="bench_mem",
                cron="*/5 * * * *",
                user_id=i % 10000,
                payload={"email": f"user{i}@example.com", "action": "daily_report"},
                task_id=f"bench_mem_{i}",
                max_retries=3,
                retry_delay=60,
            )

        info_after = await r.info("memory")
        mem_after = info_after["used_memory"]
        delta = mem_after - mem_before
        per_task = delta / count

        audit.log(f"{count:>7,} 任务: {delta / 1024 / 1024:.1f} MB (每任务 {per_task:.0f} bytes)")


async def bench_concurrent_nodes(scheduler: RedisScheduler, r: aioredis.Redis):
    """测试 13: 模拟多节点并发写入"""
    audit.header("12. 多节点并发写入")

    await cleanup(r)

    async def node_writer(node_id: int, count: int):
        s = RedisScheduler(redis_url=REDIS_URL, node_id=f"bench_node_{node_id}", shard_count=16)
        for i in range(count):
            await s.create_cron_task(
                task_type="bench_concurrent",
                cron="*/5 * * * *",
                user_id=node_id * 100000 + i,
                payload={"node": node_id},
                task_id=f"bench_conc_{node_id}_{i}",
            )
        if s._redis:
            await s._redis.aclose()

    for nodes in [2, 4, 8]:
        await cleanup(r)
        per_node = 5000
        total = nodes * per_node

        t0 = time.time()
        await asyncio.gather(*[node_writer(n, per_node) for n in range(nodes)])
        dt = time.time() - t0

        actual = await scheduler.count_tasks()
        audit.log(f"{nodes} 节点 x {per_node:,} = {total:,} 任务: {dt:.2f}s ({total/dt:,.0f} tasks/s), 实际 {actual:,}")


# ============================================================
# Main
# ============================================================

async def main():
    print(f"🚀 redis-cron v0.2.0 压测开始")
    print(f"   Redis: {REDIS_URL}")
    print(f"   审计文件: {AUDIT_FILE}")
    print()

    r = aioredis.from_url(REDIS_URL, decode_responses=False)

    # 连接测试
    try:
        await r.ping()
        info = await r.info("server")
        redis_ver = info.get("redis_version", "unknown")
        audit.log(f"Redis 连接成功, 版本: {redis_ver}")
        print(f"   Redis 版本: {redis_ver}")
    except Exception as e:
        print(f"❌ Redis 连接失败: {e}")
        return

    scheduler = RedisScheduler(redis_url=REDIS_URL, node_id="bench_main", shard_count=16)

    try:
        print("\n[1/12] 批量创建 cron 任务...")
        await bench_create_tasks(scheduler, r)

        print("[2/12] 创建延迟任务...")
        await bench_create_delayed(scheduler, r)

        print("[3/12] 读取操作...")
        await bench_read_ops(scheduler, r)

        print("[4/12] 更新操作...")
        await bench_update_ops(scheduler, r)

        print("[5/12] 暂停/恢复...")
        await bench_pause_resume(scheduler, r)

        print("[6/12] 手动触发...")
        await bench_trigger(scheduler, r)

        print("[7/12] 删除操作...")
        await bench_delete(scheduler, r)

        print("[8/12] 批量暂停/恢复...")
        await bench_bulk_ops(scheduler, r)

        print("[9/12] 执行历史...")
        await bench_execution_history(scheduler, r)

        print("[10/12] 状态模型验证...")
        await bench_status_model(scheduler, r)

        print("[11/12] 用户索引...")
        await bench_user_index(scheduler, r)

        print("[12/12] 内存占用...")
        await bench_memory(scheduler, r)

        # 多节点并发单独跑 (可选，因为比较慢)
        print("[bonus] 多节点并发...")
        await bench_concurrent_nodes(scheduler, r)

    finally:
        await cleanup(r)
        if scheduler._redis:
            await scheduler._redis.aclose()
        await r.aclose()

    # 写审计文件
    audit.save()
    print(f"\n✅ 压测完成! 审计报告: {AUDIT_FILE}")


if __name__ == "__main__":
    asyncio.run(main())
