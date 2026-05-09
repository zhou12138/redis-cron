# redis-cron v0.2.0 压测审计报告

- **生成时间**: 2026-05-09 06:53:12
- **Redis**: `redis://localhost:6379`
- **总耗时**: 550.8s

- [   0.006s] Redis 连接成功, 版本: 7.4.7

## 1. 批量创建任务

- [   0.758s] 创建 1,000 个 cron 任务: 0.75s (1,334 tasks/s)
- [   4.439s] 创建 5,000 个 cron 任务: 3.67s (1,363 tasks/s)
- [  11.907s] 创建 10,000 个 cron 任务: 7.42s (1,348 tasks/s)

| 任务数 | 耗时 | TPS |
| --- | --- | --- |
| 1,000 | 0.75s | 1,334 |
| 5,000 | 3.67s | 1,363 |
| 10,000 | 7.42s | 1,348 |


### 1b. 延迟任务创建

- [  16.931s] 创建 10,000 个延迟任务: 4.92s (2,031 tasks/s)

## 2. 读取操作

- [  17.000s] 准备 10,000 个任务...
- [  24.702s] get_task x 100: 0.034s (2,902 ops/s)
- [  24.737s] list_tasks(limit=100): 0.035s, 返回 100 条
- [  24.755s] list_tasks(type=bench_report, limit=50): 0.018s, 返回 50 条
- [  24.759s] count_tasks(): 0.004s, 总计 10,000 条
- [  24.763s] list_tasks_by_user(42): 0.004s, 返回 10 条
- [  24.767s] list_tasks_by_user(42, type=bench_report): 0.004s, 返回 10 条

## 3. 更新操作

- [  28.754s] update_task(payload) x 500: 0.334s (1,498 ops/s)
- [  29.220s] update_task(cron) x 500: 0.466s (1,072 ops/s)
- [  29.221s] 验证 bench_upd_0: cron=*/15 * * * *, payload={'v': 2, 'updated': True}

## 4. 暂停/恢复

- [  33.277s] pause_task x 1000: 0.665s (1,503 ops/s)
- [  33.277s] 验证 bench_pr_0 status=paused ✓
- [  33.282s] 暂停 1000 后 ZSET 任务数: 4,000 (期望 4,000)
- [  34.191s] resume_task x 1000: 0.909s (1,100 ops/s)
- [  34.195s] 恢复后 ZSET 任务数: 5,000 (期望 5,000)

## 5. 手动触发

- [  35.534s] trigger_task x 1000: 0.613s (1,633 ops/s)
- [  35.534s] 验证 bench_trig_0 score=0.0 (期望 0.0) ✓
- [  35.536s] 触发已暂停任务: result=False (期望 False) ✓
- [  35.536s] 触发不存在任务: result=False (期望 False) ✓

## 6. 删除操作

- [  39.573s] delete_task x 500: 0.366s (1,367 ops/s)
- [  39.573s] 验证 bench_del_0 已删除: True ✓
- [  40.286s] bulk_delete_tasks x 1000: 0.713s, 删除 1000 条 (1,404 ops/s)
- [  40.290s] 剩余任务数: 3,500 (期望 3,500)

## 7. 批量暂停/恢复

- [  43.079s] bulk_pause_tasks x 1000: 0.667s, 暂停 1000 条 (1,499 ops/s)
- [  43.083s] ZSET 活跃任务: 2,000 (期望 2,000)
- [  44.015s] bulk_resume_tasks x 1000: 0.932s, 恢复 1000 条 (1,072 ops/s)
- [  44.020s] ZSET 活跃任务: 3,000 (期望 3,000)

## 8. 执行历史

- [  44.052s] get_task_history(limit=10): 0.0004s, 返回 10 条
- [  44.053s] get_task_history(limit=50): 0.0005s, 返回 50 条
- [  44.053s] 历史样本: fire_time=1778309025.6107728, status=success, duration_ms=590
- [  44.180s] get_task_history x 100: 0.030s (3,373 ops/s)

## 9. 状态模型验证

- [  44.186s] 创建后 status=active (期望 active) ✓
- [  44.186s] max_retries=3 (期望 3) ✓
- [  44.186s] retry_delay=30 (期望 30) ✓
- [  44.186s] retry_count=0 (期望 0) ✓
- [  44.186s] run_count=0 (期望 0) ✓
- [  44.186s] fail_count=0 (期望 0) ✓
- [  44.187s] 暂停后 status=paused (期望 paused) ✓
- [  44.188s] 恢复后 status=active (期望 active) ✓
- [  44.189s] 手动设 running 后 status=running (期望 running) ✓
- [  44.190s] 延迟任务 status=active (期望 active) ✓
- [  44.190s] 延迟任务 is_cron=False ✓

## 10. 用户索引 (user_tasks)

- [  47.819s] 创建 5,000 任务 (100 用户 x 50 任务): 3.63s
- [  47.836s] list_tasks_by_user(42): 0.017s, 返回 50 条 (期望 50)
- [  47.854s] list_tasks_by_user(42, type=bench_idx): 0.017s, 返回 25 条 (期望 25)
- [  47.874s] 暂停 5 个后 list_tasks_by_user(42): 50 条, paused=5 (期望 5)
- [  47.892s] 删除 1 个后 list_tasks_by_user(42): 49 条 (期望 49)
- [  48.264s] list_tasks_by_user x 100 用户: 0.372s (269 queries/s)

## 11. 内存占用

- [  55.614s]  10,000 任务: 6.4 MB (每任务 667 bytes)
- [ 131.047s] 100,000 任务: 56.0 MB (每任务 587 bytes)
- [ 510.072s] 500,000 任务: 275.4 MB (每任务 578 bytes)

## 12. 多节点并发写入

- [ 518.360s] 2 节点 x 5,000 = 10,000 任务: 6.04s (1,657 tasks/s), 实际 10,000
- [ 529.387s] 4 节点 x 5,000 = 20,000 任务: 10.92s (1,831 tasks/s), 实际 20,000
- [ 550.381s] 8 节点 x 5,000 = 40,000 任务: 20.79s (1,924 tasks/s), 实际 40,000
