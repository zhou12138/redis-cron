"""所有 Lua 脚本常量，确保 Redis 操作的原子性。"""

# 原子获取 shard 锁（带 Fencing Token）
# KEYS[1] = shard_lock:{N}
# KEYS[2] = shard_fence:{N}
# ARGV[1] = node_id
# ARGV[2] = ttl
ACQUIRE_SHARD_LOCK = """
local current = redis.call('GET', KEYS[1])
if current == false then
    local token = redis.call('INCR', KEYS[2])
    redis.call('SET', KEYS[1], ARGV[1] .. ':' .. token, 'EX', tonumber(ARGV[2]))
    return token
end
return -1
"""

# 续约 shard 锁
# KEYS[1] = shard_lock:{N}
# ARGV[1] = owner_val (node_id:token)
# ARGV[2] = ttl
RENEW_SHARD_LOCK = """
local val = redis.call('GET', KEYS[1])
if val == ARGV[1] then
    redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
    return 1
end
return 0
"""

# 原子取到期任务（带所有权校验 + 两阶段提交）
# KEYS[1] = shard_lock:{N}
# KEYS[2] = trigger:shard_{N}
# KEYS[3] = processing:shard_{N}
# ARGV[1] = owner_val (node_id:token)
# ARGV[2] = now (时间戳)
# ARGV[3] = batch_size
FETCH_DUE_TASKS = """
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

# 任务确认（ACK）：从 processing 移除，可选放回触发队列
# KEYS[1] = processing:shard_{N}
# KEYS[2] = trigger:shard_{N}
# ARGV[1] = task_id
# ARGV[2] = next_fire_time (0 表示不再触发)
ACK_TASK = """
redis.call('HDEL', KEYS[1], ARGV[1])

local next_fire = tonumber(ARGV[2])
if next_fire > 0 then
    redis.call('ZADD', KEYS[2], next_fire, ARGV[1])
end

return 1
"""
