"""工具函数：jitter 计算、cron 解析。"""

from __future__ import annotations

import datetime
import time
from zoneinfo import ZoneInfo

from croniter import croniter


def calc_next_fire(cron_expr: str, base_time: float | None = None, tz: str = "UTC") -> float:
    """计算下一次触发时间戳。

    Args:
        cron_expr: Cron 表达式，如 "0 8 * * *"
        base_time: 基准时间戳，默认当前时间
        tz: 时区名称，如 "Asia/Shanghai"，默认 "UTC"

    Returns:
        下一次触发的 Unix 时间戳
    """
    base = base_time or time.time()
    tzinfo = ZoneInfo(tz)
    # 将 UTC 时间戳转为目标时区的 datetime
    base_dt = datetime.datetime.fromtimestamp(base, tz=tzinfo)
    cron = croniter(cron_expr, base_dt)
    next_dt = cron.get_next(datetime.datetime)
    # 确保结果带时区信息，转回 UTC 时间戳
    if next_dt.tzinfo is None:
        next_dt = next_dt.replace(tzinfo=tzinfo)
    return next_dt.timestamp()


def calc_stable_jitter(user_id: int, max_jitter: int) -> int:
    """基于 user_id 的稳定 jitter，同一用户每次触发的偏移一致。

    这样避免用户感知到随机性，同时实现整点风暴打散。

    Args:
        user_id: 用户 ID
        max_jitter: 最大打散秒数

    Returns:
        固定的 jitter 秒数
    """
    if max_jitter <= 0:
        return 0
    return user_id % max_jitter


def calc_shard_id(user_id: int, shard_count: int) -> int:
    """根据 user_id 计算 shard 编号。

    Args:
        user_id: 用户 ID
        shard_count: 分片总数

    Returns:
        分片 ID
    """
    return user_id % shard_count
