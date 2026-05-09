"""时区支持测试。"""

import datetime
import time
from zoneinfo import ZoneInfo

import pytest

from redis_cron.models import CronTask, Task
from redis_cron.utils import calc_next_fire


class TestCalcNextFireTimezone:
    """calc_next_fire 时区参数测试。"""

    def test_default_utc_unchanged(self):
        """默认 UTC 行为与之前一致。"""
        base = 1700000000.0  # 2023-11-14 22:13:20 UTC
        nf_default = calc_next_fire("* * * * *", base)
        nf_utc = calc_next_fire("* * * * *", base, tz="UTC")
        assert nf_default == nf_utc

    def test_shanghai_timezone(self):
        """Asia/Shanghai 时区的 cron 任务在正确的 UTC 时间触发。

        "0 9 * * *" 在上海时区 = UTC 01:00
        """
        # 2024-01-15 00:00:00 UTC = 2024-01-15 08:00:00 CST
        base = datetime.datetime(2024, 1, 15, 0, 0, 0, tzinfo=ZoneInfo("UTC")).timestamp()

        nf = calc_next_fire("0 9 * * *", base, tz="Asia/Shanghai")
        result_dt = datetime.datetime.fromtimestamp(nf, tz=ZoneInfo("UTC"))

        # 上海 9:00 = UTC 01:00
        assert result_dt.hour == 1
        assert result_dt.minute == 0
        assert result_dt.day == 15  # same day since base is 08:00 CST, next 9:00 CST is same day

    def test_shanghai_vs_utc_differ(self):
        """同一 cron 表达式在不同时区产生不同的 UTC 触发时间。"""
        base = datetime.datetime(2024, 1, 15, 0, 0, 0, tzinfo=ZoneInfo("UTC")).timestamp()

        nf_utc = calc_next_fire("0 9 * * *", base, tz="UTC")
        nf_shanghai = calc_next_fire("0 9 * * *", base, tz="Asia/Shanghai")

        # UTC 9:00 vs Shanghai 9:00 (= UTC 1:00), they must differ
        assert nf_utc != nf_shanghai
        # Shanghai fires earlier (UTC 01:00 < UTC 09:00)
        assert nf_shanghai < nf_utc

    def test_dst_spring_forward(self):
        """America/New_York 春天夏令时跳过: 2:00 AM 不存在，跳到 3:00 AM。

        2024-03-10 是 DST 切换日，02:00 → 03:00。
        "30 2 * * *" 在 EST 的 2:30 AM 不存在，croniter 应跳过。
        """
        # 2024-03-10 01:00:00 EST = 06:00:00 UTC
        tz = ZoneInfo("America/New_York")
        base = datetime.datetime(2024, 3, 10, 1, 0, 0, tzinfo=tz).timestamp()

        nf = calc_next_fire("30 2 * * *", base, tz="America/New_York")
        result_dt = datetime.datetime.fromtimestamp(nf, tz=tz)

        # 2:30 AM doesn't exist on 2024-03-10, should go to next day or 3:30
        # The key point: it should NOT fire at an impossible time
        assert result_dt >= datetime.datetime(2024, 3, 10, 3, 0, 0, tzinfo=tz)

    def test_dst_fall_back(self):
        """America/New_York 秋天夏令时回退: 1:00 AM 重复出现。

        2024-11-03 是 DST 结束日，02:00 → 01:00。
        "30 1 * * *" 应该正常触发。
        """
        tz = ZoneInfo("America/New_York")
        # Before DST change: 2024-11-03 00:00:00 EDT
        base = datetime.datetime(2024, 11, 3, 0, 0, 0, tzinfo=tz).timestamp()

        nf = calc_next_fire("30 1 * * *", base, tz="America/New_York")
        result_dt = datetime.datetime.fromtimestamp(nf, tz=tz)

        # Should fire at 1:30 AM on Nov 3
        assert result_dt.day == 3
        assert result_dt.hour == 1
        assert result_dt.minute == 30

    def test_tokyo_no_dst(self):
        """Asia/Tokyo 没有 DST，行为稳定。"""
        base = datetime.datetime(2024, 6, 15, 0, 0, 0, tzinfo=ZoneInfo("UTC")).timestamp()

        nf = calc_next_fire("0 9 * * *", base, tz="Asia/Tokyo")
        result_dt = datetime.datetime.fromtimestamp(nf, tz=ZoneInfo("UTC"))

        # Tokyo 9:00 = UTC 00:00, but base is already 00:00 UTC = 09:00 JST
        # so next fire should be next day UTC 00:00
        assert result_dt.hour == 0
        assert result_dt.minute == 0


class TestTimezoneFieldSerialization:
    """timezone 字段的序列化/反序列化测试。"""

    def test_task_default_timezone(self):
        """Task 默认 timezone 是 UTC。"""
        t = Task(task_type="test")
        assert t.timezone == "UTC"

    def test_task_custom_timezone(self):
        """Task 自定义 timezone。"""
        t = Task(task_type="test", timezone="Asia/Shanghai")
        assert t.timezone == "Asia/Shanghai"

    def test_task_to_redis_includes_timezone(self):
        """to_redis 包含 timezone 字段。"""
        t = Task(task_type="test", timezone="America/New_York")
        d = t.to_redis()
        assert d["timezone"] == "America/New_York"

    def test_task_from_redis_roundtrip(self):
        """Task timezone 字段序列化-反序列化往返。"""
        t = Task(task_id="t1", task_type="test", timezone="Europe/London")
        d = t.to_redis()
        restored = Task.from_redis("t1", d)
        assert restored.timezone == "Europe/London"

    def test_task_from_redis_missing_timezone_defaults_utc(self):
        """旧数据没有 timezone 字段时默认 UTC。"""
        t = Task(task_id="t1", task_type="test")
        d = t.to_redis()
        del d["timezone"]
        restored = Task.from_redis("t1", d)
        assert restored.timezone == "UTC"

    def test_cron_task_timezone_roundtrip(self):
        """CronTask timezone 字段序列化-反序列化往返。"""
        t = CronTask(task_id="c1", task_type="test", cron="0 8 * * *", timezone="Asia/Tokyo")
        d = t.to_redis()
        assert d["timezone"] == "Asia/Tokyo"
        restored = CronTask.from_redis("c1", d)
        assert restored.timezone == "Asia/Tokyo"

    def test_cron_task_from_redis_missing_timezone(self):
        """CronTask 旧数据没有 timezone 字段时默认 UTC。"""
        t = CronTask(task_id="c1", task_type="test", cron="0 8 * * *")
        d = t.to_redis()
        del d["timezone"]
        restored = CronTask.from_redis("c1", d)
        assert restored.timezone == "UTC"


class TestSchedulerTimezone:
    """Scheduler 层时区集成测试。"""

    @pytest.fixture
    def scheduler(self):
        import fakeredis.aioredis
        from redis_cron import RedisScheduler
        s = RedisScheduler(redis_url="redis://localhost:6379", shard_count=4)
        s._redis = fakeredis.aioredis.FakeRedis()
        return s

    @pytest.mark.asyncio
    async def test_create_cron_task_with_timezone(self, scheduler):
        """创建带时区的 cron 任务，timezone 存入 Redis。"""
        tid = await scheduler.create_cron_task(
            task_type="report",
            cron="0 9 * * *",
            timezone="Asia/Shanghai",
        )
        task = await scheduler.get_task(tid)
        assert task is not None
        assert task.timezone == "Asia/Shanghai"

    @pytest.mark.asyncio
    async def test_create_delayed_task_with_timezone(self, scheduler):
        """创建带时区的延迟任务。"""
        tid = await scheduler.create_delayed_task(
            task_type="notify",
            delay_seconds=60,
            timezone="Europe/Berlin",
        )
        task = await scheduler.get_task(tid)
        assert task is not None
        assert task.timezone == "Europe/Berlin"

    @pytest.mark.asyncio
    async def test_create_cron_task_default_utc(self, scheduler):
        """不传 timezone 默认 UTC。"""
        tid = await scheduler.create_cron_task(
            task_type="report",
            cron="0 9 * * *",
        )
        task = await scheduler.get_task(tid)
        assert task.timezone == "UTC"

    @pytest.mark.asyncio
    async def test_cron_fire_time_respects_timezone(self, scheduler):
        """带时区的 cron 任务的 fire_time 是正确的 UTC 时间戳。"""
        tid_utc = await scheduler.create_cron_task(
            task_type="report",
            cron="0 9 * * *",
            timezone="UTC",
        )
        tid_sh = await scheduler.create_cron_task(
            task_type="report",
            cron="0 9 * * *",
            timezone="Asia/Shanghai",
        )
        t_utc = await scheduler.get_task(tid_utc)
        t_sh = await scheduler.get_task(tid_sh)

        # Shanghai 9:00 is UTC 01:00, should fire earlier
        assert t_sh.fire_time != t_utc.fire_time
