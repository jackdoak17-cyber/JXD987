import datetime as dt

import pytest

from scripts.leaderboard_resource_meter import build_snapshot, evaluate


def metrics(*, cpu=100.0, idle=70.0, iowait=10.0, disk0=20.0, disk1=30.0,
            available=800_000_000.0, total=2_000_000_000.0) -> str:
    labels = 'supabase_project_ref="project",service_type="db"'
    return "\n".join([
        f'node_cpu_seconds_total{{{labels},cpu="0",mode="user"}} {cpu}',
        f'node_cpu_seconds_total{{{labels},cpu="0",mode="idle"}} {idle}',
        f'node_cpu_seconds_total{{{labels},cpu="0",mode="iowait"}} {iowait}',
        f'node_disk_io_time_seconds_total{{{labels},device="nvme0n1"}} {disk0}',
        f'node_disk_io_time_seconds_total{{{labels},device="nvme1n1"}} {disk1}',
        f'node_memory_MemAvailable_bytes{{{labels}}} {available}',
        f'node_memory_MemTotal_bytes{{{labels}}} {total}',
        f'node_memory_SwapFree_bytes{{{labels}}} 800000000',
        f'node_memory_SwapTotal_bytes{{{labels}}} 1000000000',
    ]) + "\n"


T0 = dt.datetime(2026, 11, 26, 20, 30, tzinfo=dt.timezone.utc)


def pair(before: str, after: str):
    return (
        build_snapshot(before, T0),
        build_snapshot(after, T0 + dt.timedelta(seconds=60)),
    )


def test_green_small_compute_interval_passes_admission():
    before, after = pair(
        metrics(),
        metrics(cpu=108, idle=116, iowait=11, disk0=32, disk1=48),
    )
    result = evaluate(before, after, mode="admission")
    assert result["ok"] is True
    assert result["breaches"] == []
    assert result["memory_available_pct"] == 40.0
    assert result["disk_busy_pct"] == {"nvme0n1": 20.0, "nvme1n1": 30.0}


@pytest.mark.parametrize(("after", "reason"), [
    (metrics(cpu=150, idle=72, iowait=11, disk0=21, disk1=31), "cpu_busy_gte_50pct"),
    (metrics(cpu=102, idle=71, iowait=30, disk0=21, disk1=31), "cpu_iowait_gte_20pct"),
    (metrics(cpu=102, idle=110, iowait=11, disk0=80, disk1=31), "disk_busy_gte_70pct"),
    (metrics(cpu=102, idle=110, iowait=11, disk0=21, disk1=31,
             available=300_000_000), "memory_available_lt_25pct_or_384mib"),
])
def test_each_admission_limit_fails_closed(after, reason):
    before, current = pair(metrics(), after)
    result = evaluate(before, current, mode="admission")
    assert result["ok"] is False
    assert reason in result["breaches"]


def test_counter_reset_and_stale_interval_fail_closed():
    before, after = pair(metrics(), metrics(cpu=90, idle=80, iowait=11))
    with pytest.raises(ValueError, match="counter reset"):
        evaluate(before, after, mode="admission")
    after["observed_at"] = (T0 + dt.timedelta(seconds=181)).isoformat()
    with pytest.raises(ValueError, match="outside"):
        evaluate(before, after, mode="admission")


def test_post_interval_covers_the_two_minute_worker_and_followup_observation():
    before, after = pair(
        metrics(),
        metrics(cpu=108, idle=116, iowait=11, disk0=32, disk1=48),
    )
    after["observed_at"] = (T0 + dt.timedelta(seconds=181)).isoformat()
    assert evaluate(before, after, mode="post")["ok"] is True
    after["observed_at"] = (T0 + dt.timedelta(seconds=241)).isoformat()
    with pytest.raises(ValueError, match="45-240"):
        evaluate(before, after, mode="post")


def test_required_metrics_cannot_be_missing():
    with pytest.raises(ValueError, match="required"):
        build_snapshot("node_memory_MemTotal_bytes 1\n", T0)
