"""
Integration tests for the Scheduler: temporal and reactive fault-injection triggers.
Pure asyncio — no network or Docker dependency.
"""
from __future__ import annotations

import asyncio
import time
from typing import Optional

import pytest

from palamedes.core.scheduler import Scheduler
from palamedes.models.events import ExperimentPhase
from palamedes.models.experiment import MetricSnapshot


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _FakeSoftwareCollector:
    """Minimal stand-in for SoftwareMetricsCollector exposing only ``latest``."""

    def __init__(self, snapshot: Optional[MetricSnapshot] = None) -> None:
        self._snapshot = snapshot

    @property
    def latest(self) -> Optional[MetricSnapshot]:
        return self._snapshot

    def push(self, **metric_values: float) -> None:
        snap = MetricSnapshot(
            ts_ms=int(time.time() * 1000),
            phase=ExperimentPhase.FAULT_INJECTION,
        )
        for attr, val in metric_values.items():
            setattr(snap, attr, val)
        self._snapshot = snap


# ---------------------------------------------------------------------------
# Temporal trigger tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_temporal_trigger_fires_after_offset():
    sw = _FakeSoftwareCollector()
    scheduler = Scheduler(sw)

    fired_at: list[float] = []

    async def callback() -> None:
        fired_at.append(asyncio.get_event_loop().time())

    start = asyncio.get_event_loop().time()
    task = scheduler.schedule_temporal(offset_seconds=0.2, callback=callback)
    await asyncio.wait_for(task, timeout=2.0)

    assert len(fired_at) == 1
    assert fired_at[0] - start >= 0.15  # fired after at least ~0.2 s (with tolerance)


@pytest.mark.asyncio
async def test_temporal_trigger_fires_exactly_once():
    sw = _FakeSoftwareCollector()
    scheduler = Scheduler(sw)

    call_count = 0

    async def callback() -> None:
        nonlocal call_count
        call_count += 1

    task = scheduler.schedule_temporal(offset_seconds=0.1, callback=callback)
    await asyncio.wait_for(task, timeout=2.0)
    await asyncio.sleep(0.3)  # extra wait to confirm no second firing

    assert call_count == 1


@pytest.mark.asyncio
async def test_temporal_trigger_cancelled_before_firing():
    sw = _FakeSoftwareCollector()
    scheduler = Scheduler(sw)

    fired = asyncio.Event()

    async def callback() -> None:
        fired.set()

    scheduler.schedule_temporal(offset_seconds=10.0, callback=callback)
    await asyncio.sleep(0.05)
    await scheduler.cancel_all()

    assert not fired.is_set()


# ---------------------------------------------------------------------------
# Reactive trigger tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reactive_trigger_fires_when_threshold_crossed_gt():
    sw = _FakeSoftwareCollector()
    scheduler = Scheduler(sw)

    fired = asyncio.Event()

    async def callback() -> None:
        fired.set()

    scheduler.schedule_reactive(
        metric="cpu_percent",
        threshold=70.0,
        comparator="gt",
        callback=callback,
        poll_interval_s=0.05,
    )

    # Below threshold — must not fire
    sw.push(cpu_percent=50.0)
    await asyncio.sleep(0.15)
    assert not fired.is_set()

    # Cross threshold — must fire
    sw.push(cpu_percent=85.0)
    await asyncio.wait_for(fired.wait(), timeout=1.0)

    await scheduler.cancel_all()


@pytest.mark.asyncio
async def test_reactive_trigger_fires_when_threshold_crossed_lt():
    sw = _FakeSoftwareCollector()
    scheduler = Scheduler(sw)

    fired = asyncio.Event()

    async def callback() -> None:
        fired.set()

    scheduler.schedule_reactive(
        metric="throughput_rps",
        threshold=200.0,
        comparator="lt",
        callback=callback,
        poll_interval_s=0.05,
    )

    sw.push(throughput_rps=500.0)
    await asyncio.sleep(0.15)
    assert not fired.is_set()

    sw.push(throughput_rps=50.0)
    await asyncio.wait_for(fired.wait(), timeout=1.0)

    await scheduler.cancel_all()


@pytest.mark.asyncio
@pytest.mark.parametrize("comparator,safe_value,trigger_value", [
    ("gt",  50.0,  80.0),
    ("gte", 60.0,  70.0),
    ("lt",  90.0,  30.0),
    ("lte", 80.0,  70.0),
])
async def test_reactive_trigger_comparators(comparator, safe_value, trigger_value):
    sw = _FakeSoftwareCollector()
    sw.push(throughput_rps=safe_value)
    scheduler = Scheduler(sw)

    fired = asyncio.Event()

    async def callback() -> None:
        fired.set()

    scheduler.schedule_reactive(
        metric="throughput_rps",
        threshold=70.0,
        comparator=comparator,
        callback=callback,
        poll_interval_s=0.05,
    )

    await asyncio.sleep(0.15)
    sw.push(throughput_rps=trigger_value)
    await asyncio.wait_for(fired.wait(), timeout=1.0)

    assert fired.is_set()
    await scheduler.cancel_all()


@pytest.mark.asyncio
async def test_reactive_trigger_no_snapshot_does_not_fire():
    """Reactive trigger must not fire when latest snapshot is None."""
    sw = _FakeSoftwareCollector(snapshot=None)
    scheduler = Scheduler(sw)

    fired = asyncio.Event()

    async def callback() -> None:
        fired.set()

    scheduler.schedule_reactive(
        metric="cpu_percent",
        threshold=0.0,
        comparator="gt",
        callback=callback,
        poll_interval_s=0.05,
    )

    await asyncio.sleep(0.2)
    assert not fired.is_set()

    await scheduler.cancel_all()


@pytest.mark.asyncio
async def test_cancel_all_clears_task_list():
    sw = _FakeSoftwareCollector()
    scheduler = Scheduler(sw)

    scheduler.schedule_temporal(offset_seconds=5.0, callback=lambda: asyncio.sleep(0))
    scheduler.schedule_temporal(offset_seconds=5.0, callback=lambda: asyncio.sleep(0))

    assert len(scheduler._tasks) == 2
    await scheduler.cancel_all()
    assert len(scheduler._tasks) == 0
