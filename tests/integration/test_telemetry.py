"""
Integration tests for TelemetryCollector: DuckDB schema creation, buffering,
synchronous and asynchronous flush, and phase labelling.
"""
from __future__ import annotations

import asyncio
import time

import duckdb
import pytest

from palamedes.models.events import ExperimentEvent, EventType, ExperimentPhase
from palamedes.models.experiment import MetricSnapshot
from palamedes.telemetry.collector import TelemetryCollector


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _snapshot(phase: ExperimentPhase = ExperimentPhase.BASELINE) -> MetricSnapshot:
    return MetricSnapshot(
        ts_ms=int(time.time() * 1000),
        phase=phase,
        throughput_rps=500.0,
        p50_latency_ms=10.0,
        p95_latency_ms=20.0,
        p99_latency_ms=30.0,
        error_rate_percent=0.5,
        cpu_percent=25.0,
        memory_percent=40.0,
        active_vus=50,
    )


def _event(phase: ExperimentPhase = ExperimentPhase.SETUP) -> ExperimentEvent:
    return ExperimentEvent(
        experiment_id="test",
        event_type=EventType.SETUP_COMPLETE,
        ts_ms=int(time.time() * 1000),
        phase=phase,
    )


def _count(db_path: str, table: str) -> int:
    conn = duckdb.connect(db_path, read_only=True)
    n = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    conn.close()
    return n


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_collector_creates_metrics_and_events_tables(tmp_path):
    db_path = str(tmp_path / "m.duckdb")
    async with TelemetryCollector("test", db_path):
        pass

    conn = duckdb.connect(db_path, read_only=True)
    tables = {r[0] for r in conn.execute("SHOW TABLES").fetchall()}
    conn.close()
    assert {"metrics", "events"} <= tables


@pytest.mark.asyncio
async def test_open_is_idempotent(tmp_path):
    """Calling open() on an existing DB must not raise (IF NOT EXISTS guard)."""
    db_path = str(tmp_path / "m.duckdb")
    c = TelemetryCollector("test", db_path)
    c.open()
    c.open()  # second call must not raise
    c.close()


# ---------------------------------------------------------------------------
# Snapshot persistence
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_single_snapshot_persisted_after_flush(tmp_path):
    db_path = str(tmp_path / "m.duckdb")
    async with TelemetryCollector("test", db_path) as col:
        col.add_snapshot(_snapshot())
        col._flush_sync()

    assert _count(db_path, "metrics") == 1


@pytest.mark.asyncio
async def test_multiple_snapshots_all_persisted(tmp_path):
    db_path = str(tmp_path / "m.duckdb")
    n = 25
    async with TelemetryCollector("exp-multi", db_path) as col:
        for _ in range(n):
            col.add_snapshot(_snapshot())
        col._flush_sync()

    assert _count(db_path, "metrics") == n


@pytest.mark.asyncio
async def test_snapshot_phase_stored_correctly(tmp_path):
    db_path = str(tmp_path / "m.duckdb")
    async with TelemetryCollector("test", db_path) as col:
        col.add_snapshot(_snapshot(ExperimentPhase.FAULT_INJECTION))
        col._flush_sync()

    conn = duckdb.connect(db_path, read_only=True)
    phase = conn.execute("SELECT phase FROM metrics").fetchone()[0]
    conn.close()
    assert phase == "FAULT_INJECTION"


@pytest.mark.asyncio
async def test_snapshot_values_stored_correctly(tmp_path):
    db_path = str(tmp_path / "m.duckdb")
    snap = _snapshot()
    async with TelemetryCollector("test", db_path) as col:
        col.add_snapshot(snap)
        col._flush_sync()

    conn = duckdb.connect(db_path, read_only=True)
    row = conn.execute("SELECT throughput_rps, p99_latency_ms, error_rate_percent FROM metrics").fetchone()
    conn.close()
    assert row[0] == pytest.approx(500.0)
    assert row[1] == pytest.approx(30.0)
    assert row[2] == pytest.approx(0.5)


# ---------------------------------------------------------------------------
# Event persistence
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_single_event_persisted_after_flush(tmp_path):
    db_path = str(tmp_path / "m.duckdb")
    async with TelemetryCollector("test", db_path) as col:
        col.add_event(_event())
        col._flush_sync()

    assert _count(db_path, "events") == 1


@pytest.mark.asyncio
async def test_event_type_stored_correctly(tmp_path):
    db_path = str(tmp_path / "m.duckdb")
    ev = ExperimentEvent(
        experiment_id="test",
        event_type=EventType.FAULT_INJECTED,
        ts_ms=int(time.time() * 1000),
        phase=ExperimentPhase.FAULT_INJECTION,
        detail="cpu_stress active",
    )
    async with TelemetryCollector("test", db_path) as col:
        col.add_event(ev)
        col._flush_sync()

    conn = duckdb.connect(db_path, read_only=True)
    row = conn.execute("SELECT event_type, detail FROM events").fetchone()
    conn.close()
    assert row[0] == "fault_injected"
    assert row[1] == "cpu_stress active"


# ---------------------------------------------------------------------------
# Close flushes remaining buffer
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_close_flushes_remaining_buffer(tmp_path):
    db_path = str(tmp_path / "m.duckdb")
    col = TelemetryCollector("test", db_path)
    col.open()
    for _ in range(7):
        col.add_snapshot(_snapshot())
    # close() must flush without an explicit _flush_sync() call
    col.close()

    assert _count(db_path, "metrics") == 7


# ---------------------------------------------------------------------------
# Async flush loop
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_flush_loop_writes_snapshots_to_db(tmp_path):
    db_path = str(tmp_path / "m.duckdb")
    async with TelemetryCollector("test", db_path) as col:
        flush_task = asyncio.create_task(col.run_flush_loop(interval_s=0.1))
        col.add_snapshot(_snapshot())
        await asyncio.sleep(0.35)  # allow at least one flush cycle
        flush_task.cancel()
        try:
            await flush_task
        except asyncio.CancelledError:
            pass

    assert _count(db_path, "metrics") >= 1


@pytest.mark.asyncio
async def test_flush_loop_cancellation_triggers_final_flush(tmp_path):
    """Cancelling run_flush_loop must flush whatever is left in the buffer."""
    db_path = str(tmp_path / "m.duckdb")
    async with TelemetryCollector("test", db_path) as col:
        # Use a very long interval so the first automatic flush hasn't run yet
        flush_task = asyncio.create_task(col.run_flush_loop(interval_s=60.0))
        col.add_snapshot(_snapshot())
        col.add_snapshot(_snapshot())
        flush_task.cancel()
        try:
            await flush_task
        except asyncio.CancelledError:
            pass

    assert _count(db_path, "metrics") == 2


# ---------------------------------------------------------------------------
# Multi-experiment isolation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_multiple_experiments_isolated_in_same_db(tmp_path):
    db_path = str(tmp_path / "m.duckdb")
    async with TelemetryCollector("exp-a", db_path) as col_a:
        for _ in range(3):
            col_a.add_snapshot(_snapshot())
        col_a._flush_sync()

    async with TelemetryCollector("exp-b", db_path) as col_b:
        for _ in range(5):
            col_b.add_snapshot(_snapshot())
        col_b._flush_sync()

    conn = duckdb.connect(db_path, read_only=True)
    a = conn.execute("SELECT COUNT(*) FROM metrics WHERE exp_id = 'exp-a'").fetchone()[0]
    b = conn.execute("SELECT COUNT(*) FROM metrics WHERE exp_id = 'exp-b'").fetchone()[0]
    conn.close()
    assert a == 3
    assert b == 5
