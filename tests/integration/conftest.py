"""
Shared fixtures for integration tests.
Provides a seeded DuckDB file and a matching ExperimentResult ready for analytics tests.
"""
from __future__ import annotations

import time

import duckdb
import pytest

from palamedes.models.events import (
    EventTimeline,
    EventType,
    ExperimentEvent,
    ExperimentPhase,
)
from palamedes.models.experiment import ExperimentResult


def seed_db(db_path: str, exp_id: str = "test") -> int:
    """
    Write synthetic three-phase data into a fresh DuckDB file.
    Returns ``now_ms`` (the base timestamp used for all rows).
    """
    now_ms = int(time.time() * 1000)
    conn = duckdb.connect(db_path)
    conn.execute("""
        CREATE TABLE metrics (
            exp_id TEXT, ts_ms BIGINT, phase TEXT,
            throughput_rps DOUBLE, p50_latency_ms DOUBLE,
            p95_latency_ms DOUBLE, p99_latency_ms DOUBLE,
            error_rate_percent DOUBLE, cpu_percent DOUBLE,
            memory_percent DOUBLE, network_bytes_sent DOUBLE,
            network_bytes_recv DOUBLE, active_vus INTEGER
        )
    """)
    conn.execute("""
        CREATE TABLE events (
            exp_id TEXT, ts_ms BIGINT, phase TEXT,
            event_type TEXT, detail TEXT
        )
    """)
    # BASELINE: healthy — 800 rps, 0% errors, p99 = 20 ms
    for i in range(10):
        conn.execute(
            "INSERT INTO metrics VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [exp_id, now_ms + i * 500, "BASELINE",
             800.0, 10.0, 15.0, 20.0, 0.0, 30.0, 40.0, 0.0, 0.0, 50],
        )
    # FAULT_INJECTION: degraded — 100 rps, 30% errors, p99 = 800 ms
    for i in range(10):
        conn.execute(
            "INSERT INTO metrics VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [exp_id, now_ms + 5_000 + i * 500, "FAULT_INJECTION",
             100.0, 300.0, 500.0, 800.0, 30.0, 90.0, 80.0, 0.0, 0.0, 50],
        )
    # RECOVERY: partially restored — 750 rps, 0.2% errors, p99 = 25 ms
    for i in range(10):
        conn.execute(
            "INSERT INTO metrics VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [exp_id, now_ms + 10_000 + i * 500, "RECOVERY",
             750.0, 12.0, 18.0, 25.0, 0.2, 35.0, 45.0, 0.0, 0.0, 50],
        )
    conn.execute(
        "INSERT INTO events VALUES (?,?,?,?,?)",
        [exp_id, now_ms + 5_000, "FAULT_INJECTION", "fault_injected", None],
    )
    conn.execute(
        "INSERT INTO events VALUES (?,?,?,?,?)",
        [exp_id, now_ms + 15_000, "RECOVERY", "recovery_complete", None],
    )
    conn.commit()
    conn.close()
    return now_ms


@pytest.fixture
def seeded_db(tmp_path):
    db_path = str(tmp_path / "metrics.duckdb")
    seed_db(db_path)
    return db_path


@pytest.fixture
def experiment_result_with_db(seeded_db, tmp_path):
    now_ms = int(time.time() * 1000)
    timeline = EventTimeline(experiment_id="test")
    timeline.events.extend([
        ExperimentEvent(
            experiment_id="test",
            event_type=EventType.FAULT_INJECTED,
            ts_ms=now_ms + 5_000,
            phase=ExperimentPhase.FAULT_INJECTION,
        ),
        ExperimentEvent(
            experiment_id="test",
            event_type=EventType.RECOVERY_COMPLETE,
            ts_ms=now_ms + 15_000,
            phase=ExperimentPhase.RECOVERY,
        ),
    ])
    return ExperimentResult(
        experiment_id="test",
        config_path=str(tmp_path),
        timeline=timeline,
        db_path=seeded_db,
    )
