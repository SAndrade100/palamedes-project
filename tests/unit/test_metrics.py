import os
import time
import tempfile

import duckdb
import pytest

from palamedes.analytics.metrics import compute_dependability_metrics
from palamedes.models.events import (
    EventTimeline,
    EventType,
    ExperimentEvent,
    ExperimentPhase,
)
from palamedes.models.experiment import ExperimentResult


def _seed_db(db_path: str, now_ms: int) -> None:
    """Create a minimal metrics.duckdb with synthetic data for all phases."""
    conn = duckdb.connect(db_path)
    conn.execute(
        """
        CREATE TABLE metrics (
            exp_id TEXT, ts_ms BIGINT, phase TEXT,
            throughput_rps DOUBLE, p50_latency_ms DOUBLE,
            p95_latency_ms DOUBLE, p99_latency_ms DOUBLE,
            error_rate_percent DOUBLE, cpu_percent DOUBLE,
            memory_percent DOUBLE, network_bytes_sent DOUBLE,
            network_bytes_recv DOUBLE, active_vus INTEGER
        )
        """
    )
    conn.execute(
        "CREATE TABLE events (exp_id TEXT, ts_ms BIGINT, phase TEXT, event_type TEXT, detail TEXT)"
    )
    # BASELINE: 1000 rps, no errors, p99=30ms
    for i in range(10):
        conn.execute(
            "INSERT INTO metrics VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            ["test", now_ms + i * 500, "BASELINE",
             1000.0, 10.0, 20.0, 30.0, 0.0, 20.0, 30.0, 0.0, 0.0, 100],
        )
    # FAULT_INJECTION: 200 rps, 50% errors, p99=600ms
    for i in range(10):
        conn.execute(
            "INSERT INTO metrics VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            ["test", now_ms + 5_000 + i * 500, "FAULT_INJECTION",
             200.0, 100.0, 200.0, 600.0, 50.0, 90.0, 80.0, 0.0, 0.0, 100],
        )
    # RECOVERY: 900 rps, error=0.3%, p99=35ms
    for i in range(10):
        conn.execute(
            "INSERT INTO metrics VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            ["test", now_ms + 10_000 + i * 500, "RECOVERY",
             900.0, 12.0, 22.0, 35.0, 0.3, 30.0, 40.0, 0.0, 0.0, 100],
        )
    conn.commit()
    conn.close()


@pytest.fixture
def experiment_result():
    now_ms = int(time.time() * 1000)
    with tempfile.TemporaryDirectory() as tmp:
        db_path = os.path.join(tmp, "metrics.duckdb")
        _seed_db(db_path, now_ms)

        timeline = EventTimeline(experiment_id="test")
        timeline.events.append(
            ExperimentEvent(
                experiment_id="test",
                event_type=EventType.FAULT_INJECTED,
                ts_ms=now_ms + 5_000,
                phase=ExperimentPhase.FAULT_INJECTION,
            )
        )
        timeline.events.append(
            ExperimentEvent(
                experiment_id="test",
                event_type=EventType.RECOVERY_COMPLETE,
                ts_ms=now_ms + 15_000,
                phase=ExperimentPhase.RECOVERY,
            )
        )

        result = ExperimentResult(
            experiment_id="test",
            config_path=tmp,
            timeline=timeline,
            db_path=db_path,
        )
        yield result


def test_mtrs(experiment_result):
    dm = compute_dependability_metrics(experiment_result)
    assert dm.mtrs_ms == pytest.approx(10_000.0, abs=1.0)


def test_baseline_throughput(experiment_result):
    dm = compute_dependability_metrics(experiment_result)
    assert dm.baseline_throughput_rps == pytest.approx(1000.0, abs=0.1)


def test_fault_min_throughput(experiment_result):
    dm = compute_dependability_metrics(experiment_result)
    assert dm.fault_min_throughput_rps == pytest.approx(200.0, abs=0.1)


def test_performance_attenuation(experiment_result):
    dm = compute_dependability_metrics(experiment_result)
    assert dm.performance_attenuation_pct == pytest.approx(80.0, abs=0.1)


def test_unavailability_window_positive(experiment_result):
    dm = compute_dependability_metrics(
        experiment_result,
        sla_max_error_rate_pct=1.0,
        sla_max_p99_ms=500.0,
    )
    # All FAULT_INJECTION rows have error_rate=50% > 1% → window > 0
    assert dm.unavailability_window_ms is not None
    assert dm.unavailability_window_ms > 0


def test_no_db_returns_empty():
    result = ExperimentResult(
        experiment_id="test",
        config_path=".",
        db_path=None,
    )
    dm = compute_dependability_metrics(result)
    assert dm.mtrs_ms is None
    assert dm.baseline_throughput_rps is None
