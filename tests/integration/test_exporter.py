"""
Integration tests for the analytics exporters (CSV, JSON, Parquet).
Each test uses a seeded DuckDB via the ``experiment_result_with_db`` fixture
from conftest.py — no mocks required.
"""
from __future__ import annotations

import json
from pathlib import Path

import duckdb
import polars as pl
import pytest

from palamedes.analytics.exporter import export_csv, export_json, export_parquet
from palamedes.models.experiment import ExperimentResult


# ---------------------------------------------------------------------------
# CSV export
# ---------------------------------------------------------------------------


def test_export_csv_creates_two_files(experiment_result_with_db, tmp_path):
    paths = export_csv(experiment_result_with_db, str(tmp_path / "out"))

    assert len(paths) == 2
    assert all(Path(p).exists() for p in paths)


def test_export_csv_creates_metrics_and_events_files(experiment_result_with_db, tmp_path):
    paths = export_csv(experiment_result_with_db, str(tmp_path / "out"))

    names = {Path(p).name for p in paths}
    assert any("metrics" in n for n in names)
    assert any("events" in n for n in names)


def test_export_csv_metrics_contains_expected_columns(experiment_result_with_db, tmp_path):
    paths = export_csv(experiment_result_with_db, str(tmp_path / "out"))
    metrics_path = next(p for p in paths if "metrics" in p)

    df = pl.read_csv(metrics_path)
    for col in ("throughput_rps", "p99_latency_ms", "error_rate_percent", "phase"):
        assert col in df.columns


def test_export_csv_metrics_row_count_matches_db(experiment_result_with_db, tmp_path):
    conn = duckdb.connect(experiment_result_with_db.db_path, read_only=True)
    expected = conn.execute("SELECT COUNT(*) FROM metrics").fetchone()[0]
    conn.close()

    paths = export_csv(experiment_result_with_db, str(tmp_path / "out"))
    metrics_path = next(p for p in paths if "metrics" in p)
    df = pl.read_csv(metrics_path)

    assert len(df) == expected


def test_export_csv_raises_when_no_db_path(tmp_path):
    result = ExperimentResult(experiment_id="nodb", config_path=str(tmp_path), db_path=None)
    with pytest.raises(ValueError, match="No db_path"):
        export_csv(result, str(tmp_path / "out"))


# ---------------------------------------------------------------------------
# JSON export
# ---------------------------------------------------------------------------


def test_export_json_creates_file(experiment_result_with_db, tmp_path):
    path = export_json(experiment_result_with_db, str(tmp_path / "out"))
    assert Path(path).exists()


def test_export_json_has_required_top_level_keys(experiment_result_with_db, tmp_path):
    path = export_json(experiment_result_with_db, str(tmp_path / "out"))
    data = json.loads(Path(path).read_text())

    for key in ("experiment_id", "success", "error", "phases", "timeline", "dependability"):
        assert key in data


def test_export_json_experiment_id_matches(experiment_result_with_db, tmp_path):
    path = export_json(experiment_result_with_db, str(tmp_path / "out"))
    data = json.loads(Path(path).read_text())

    assert data["experiment_id"] == "test"


def test_export_json_timeline_contains_fault_and_recovery_events(experiment_result_with_db, tmp_path):
    path = export_json(experiment_result_with_db, str(tmp_path / "out"))
    data = json.loads(Path(path).read_text())

    event_types = {e["event_type"] for e in data["timeline"]}
    assert "fault_injected" in event_types
    assert "recovery_complete" in event_types


def test_export_json_success_is_true_when_no_error(experiment_result_with_db, tmp_path):
    path = export_json(experiment_result_with_db, str(tmp_path / "out"))
    data = json.loads(Path(path).read_text())

    assert data["success"] is True
    assert data["error"] is None


def test_export_json_with_dependability_metrics(experiment_result_with_db, tmp_path):
    from palamedes.analytics.metrics import compute_dependability_metrics

    experiment_result_with_db.dependability = compute_dependability_metrics(
        experiment_result_with_db
    )
    path = export_json(experiment_result_with_db, str(tmp_path / "out"))
    data = json.loads(Path(path).read_text())

    assert data["dependability"] is not None
    assert "mtrs_ms" in data["dependability"]


# ---------------------------------------------------------------------------
# Parquet export
# ---------------------------------------------------------------------------


def test_export_parquet_creates_file(experiment_result_with_db, tmp_path):
    path = export_parquet(experiment_result_with_db, str(tmp_path / "out"))
    assert Path(path).exists()


def test_export_parquet_row_count_matches_db(experiment_result_with_db, tmp_path):
    conn = duckdb.connect(experiment_result_with_db.db_path, read_only=True)
    expected = conn.execute("SELECT COUNT(*) FROM metrics").fetchone()[0]
    conn.close()

    path = export_parquet(experiment_result_with_db, str(tmp_path / "out"))
    df = pl.read_parquet(path)

    assert len(df) == expected


def test_export_parquet_contains_throughput_column(experiment_result_with_db, tmp_path):
    path = export_parquet(experiment_result_with_db, str(tmp_path / "out"))
    df = pl.read_parquet(path)

    assert "throughput_rps" in df.columns


def test_export_parquet_raises_when_no_db_path(tmp_path):
    result = ExperimentResult(experiment_id="nodb", config_path=str(tmp_path), db_path=None)
    with pytest.raises(ValueError, match="No db_path"):
        export_parquet(result, str(tmp_path / "out"))
