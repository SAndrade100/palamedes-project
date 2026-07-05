from __future__ import annotations

import json
import logging
from pathlib import Path

import duckdb

from palamedes.models.experiment import ExperimentResult

logger = logging.getLogger(__name__)


def export_csv(result: ExperimentResult, output_dir: str) -> list[str]:
    """
    Export ``metrics`` and ``events`` tables from DuckDB to CSV files (RF11).
    Returns the list of written file paths.
    """
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    if not result.db_path:
        raise ValueError("No db_path on result")

    conn = duckdb.connect(result.db_path, read_only=True)
    exported: list[str] = []
    try:
        for table in ("metrics", "events"):
            path = out / f"{result.experiment_id}_{table}.csv"
            conn.execute(f"COPY {table} TO '{path}' (HEADER, DELIMITER ',')")
            exported.append(str(path))
            logger.info("Exported %s → %s", table, path)
    finally:
        conn.close()

    return exported


def export_json(result: ExperimentResult, output_dir: str) -> str:
    """
    Export the full ExperimentResult (phases, timeline, dependability) to JSON (RF11).
    Returns the written file path.
    """
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    payload: dict = {
        "experiment_id": result.experiment_id,
        "success": result.success,
        "error": result.error,
        "phases": {
            k: {
                "phase": v.phase.value,
                "start_ts_ms": v.start_ts_ms,
                "end_ts_ms": v.end_ts_ms,
                "duration_ms": v.duration_ms,
            }
            for k, v in result.phases.items()
        },
        "timeline": [
            {
                "event_type": e.event_type.value,
                "ts_ms": e.ts_ms,
                "phase": e.phase.value,
                "detail": e.detail,
            }
            for e in (result.timeline.events if result.timeline else [])
        ],
        "dependability": None,
    }

    if result.dependability:
        d = result.dependability
        payload["dependability"] = {
            "mtrs_ms": d.mtrs_ms,
            "unavailability_window_ms": d.unavailability_window_ms,
            "performance_attenuation_pct": d.performance_attenuation_pct,
            "baseline_throughput_rps": d.baseline_throughput_rps,
            "fault_min_throughput_rps": d.fault_min_throughput_rps,
            "fault_injected_ts_ms": d.fault_injected_ts_ms,
            "recovery_complete_ts_ms": d.recovery_complete_ts_ms,
        }

    path = out / f"{result.experiment_id}_result.json"
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    logger.info("Exported result → %s", path)
    return str(path)


def export_parquet(result: ExperimentResult, output_dir: str) -> str:
    """
    Export ``metrics`` table to Parquet (RF11 — structured open format).
    Returns the written file path.
    """
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    if not result.db_path:
        raise ValueError("No db_path on result")

    path = out / f"{result.experiment_id}_metrics.parquet"
    conn = duckdb.connect(result.db_path, read_only=True)
    try:
        conn.execute(f"COPY metrics TO '{path}' (FORMAT PARQUET)")
    finally:
        conn.close()

    logger.info("Exported Parquet → %s", path)
    return str(path)
