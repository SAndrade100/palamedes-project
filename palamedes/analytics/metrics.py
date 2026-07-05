from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import duckdb
import polars as pl

from palamedes.models.events import EventType
from palamedes.models.experiment import DependabilityMetrics, ExperimentResult

logger = logging.getLogger(__name__)


def compute_dependability_metrics(
    result: ExperimentResult,
    sla_max_error_rate_pct: float = 1.0,
    sla_max_p99_ms: float = 500.0,
) -> DependabilityMetrics:
    """
    Compute empirical dependability metrics from DuckDB telemetry (RF12):

    - **MTRS**: time from ``fault_injected`` to ``recovery_complete``
    - **Unavailability window**: cumulative milliseconds where SLA was violated
      during FAULT_INJECTION or RECOVERY phases
    - **Performance attenuation**: percentage throughput drop during fault vs baseline
    """
    if not result.db_path or not Path(result.db_path).exists():
        logger.warning("No DuckDB available; cannot compute dependability metrics")
        return DependabilityMetrics()

    timeline = result.timeline
    fault_ts = timeline.get_ts(EventType.FAULT_INJECTED) if timeline else None
    recovery_ts = timeline.get_ts(EventType.RECOVERY_COMPLETE) if timeline else None

    dm = DependabilityMetrics(
        fault_injected_ts_ms=fault_ts,
        recovery_complete_ts_ms=recovery_ts,
    )

    # MTRS
    if fault_ts is not None and recovery_ts is not None:
        dm.mtrs_ms = float(recovery_ts - fault_ts)

    conn = duckdb.connect(result.db_path, read_only=True)
    try:
        # Baseline throughput
        row = conn.execute(
            "SELECT AVG(throughput_rps) FROM metrics WHERE phase = 'BASELINE'"
        ).fetchone()
        dm.baseline_throughput_rps = row[0] if row and row[0] is not None else None

        # Minimum throughput during fault
        row = conn.execute(
            "SELECT MIN(throughput_rps) FROM metrics WHERE phase = 'FAULT_INJECTION'"
        ).fetchone()
        dm.fault_min_throughput_rps = row[0] if row and row[0] is not None else None

        # Performance attenuation %
        if (
            dm.baseline_throughput_rps
            and dm.baseline_throughput_rps > 0
            and dm.fault_min_throughput_rps is not None
        ):
            dm.performance_attenuation_pct = (
                (dm.baseline_throughput_rps - dm.fault_min_throughput_rps)
                / dm.baseline_throughput_rps
            ) * 100.0

        # Unavailability window: sum of intervals where SLA was violated
        df = conn.execute(
            """
            SELECT ts_ms,
                   CASE
                       WHEN error_rate_percent > ? OR p99_latency_ms > ?
                       THEN 1 ELSE 0
                   END AS violated
            FROM metrics
            WHERE phase IN ('FAULT_INJECTION', 'RECOVERY')
            ORDER BY ts_ms
            """,
            [sla_max_error_rate_pct, sla_max_p99_ms],
        ).pl()

        if len(df) > 1:
            df = df.with_columns(
                (pl.col("ts_ms").shift(-1) - pl.col("ts_ms")).alias("interval_ms")
            )
            violated_ms = df.filter(pl.col("violated") == 1)["interval_ms"].sum()
            dm.unavailability_window_ms = float(violated_ms) if violated_ms else 0.0

    except Exception:
        logger.exception("Error computing dependability metrics from %s", result.db_path)
    finally:
        conn.close()

    return dm
