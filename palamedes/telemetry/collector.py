from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Optional

import duckdb

from palamedes.models.events import ExperimentEvent
from palamedes.models.experiment import MetricSnapshot

logger = logging.getLogger(__name__)

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS metrics (
    exp_id              TEXT    NOT NULL,
    ts_ms               BIGINT  NOT NULL,
    phase               TEXT    NOT NULL,
    throughput_rps      DOUBLE,
    p50_latency_ms      DOUBLE,
    p95_latency_ms      DOUBLE,
    p99_latency_ms      DOUBLE,
    error_rate_percent  DOUBLE,
    cpu_percent         DOUBLE,
    memory_percent      DOUBLE,
    network_bytes_sent  DOUBLE,
    network_bytes_recv  DOUBLE,
    active_vus          INTEGER
);

CREATE TABLE IF NOT EXISTS events (
    exp_id      TEXT    NOT NULL,
    ts_ms       BIGINT  NOT NULL,
    phase       TEXT    NOT NULL,
    event_type  TEXT    NOT NULL,
    detail      TEXT
);
"""


class TelemetryCollector:
    """
    Async telemetry collector: buffers MetricSnapshots and ExperimentEvents
    and flushes them to DuckDB in batches (RF08, RF09, RF10, RF11).

    Usage::

        async with TelemetryCollector(exp_id, "results/exp/metrics.duckdb") as collector:
            collector.add_snapshot(snapshot)
            asyncio.create_task(collector.run_flush_loop())
    """

    def __init__(self, experiment_id: str, db_path: str) -> None:
        self._exp_id = experiment_id
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn: Optional[duckdb.DuckDBPyConnection] = None
        self._metric_buffer: list[MetricSnapshot] = []
        self._event_buffer: list[ExperimentEvent] = []

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def open(self) -> None:
        self._conn = duckdb.connect(str(self._db_path))
        self._conn.execute(_SCHEMA_SQL)
        self._conn.commit()
        logger.info("TelemetryCollector opened: %s", self._db_path)

    def close(self) -> None:
        self._flush_sync()
        if self._conn:
            self._conn.close()
            self._conn = None

    # ------------------------------------------------------------------
    # Buffer writes (thread-safe at GIL level for list.append)
    # ------------------------------------------------------------------

    def add_snapshot(self, snapshot: MetricSnapshot) -> None:
        self._metric_buffer.append(snapshot)

    def add_event(self, event: ExperimentEvent) -> None:
        self._event_buffer.append(event)

    # ------------------------------------------------------------------
    # Background flush loop
    # ------------------------------------------------------------------

    async def run_flush_loop(self, interval_s: float = 1.0) -> None:
        """Flush buffers to DuckDB at *interval_s* intervals."""
        try:
            while True:
                await asyncio.sleep(interval_s)
                self._flush_sync()
        except asyncio.CancelledError:
            self._flush_sync()  # final flush on cancellation

    # ------------------------------------------------------------------
    # Synchronous flush (runs in event-loop thread)
    # ------------------------------------------------------------------

    def _flush_sync(self) -> None:
        if not self._conn:
            return

        if self._metric_buffer:
            rows = [
                (
                    self._exp_id,
                    s.ts_ms,
                    s.phase.value,
                    s.throughput_rps,
                    s.p50_latency_ms,
                    s.p95_latency_ms,
                    s.p99_latency_ms,
                    s.error_rate_percent,
                    s.cpu_percent,
                    s.memory_percent,
                    s.network_bytes_sent,
                    s.network_bytes_recv,
                    s.active_vus,
                )
                for s in self._metric_buffer
            ]
            self._conn.executemany(
                "INSERT INTO metrics VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", rows
            )
            self._conn.commit()
            self._metric_buffer.clear()

        if self._event_buffer:
            rows_ev = [
                (
                    e.experiment_id,
                    e.ts_ms,
                    e.phase.value,
                    e.event_type.value,
                    e.detail,
                )
                for e in self._event_buffer
            ]
            self._conn.executemany(
                "INSERT INTO events VALUES (?,?,?,?,?)", rows_ev
            )
            self._conn.commit()
            self._event_buffer.clear()

    # ------------------------------------------------------------------
    # Async context manager
    # ------------------------------------------------------------------

    async def __aenter__(self) -> "TelemetryCollector":
        self.open()
        return self

    async def __aexit__(self, *_: object) -> None:
        self.close()
