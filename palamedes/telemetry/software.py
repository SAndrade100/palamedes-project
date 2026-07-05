from __future__ import annotations

import asyncio
import logging
from typing import Any, Optional

from palamedes.models.events import ExperimentPhase
from palamedes.models.experiment import MetricSnapshot
from palamedes.telemetry.collector import TelemetryCollector

logger = logging.getLogger(__name__)


class SoftwareMetricsCollector:
    """
    Polls the active load driver for software metrics at a fixed interval
    and forwards each MetricSnapshot to the TelemetryCollector (RF08).
    """

    def __init__(
        self,
        driver: Any,  # LoadDriver
        collector: TelemetryCollector,
        interval_ms: int = 500,
    ) -> None:
        self._driver = driver
        self._collector = collector
        self._interval_s = interval_ms / 1000.0
        self._current_phase = ExperimentPhase.IDLE
        self._latest: Optional[MetricSnapshot] = None
        self._task: Optional[asyncio.Task] = None

    def set_phase(self, phase: ExperimentPhase) -> None:
        self._current_phase = phase

    @property
    def latest(self) -> Optional[MetricSnapshot]:
        return self._latest

    async def start(self) -> None:
        self._task = asyncio.create_task(
            self._poll_loop(), name="sw_metrics_poller"
        )

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _poll_loop(self) -> None:
        while True:
            try:
                snapshot = await self._driver.get_metrics(self._current_phase)
                self._latest = snapshot
                self._collector.add_snapshot(snapshot)
            except Exception:
                logger.exception("Error polling software metrics")
            await asyncio.sleep(self._interval_s)

    async def __aenter__(self) -> "SoftwareMetricsCollector":
        await self.start()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.stop()
