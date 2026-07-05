from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable, Coroutine, Optional

logger = logging.getLogger(__name__)

TriggerCallback = Callable[[], Coroutine[Any, Any, None]]


class Scheduler:
    """
    Manages temporal and reactive fault-injection triggers (RF06).

    - Temporal: fire callback after a fixed offset from the moment of scheduling.
    - Reactive: poll the latest metric snapshot and fire when a threshold is crossed.
    """

    def __init__(self, software_collector: Any) -> None:  # SoftwareMetricsCollector
        self._sw = software_collector
        self._tasks: list[asyncio.Task] = []

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def schedule_temporal(
        self,
        offset_seconds: float,
        callback: TriggerCallback,
        name: str = "temporal_trigger",
    ) -> asyncio.Task:
        task = asyncio.create_task(
            self._temporal(offset_seconds, callback),
            name=name,
        )
        self._tasks.append(task)
        return task

    def schedule_reactive(
        self,
        metric: str,
        threshold: float,
        comparator: str,
        callback: TriggerCallback,
        poll_interval_s: float = 0.5,
        name: str = "reactive_trigger",
    ) -> asyncio.Task:
        task = asyncio.create_task(
            self._reactive(metric, threshold, comparator, callback, poll_interval_s),
            name=name,
        )
        self._tasks.append(task)
        return task

    async def cancel_all(self) -> None:
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

    # ------------------------------------------------------------------
    # Internal coroutines
    # ------------------------------------------------------------------

    @staticmethod
    async def _temporal(offset: float, callback: TriggerCallback) -> None:
        logger.info("Temporal trigger armed: fires in %.1fs", offset)
        await asyncio.sleep(offset)
        logger.info("Temporal trigger fired")
        await callback()

    async def _reactive(
        self,
        metric: str,
        threshold: float,
        comparator: str,
        callback: TriggerCallback,
        poll_interval_s: float,
    ) -> None:
        _comparators: dict[str, Callable[[float, float], bool]] = {
            "gt": lambda v, t: v > t,
            "lt": lambda v, t: v < t,
            "gte": lambda v, t: v >= t,
            "lte": lambda v, t: v <= t,
        }
        compare = _comparators.get(comparator, _comparators["gt"])
        logger.info("Reactive trigger armed: %s %s %.2f", metric, comparator, threshold)

        while True:
            snapshot = self._sw.latest
            if snapshot is not None:
                value = getattr(snapshot, metric, None)
                if value is not None and compare(float(value), threshold):
                    logger.info(
                        "Reactive trigger fired: %s=%.2f %s %.2f",
                        metric,
                        value,
                        comparator,
                        threshold,
                    )
                    await callback()
                    return
            await asyncio.sleep(poll_interval_s)
