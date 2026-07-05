from __future__ import annotations

import asyncio
import logging
import time
from typing import Optional

import psutil

from palamedes.models.events import ExperimentPhase
from palamedes.models.experiment import MetricSnapshot
from palamedes.telemetry.collector import TelemetryCollector

logger = logging.getLogger(__name__)


class InfraMetricsCollector:
    """
    Collects infrastructure metrics at a fixed interval (RF09):
    - Container CPU and memory usage via Docker Stats API
    - Host-level network I/O via psutil

    Snapshots are forwarded to the TelemetryCollector.
    Docker client is lazily initialized so the collector can be
    instantiated even if Docker is temporarily unavailable.
    """

    def __init__(
        self,
        container_name: str,
        collector: TelemetryCollector,
        interval_ms: int = 500,
    ) -> None:
        self._container_name = container_name
        self._collector = collector
        self._interval_s = interval_ms / 1000.0
        self._current_phase = ExperimentPhase.IDLE
        self._task: Optional[asyncio.Task] = None
        self._docker_client = None  # lazy init

    def set_phase(self, phase: ExperimentPhase) -> None:
        self._current_phase = phase

    async def start(self) -> None:
        self._task = asyncio.create_task(
            self._poll_loop(), name="infra_metrics_poller"
        )

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _poll_loop(self) -> None:
        loop = asyncio.get_event_loop()
        while True:
            try:
                snapshot = await loop.run_in_executor(None, self._collect_sync)
                if snapshot:
                    self._collector.add_snapshot(snapshot)
            except Exception:
                logger.exception("Error collecting infra metrics")
            await asyncio.sleep(self._interval_s)

    def _collect_sync(self) -> Optional[MetricSnapshot]:
        cpu_pct = 0.0
        mem_pct = 0.0
        net_sent = 0.0
        net_recv = 0.0

        # Docker container stats
        try:
            if self._docker_client is None:
                import docker
                self._docker_client = docker.from_env()
            container = self._docker_client.containers.get(self._container_name)
            stats = container.stats(stream=False)
            cpu_pct = _calc_cpu_percent(stats)
            mem_pct = _calc_mem_percent(stats)
        except Exception as exc:
            logger.debug("Docker stats unavailable for %r: %s", self._container_name, exc)

        # Host network I/O
        try:
            net = psutil.net_io_counters()
            net_sent = float(net.bytes_sent)
            net_recv = float(net.bytes_recv)
        except Exception:
            pass

        return MetricSnapshot(
            ts_ms=int(time.time() * 1000),
            phase=self._current_phase,
            cpu_percent=cpu_pct,
            memory_percent=mem_pct,
            network_bytes_sent=net_sent,
            network_bytes_recv=net_recv,
        )

    async def __aenter__(self) -> "InfraMetricsCollector":
        await self.start()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.stop()


def _calc_cpu_percent(stats: dict) -> float:
    """Derive CPU usage % from a Docker stats response dict."""
    try:
        cpu_delta = (
            stats["cpu_stats"]["cpu_usage"]["total_usage"]
            - stats["precpu_stats"]["cpu_usage"]["total_usage"]
        )
        system_delta = (
            stats["cpu_stats"]["system_cpu_usage"]
            - stats["precpu_stats"]["system_cpu_usage"]
        )
        percpu = stats["cpu_stats"]["cpu_usage"].get("percpu_usage") or [1]
        num_cpus = len(percpu)
        if system_delta > 0 and num_cpus > 0:
            return (cpu_delta / system_delta) * num_cpus * 100.0
    except (KeyError, ZeroDivisionError, TypeError):
        pass
    return 0.0


def _calc_mem_percent(stats: dict) -> float:
    """Derive memory usage % from a Docker stats response dict."""
    try:
        usage = stats["memory_stats"]["usage"]
        limit = stats["memory_stats"]["limit"]
        if limit > 0:
            return (usage / limit) * 100.0
    except (KeyError, ZeroDivisionError, TypeError):
        pass
    return 0.0
