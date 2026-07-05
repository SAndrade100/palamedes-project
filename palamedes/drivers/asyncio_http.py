from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from typing import Optional

import aiohttp
import numpy as np

from palamedes.config.schema import AsyncioLoadConfig
from palamedes.models.events import ExperimentPhase
from palamedes.models.experiment import MetricSnapshot

logger = logging.getLogger(__name__)

# Rolling window for throughput and percentile calculations
_WINDOW_S = 5.0


class AsyncioHttpDriver:
    """
    Load driver using aiohttp with Poisson inter-arrival times (RF04, RF05).

    Arrival process: inter-arrival = Exp(1/λ) via numpy.random.exponential.
    Provides per-request latency tracing and supports runtime rate adjustment.
    """

    def __init__(self, config: AsyncioLoadConfig) -> None:
        self._config = config
        self._arrival_rate = config.arrival_rate_rps
        self._session: Optional[aiohttp.ClientSession] = None
        self._running = False
        self._generator_task: Optional[asyncio.Task] = None
        # (timestamp_s, latency_ms, success)
        self._response_times: deque[tuple[float, float, bool]] = deque()
        self._active_requests = 0

    async def start(self) -> None:
        self._session = aiohttp.ClientSession()
        self._running = True
        self._generator_task = asyncio.create_task(
            self._generator_loop(), name="aiohttp_generator"
        )
        logger.info(
            "AsyncioHttpDriver started: %s  @ %.1f rps (Poisson)",
            self._config.target_url,
            self._arrival_rate,
        )

    async def stop(self) -> None:
        self._running = False
        if self._generator_task:
            self._generator_task.cancel()
            try:
                await self._generator_task
            except asyncio.CancelledError:
                pass
        if self._session:
            await self._session.close()

    async def set_target_rps(self, rps: float) -> None:
        self._arrival_rate = max(rps, 0.0)
        logger.info("AsyncioHttpDriver: rate updated to %.1f rps", rps)

    async def get_metrics(self, phase: ExperimentPhase) -> MetricSnapshot:
        now = time.time()
        cutoff = now - _WINDOW_S
        recent = [
            (ts, lat, ok)
            for ts, lat, ok in self._response_times
            if ts >= cutoff
        ]
        ts_ms = int(now * 1000)

        if not recent:
            return MetricSnapshot(
                ts_ms=ts_ms, phase=phase, active_vus=self._active_requests
            )

        throughput = len(recent) / _WINDOW_S
        latencies = sorted(lat for _, lat, _ in recent)
        errors = sum(1 for _, _, ok in recent if not ok)
        error_rate = (errors / len(latencies)) * 100.0
        n = len(latencies)

        def pct(p: float) -> float:
            idx = min(int(p / 100.0 * n), n - 1)
            return latencies[idx]

        return MetricSnapshot(
            ts_ms=ts_ms,
            phase=phase,
            throughput_rps=throughput,
            p50_latency_ms=pct(50),
            p95_latency_ms=pct(95),
            p99_latency_ms=pct(99),
            error_rate_percent=error_rate,
            active_vus=self._active_requests,
        )

    # ------------------------------------------------------------------
    # Internal coroutines
    # ------------------------------------------------------------------

    async def _generator_loop(self) -> None:
        """Generate requests following a Poisson process."""
        while self._running:
            if self._arrival_rate > 0:
                interval = float(np.random.exponential(1.0 / self._arrival_rate))
            else:
                interval = 1.0
            await asyncio.sleep(interval)
            asyncio.create_task(self._send_request(), name="http_req")

    async def _send_request(self) -> None:
        if self._session is None:
            return
        self._active_requests += 1
        start = time.perf_counter()
        success = False
        try:
            async with self._session.request(
                self._config.method,
                self._config.target_url,
                timeout=aiohttp.ClientTimeout(total=30.0),
            ) as resp:
                success = resp.status < 500
                await resp.read()
        except Exception:
            pass
        finally:
            latency_ms = (time.perf_counter() - start) * 1000.0
            ts = time.time()
            self._response_times.append((ts, latency_ms, success))
            # Prune entries older than 2× the rolling window
            cutoff = ts - _WINDOW_S * 2
            while self._response_times and self._response_times[0][0] < cutoff:
                self._response_times.popleft()
            self._active_requests -= 1

    async def __aenter__(self) -> "AsyncioHttpDriver":
        await self.start()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.stop()
