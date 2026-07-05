from __future__ import annotations

import asyncio
import json
import logging
import time
from pathlib import Path
from typing import Optional

from palamedes.config.schema import K6LoadConfig
from palamedes.models.events import ExperimentPhase
from palamedes.models.experiment import MetricSnapshot

logger = logging.getLogger(__name__)

_METRIC_FILE = "k6_metrics.jsonl"


class K6Driver:
    """
    Load driver that launches k6 as a subprocess with ``--out json``.
    Parses the streaming JSONL output to extract throughput and latency metrics.

    Requires k6 to be installed and available on PATH.
    """

    def __init__(self, config: K6LoadConfig, output_dir: str = ".") -> None:
        self._config = config
        self._output_dir = Path(output_dir)
        self._output_dir.mkdir(parents=True, exist_ok=True)
        self._metrics_path = self._output_dir / _METRIC_FILE
        self._process: Optional[asyncio.subprocess.Process] = None
        self._reader_task: Optional[asyncio.Task] = None
        self._latest: dict[str, float] = {}
        self._last_file_pos: int = 0

    async def start(self) -> None:
        script = Path(self._config.script)
        if not script.exists():
            raise FileNotFoundError(f"k6 script not found: {script}")

        # Remove stale metrics file
        if self._metrics_path.exists():
            self._metrics_path.unlink()
        self._last_file_pos = 0

        cmd = ["k6", "run", "--out", f"json={self._metrics_path}", str(script)]
        logger.info("Starting k6: %s", " ".join(cmd))
        self._process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        self._reader_task = asyncio.create_task(
            self._metrics_loop(), name="k6_metrics_reader"
        )

    async def stop(self) -> None:
        if self._process and self._process.returncode is None:
            try:
                self._process.terminate()
                await asyncio.wait_for(self._process.wait(), timeout=10.0)
            except asyncio.TimeoutError:
                self._process.kill()
        if self._reader_task:
            self._reader_task.cancel()
            try:
                await self._reader_task
            except asyncio.CancelledError:
                pass

    async def get_metrics(self, phase: ExperimentPhase) -> MetricSnapshot:
        return MetricSnapshot(
            ts_ms=int(time.time() * 1000),
            phase=phase,
            throughput_rps=self._latest.get("http_reqs_rate", 0.0),
            p50_latency_ms=self._latest.get("http_req_duration_p50", 0.0),
            p95_latency_ms=self._latest.get("http_req_duration_p95", 0.0),
            p99_latency_ms=self._latest.get("http_req_duration_p99", 0.0),
            error_rate_percent=self._latest.get("http_req_failed_rate", 0.0) * 100,
        )

    async def set_target_rps(self, rps: float) -> None:
        # k6 does not expose a stable runtime rate-change API without a REST
        # server configuration; log a warning.
        logger.warning(
            "K6Driver: dynamic rate adjustment requires k6 REST API (not configured)"
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _metrics_loop(self) -> None:
        """Tail the k6 JSON output file and update cached metrics."""
        while True:
            try:
                if self._metrics_path.exists():
                    with self._metrics_path.open("r", encoding="utf-8") as fh:
                        fh.seek(self._last_file_pos)
                        for line in fh:
                            self._parse_line(line.strip())
                        self._last_file_pos = fh.tell()
            except asyncio.CancelledError:
                return
            except Exception:
                logger.exception("k6 metrics parsing error")
            await asyncio.sleep(0.5)

    def _parse_line(self, line: str) -> None:
        if not line:
            return
        try:
            obj = json.loads(line)
            metric = obj.get("metric", "")
            data = obj.get("data", {})
            value = float(data.get("value", 0.0))
            obj_type = obj.get("type", "")

            if metric == "http_reqs" and obj_type == "Metric":
                self._latest["http_reqs_rate"] = value
            elif metric == "http_req_duration" and obj_type == "Point":
                # k6 emits individual samples; accumulate and track p50/p95/p99
                # via the Trend summary lines (type="Metric")
                pass
            elif metric == "http_req_duration" and obj_type == "Metric":
                contains = data.get("contains", "")
                if "p(50)" in contains:
                    self._latest["http_req_duration_p50"] = value
                elif "p(95)" in contains:
                    self._latest["http_req_duration_p95"] = value
                elif "p(99)" in contains:
                    self._latest["http_req_duration_p99"] = value
            elif metric == "http_req_failed" and obj_type == "Metric":
                self._latest["http_req_failed_rate"] = value
        except (json.JSONDecodeError, KeyError, ValueError):
            pass

    async def __aenter__(self) -> "K6Driver":
        await self.start()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.stop()
