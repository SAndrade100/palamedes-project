"""
Integration tests for the full experiment lifecycle (Orchestrator).

The Docker-based fault injector is replaced by a no-op stub so the tests run
without a live container runtime.  A real aiohttp server serves as the load
target and a real DuckDB file is created for telemetry verification.

Phase durations are shortened via ``model_construct`` (bypasses Pydantic
validators) so the full experiment completes in roughly 6–8 seconds.
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import duckdb
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from palamedes.config.schema import (
    AsyncioLoadConfig,
    BaselinePhaseConfig,
    FaultConfig,
    LoadConfig,
    MetricsConfig,
    PalamedesConfig,
    PhasesConfig,
    SLAConfig,
    TargetConfig,
    TemporalTrigger,
    WarmupPhaseConfig,
    ExperimentConfig,
)
from palamedes.core.orchestrator import Orchestrator


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _NoopInjector:
    """Fault injector stub — inject/restore are no-ops, no Docker required."""

    async def inject(self) -> None:
        pass

    async def restore(self) -> None:
        pass

    async def verify_injected(self) -> bool:
        return True


def _build_fast_config(target_url: str) -> PalamedesConfig:
    """
    Build an experiment config with durations well below the schema minimums
    (1 s warmup, 1 s baseline, 0.2 s trigger offset) using model_construct to
    bypass Pydantic field validators.
    """
    exp = ExperimentConfig.model_construct(
        id="orch-integration",
        description=None,
        target=TargetConfig(container="test-container"),
        phases=PhasesConfig.model_construct(
            warmup=WarmupPhaseConfig.model_construct(
                duration_seconds=1, steady_state=None
            ),
            baseline=BaselinePhaseConfig.model_construct(duration_seconds=1),
            recovery_timeout_seconds=5,
        ),
        load=LoadConfig(
            driver="asyncio",
            config={"target_url": target_url, "arrival_rate_rps": 10.0},
        ),
        fault=FaultConfig(
            type="container_stop",
            target_container="test-container",
            trigger=TemporalTrigger(type="temporal", offset_seconds=0.2),
            # duration_seconds=0 means the injector is "restored" immediately
            # inside the callback; fine since the injector is a no-op.
            duration_seconds=0.0,
        ),
        # Generous SLA so the local server is always "within spec"
        sla=SLAConfig(max_error_rate_percent=10.0, max_p99_latency_ms=5000.0),
        metrics=MetricsConfig.model_construct(
            collection_interval_ms=200,
            software=["throughput_rps", "p99_latency_ms", "error_rate_percent"],
            infra=["cpu_percent"],
        ),
    )
    return PalamedesConfig.model_construct(experiment=exp, batch=None)


async def _ok_handler(request: web.Request) -> web.Response:
    return web.Response(text="OK")


def _ok_app() -> web.Application:
    app = web.Application()
    app.router.add_route("*", "/{path:.*}", _ok_handler)
    return app


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_orchestrator_completes_successfully(tmp_path):
    async with TestServer(_ok_app()) as server:
        cfg = _build_fast_config(str(server.make_url("/")))
        with patch.object(Orchestrator, "_build_injector", new=lambda self: _NoopInjector()):
            result = await Orchestrator(cfg, results_dir=str(tmp_path)).run()

    assert result.success, f"Experiment failed unexpectedly: {result.error}"
    assert result.error is None


@pytest.mark.asyncio
async def test_orchestrator_creates_duckdb_file(tmp_path):
    async with TestServer(_ok_app()) as server:
        cfg = _build_fast_config(str(server.make_url("/")))
        with patch.object(Orchestrator, "_build_injector", new=lambda self: _NoopInjector()):
            result = await Orchestrator(cfg, results_dir=str(tmp_path)).run()

    assert result.db_path is not None
    assert Path(result.db_path).exists()


@pytest.mark.asyncio
async def test_orchestrator_populates_metrics_table(tmp_path):
    async with TestServer(_ok_app()) as server:
        cfg = _build_fast_config(str(server.make_url("/")))
        with patch.object(Orchestrator, "_build_injector", new=lambda self: _NoopInjector()):
            result = await Orchestrator(cfg, results_dir=str(tmp_path)).run()

    conn = duckdb.connect(result.db_path, read_only=True)
    metric_count = conn.execute("SELECT COUNT(*) FROM metrics").fetchone()[0]
    conn.close()

    assert metric_count > 0


@pytest.mark.asyncio
async def test_orchestrator_records_all_key_events(tmp_path):
    async with TestServer(_ok_app()) as server:
        cfg = _build_fast_config(str(server.make_url("/")))
        with patch.object(Orchestrator, "_build_injector", new=lambda self: _NoopInjector()):
            result = await Orchestrator(cfg, results_dir=str(tmp_path)).run()

    assert result.timeline is not None
    recorded = {e.event_type.value for e in result.timeline.events}

    assert "experiment_start" in recorded
    assert "baseline_start" in recorded
    assert "fault_injected" in recorded
    assert "experiment_end" in recorded


@pytest.mark.asyncio
async def test_orchestrator_metrics_cover_multiple_phases(tmp_path):
    async with TestServer(_ok_app()) as server:
        cfg = _build_fast_config(str(server.make_url("/")))
        with patch.object(Orchestrator, "_build_injector", new=lambda self: _NoopInjector()):
            result = await Orchestrator(cfg, results_dir=str(tmp_path)).run()

    conn = duckdb.connect(result.db_path, read_only=True)
    phases = {r[0] for r in conn.execute("SELECT DISTINCT phase FROM metrics").fetchall()}
    conn.close()

    # At minimum SETUP and BASELINE must have recorded metrics
    assert len(phases) >= 2


@pytest.mark.asyncio
async def test_orchestrator_events_persisted_in_duckdb(tmp_path):
    async with TestServer(_ok_app()) as server:
        cfg = _build_fast_config(str(server.make_url("/")))
        with patch.object(Orchestrator, "_build_injector", new=lambda self: _NoopInjector()):
            result = await Orchestrator(cfg, results_dir=str(tmp_path)).run()

    conn = duckdb.connect(result.db_path, read_only=True)
    event_count = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    conn.close()

    assert event_count > 0


@pytest.mark.asyncio
async def test_orchestrator_raises_for_unknown_fault_type(tmp_path):
    async with TestServer(_ok_app()) as server:
        cfg = _build_fast_config(str(server.make_url("/")))
        # Bypass schema to inject an invalid fault type at runtime
        cfg.experiment.fault = FaultConfig.model_construct(
            type="nonexistent_fault",
            target_container="test-container",
            trigger=TemporalTrigger(type="temporal", offset_seconds=0.2),
            duration_seconds=0.0,
            parameters={},
        )

        with pytest.raises(ValueError, match="Unknown fault type"):
            await Orchestrator(cfg, results_dir=str(tmp_path)).run()
