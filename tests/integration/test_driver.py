"""
Integration tests for AsyncioHttpDriver against a real aiohttp server.
No mocks — the driver sends actual HTTP requests and the metrics reflect them.
"""
from __future__ import annotations

import asyncio

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from palamedes.config.schema import AsyncioLoadConfig
from palamedes.drivers.asyncio_http import AsyncioHttpDriver
from palamedes.models.events import ExperimentPhase


async def _ok_handler(request: web.Request) -> web.Response:
    return web.Response(text="OK")


async def _error_handler(request: web.Request) -> web.Response:
    return web.Response(status=503, text="unavailable")


def _ok_app() -> web.Application:
    app = web.Application()
    app.router.add_route("*", "/{path:.*}", _ok_handler)
    return app


def _error_app() -> web.Application:
    app = web.Application()
    app.router.add_route("*", "/{path:.*}", _error_handler)
    return app


@pytest.mark.asyncio
async def test_driver_reports_nonzero_throughput():
    async with TestServer(_ok_app()) as server:
        url = str(server.make_url("/"))
        cfg = AsyncioLoadConfig(target_url=url, arrival_rate_rps=20.0)
        async with AsyncioHttpDriver(cfg) as driver:
            await asyncio.sleep(2.0)
            snapshot = await driver.get_metrics(ExperimentPhase.BASELINE)

    assert snapshot.throughput_rps > 0
    assert snapshot.p99_latency_ms > 0
    assert snapshot.error_rate_percent == pytest.approx(0.0, abs=2.0)


@pytest.mark.asyncio
async def test_driver_reports_high_error_rate_on_server_errors():
    async with TestServer(_error_app()) as server:
        url = str(server.make_url("/"))
        cfg = AsyncioLoadConfig(target_url=url, arrival_rate_rps=20.0)
        async with AsyncioHttpDriver(cfg) as driver:
            await asyncio.sleep(2.0)
            snapshot = await driver.get_metrics(ExperimentPhase.FAULT_INJECTION)

    # All responses are 503 → error_rate should be close to 100 %
    assert snapshot.error_rate_percent > 50.0


@pytest.mark.asyncio
async def test_driver_latency_percentiles_ordered():
    async with TestServer(_ok_app()) as server:
        url = str(server.make_url("/"))
        cfg = AsyncioLoadConfig(target_url=url, arrival_rate_rps=30.0)
        async with AsyncioHttpDriver(cfg) as driver:
            await asyncio.sleep(2.0)
            snapshot = await driver.get_metrics(ExperimentPhase.BASELINE)

    assert snapshot.p50_latency_ms <= snapshot.p95_latency_ms
    assert snapshot.p95_latency_ms <= snapshot.p99_latency_ms


@pytest.mark.asyncio
async def test_driver_rate_update_increases_throughput():
    async with TestServer(_ok_app()) as server:
        url = str(server.make_url("/"))
        cfg = AsyncioLoadConfig(target_url=url, arrival_rate_rps=5.0)
        async with AsyncioHttpDriver(cfg) as driver:
            await driver.set_target_rps(50.0)
            await asyncio.sleep(2.0)
            snapshot = await driver.get_metrics(ExperimentPhase.BASELINE)

    # After bumping rate to 50 rps, throughput must be well above the original 5
    assert snapshot.throughput_rps > 15.0


@pytest.mark.asyncio
async def test_driver_empty_snapshot_before_any_request_completes():
    # Use model_construct to bypass the gt=0 validator on arrival_rate_rps.
    # With rate=0 the generator sleeps 1 s before firing the first request,
    # so metrics polled immediately after start() see an empty window.
    async with TestServer(_ok_app()) as server:
        url = str(server.make_url("/"))
        cfg = AsyncioLoadConfig.model_construct(
            target_url=url, method="GET", arrival_rate_rps=0.0, ramp=None
        )
        async with AsyncioHttpDriver(cfg) as driver:
            snapshot = await driver.get_metrics(ExperimentPhase.IDLE)

    assert snapshot.throughput_rps == 0.0
    assert snapshot.p99_latency_ms == 0.0


@pytest.mark.asyncio
async def test_driver_phase_recorded_in_snapshot():
    async with TestServer(_ok_app()) as server:
        url = str(server.make_url("/"))
        cfg = AsyncioLoadConfig(target_url=url, arrival_rate_rps=10.0)
        async with AsyncioHttpDriver(cfg) as driver:
            await asyncio.sleep(1.0)
            snapshot = await driver.get_metrics(ExperimentPhase.RECOVERY)

    assert snapshot.phase == ExperimentPhase.RECOVERY
