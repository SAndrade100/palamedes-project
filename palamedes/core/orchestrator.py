from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path
from typing import Any, Optional

from palamedes.config.schema import FaultConfig, PalamedesConfig, ReactiveTrigger, TemporalTrigger
from palamedes.core.scheduler import Scheduler
from palamedes.core.state_machine import ExperimentFSM
from palamedes.models.events import EventTimeline, EventType, ExperimentPhase
from palamedes.models.experiment import DependabilityMetrics, ExperimentResult, PhaseRecord
from palamedes.telemetry.collector import TelemetryCollector
from palamedes.telemetry.infra import InfraMetricsCollector
from palamedes.telemetry.software import SoftwareMetricsCollector

logger = logging.getLogger(__name__)

# Maps fault type strings to injector classes (imported lazily to avoid
# importing docker/subprocess modules until actually needed)
_INJECTOR_MAP: dict[str, str] = {
    "container_stop": "palamedes.injectors.container.ContainerFaultInjector",
    "container_pause": "palamedes.injectors.container.ContainerFaultInjector",
    "container_kill": "palamedes.injectors.container.ContainerFaultInjector",
    "network_latency": "palamedes.injectors.network.NetworkFaultInjector",
    "network_loss": "palamedes.injectors.network.NetworkFaultInjector",
    "network_partition": "palamedes.injectors.network.NetworkFaultInjector",
    "cpu_stress": "palamedes.injectors.resource.ResourceFaultInjector",
    "memory_stress": "palamedes.injectors.resource.ResourceFaultInjector",
}


class Orchestrator:
    """
    Central experiment lifecycle coordinator (RF02).

    Drives the FSM through all phases while concurrently running:
    - Load driver  (traffic generation)
    - Software telemetry  (metrics from the load driver)
    - Infra telemetry  (Docker stats + psutil)
    - Fault injection  (triggered by the Scheduler)
    All metrics are streamed to a DuckDB file for subsequent analytics.
    """

    def __init__(self, config: PalamedesConfig, results_dir: str = "results") -> None:
        self._config = config
        self._exp_cfg = config.experiment
        self._results_dir = Path(results_dir) / self._exp_cfg.id
        self._results_dir.mkdir(parents=True, exist_ok=True)

        self._fsm = ExperimentFSM()
        self._timeline = EventTimeline(experiment_id=self._exp_cfg.id)
        self._result = ExperimentResult(
            experiment_id=self._exp_cfg.id,
            config_path=str(self._results_dir),
            timeline=self._timeline,
        )
        self._db_path = str(self._results_dir / "metrics.duckdb")

    # ------------------------------------------------------------------
    # Public entry-point
    # ------------------------------------------------------------------

    async def run(self) -> ExperimentResult:
        exp_id = self._exp_cfg.id
        logger.info("=== Experiment [%s] starting ===", exp_id)

        driver = self._build_driver()
        injector = self._build_injector()

        async with TelemetryCollector(exp_id, self._db_path) as collector:
            self._result.db_path = self._db_path
            async with driver:
                sw_collector = SoftwareMetricsCollector(
                    driver,
                    collector,
                    interval_ms=self._exp_cfg.metrics.collection_interval_ms,
                )
                infra_collector = InfraMetricsCollector(
                    self._exp_cfg.target.container,
                    collector,
                    interval_ms=self._exp_cfg.metrics.collection_interval_ms,
                )
                scheduler = Scheduler(sw_collector)

                async with sw_collector:
                    async with infra_collector:
                        flush_task = asyncio.create_task(
                            collector.run_flush_loop(1.0), name="db_flush"
                        )
                        try:
                            await self._run_phases(
                                sw_collector, infra_collector, injector, scheduler, collector
                            )
                        except Exception as exc:
                            logger.exception("Experiment error")
                            self._result.error = str(exc)
                            self._timeline.record(
                                EventType.ERROR, self._fsm.phase, str(exc)
                            )
                            # Best-effort teardown
                            try:
                                await injector.restore()
                            except Exception:
                                pass
                        finally:
                            await scheduler.cancel_all()
                            flush_task.cancel()
                            try:
                                await flush_task
                            except asyncio.CancelledError:
                                pass

        logger.info("=== Experiment [%s] finished ===", exp_id)
        return self._result

    # ------------------------------------------------------------------
    # Phase execution
    # ------------------------------------------------------------------

    async def _run_phases(
        self,
        sw_collector: SoftwareMetricsCollector,
        infra_collector: InfraMetricsCollector,
        injector: Any,
        scheduler: Scheduler,
        collector: TelemetryCollector,
    ) -> None:
        phases_cfg = self._exp_cfg.phases

        # ── SETUP ────────────────────────────────────────────────────────────
        self._fsm.transition(ExperimentPhase.SETUP)
        self._set_phase_all(ExperimentPhase.SETUP, sw_collector, infra_collector)
        self._timeline.record(EventType.EXPERIMENT_START, ExperimentPhase.SETUP)
        self._record_phase_start(ExperimentPhase.SETUP)
        await asyncio.sleep(2.0)  # let driver settle connections
        self._record_phase_end(ExperimentPhase.SETUP)
        self._timeline.record(EventType.SETUP_COMPLETE, ExperimentPhase.SETUP)
        collector.add_event(self._timeline.get(EventType.SETUP_COMPLETE))  # type: ignore[arg-type]

        # ── WARMUP ───────────────────────────────────────────────────────────
        self._fsm.transition(ExperimentPhase.WARMUP)
        self._set_phase_all(ExperimentPhase.WARMUP, sw_collector, infra_collector)
        self._timeline.record(EventType.WARMUP_START, ExperimentPhase.WARMUP)
        self._record_phase_start(ExperimentPhase.WARMUP)

        steady_reached = await self._wait_for_warmup(sw_collector, phases_cfg.warmup)
        if steady_reached:
            self._timeline.record(EventType.STEADY_STATE_REACHED, ExperimentPhase.WARMUP)
            ev = self._timeline.get(EventType.STEADY_STATE_REACHED)
            if ev:
                collector.add_event(ev)
        self._record_phase_end(ExperimentPhase.WARMUP)

        # ── BASELINE ─────────────────────────────────────────────────────────
        self._fsm.transition(ExperimentPhase.BASELINE)
        self._set_phase_all(ExperimentPhase.BASELINE, sw_collector, infra_collector)
        self._timeline.record(EventType.BASELINE_START, ExperimentPhase.BASELINE)
        self._record_phase_start(ExperimentPhase.BASELINE)

        await asyncio.sleep(phases_cfg.baseline.duration_seconds)

        self._record_phase_end(ExperimentPhase.BASELINE)
        self._timeline.record(EventType.BASELINE_COMPLETE, ExperimentPhase.BASELINE)
        ev = self._timeline.get(EventType.BASELINE_COMPLETE)
        if ev:
            collector.add_event(ev)

        # ── FAULT INJECTION ───────────────────────────────────────────────────
        self._fsm.transition(ExperimentPhase.FAULT_INJECTION)
        self._set_phase_all(ExperimentPhase.FAULT_INJECTION, sw_collector, infra_collector)
        self._record_phase_start(ExperimentPhase.FAULT_INJECTION)

        fault_cfg = self._exp_cfg.fault
        fault_injected_event = asyncio.Event()

        async def _inject_callback() -> None:
            await injector.inject()
            self._timeline.record(EventType.FAULT_INJECTED, ExperimentPhase.FAULT_INJECTION)
            ev = self._timeline.get(EventType.FAULT_INJECTED)
            if ev:
                collector.add_event(ev)
            fault_injected_event.set()
            logger.info(
                "Fault [%s] active; duration=%.1fs",
                fault_cfg.type,
                fault_cfg.duration_seconds,
            )
            if fault_cfg.duration_seconds > 0:
                await asyncio.sleep(fault_cfg.duration_seconds)
                await injector.restore()

        trigger = fault_cfg.trigger
        if isinstance(trigger, TemporalTrigger):
            scheduler.schedule_temporal(
                offset_seconds=trigger.offset_seconds,
                callback=_inject_callback,
            )
        else:
            assert isinstance(trigger, ReactiveTrigger)
            scheduler.schedule_reactive(
                metric=trigger.metric,
                threshold=trigger.threshold,
                comparator=trigger.comparator,
                callback=_inject_callback,
            )

        await fault_injected_event.wait()

        # ── RECOVERY ──────────────────────────────────────────────────────────
        self._fsm.transition(ExperimentPhase.RECOVERY)
        self._set_phase_all(ExperimentPhase.RECOVERY, sw_collector, infra_collector)
        self._timeline.record(EventType.RECOVERY_START, ExperimentPhase.RECOVERY)
        self._record_phase_end(ExperimentPhase.FAULT_INJECTION)
        self._record_phase_start(ExperimentPhase.RECOVERY)

        recovered = await self._wait_for_recovery(
            sw_collector,
            self._exp_cfg.sla,
            phases_cfg.recovery_timeout_seconds,
        )
        if recovered:
            self._timeline.record(EventType.RECOVERY_COMPLETE, ExperimentPhase.RECOVERY)
            ev = self._timeline.get(EventType.RECOVERY_COMPLETE)
            if ev:
                collector.add_event(ev)
        self._record_phase_end(ExperimentPhase.RECOVERY)

        # ── TEARDOWN ──────────────────────────────────────────────────────────
        self._fsm.transition(ExperimentPhase.TEARDOWN)
        self._timeline.record(EventType.TEARDOWN_START, ExperimentPhase.TEARDOWN)
        await injector.restore()  # idempotent — safe to call again

        self._fsm.transition(ExperimentPhase.DONE)
        self._timeline.record(EventType.EXPERIMENT_END, ExperimentPhase.DONE)
        ev = self._timeline.get(EventType.EXPERIMENT_END)
        if ev:
            collector.add_event(ev)

    # ------------------------------------------------------------------
    # Helper methods
    # ------------------------------------------------------------------

    def _set_phase_all(
        self,
        phase: ExperimentPhase,
        sw: SoftwareMetricsCollector,
        infra: InfraMetricsCollector,
    ) -> None:
        sw.set_phase(phase)
        infra.set_phase(phase)

    def _record_phase_start(self, phase: ExperimentPhase) -> None:
        self._result.phases[phase.value] = PhaseRecord(
            phase=phase,
            start_ts_ms=int(time.time() * 1000),
        )

    def _record_phase_end(self, phase: ExperimentPhase) -> None:
        record = self._result.phases.get(phase.value)
        if record:
            record.end_ts_ms = int(time.time() * 1000)

    def _build_driver(self) -> Any:
        load_cfg = self._exp_cfg.load
        if load_cfg.driver == "k6":
            from palamedes.drivers.k6 import K6Driver
            from palamedes.config.schema import K6LoadConfig

            return K6Driver(
                K6LoadConfig(**load_cfg.config),
                output_dir=str(self._results_dir),
            )
        from palamedes.drivers.asyncio_http import AsyncioHttpDriver
        from palamedes.config.schema import AsyncioLoadConfig

        return AsyncioHttpDriver(AsyncioLoadConfig(**load_cfg.config))

    def _build_injector(self) -> Any:
        fault_cfg = self._exp_cfg.fault
        dotted = _INJECTOR_MAP.get(fault_cfg.type)
        if not dotted:
            raise ValueError(f"Unknown fault type: {fault_cfg.type!r}")
        module_path, cls_name = dotted.rsplit(".", 1)
        import importlib

        mod = importlib.import_module(module_path)
        cls = getattr(mod, cls_name)
        return cls(fault_cfg)

    async def _wait_for_warmup(
        self,
        sw_collector: SoftwareMetricsCollector,
        warmup_cfg: Any,
    ) -> bool:
        """
        Await the warmup phase: duration elapses OR steady-state is detected.
        Steady-state = metric >= min_value for stability_window_seconds (RF02).
        """
        loop = asyncio.get_event_loop()
        deadline = loop.time() + warmup_cfg.duration_seconds
        ss = warmup_cfg.steady_state

        if ss is None:
            await asyncio.sleep(warmup_cfg.duration_seconds)
            return False

        stable_since: Optional[float] = None
        while loop.time() < deadline:
            snapshot = sw_collector.latest
            if snapshot is not None:
                value = getattr(snapshot, ss.metric, None)
                if value is not None and float(value) >= ss.min_value:
                    if stable_since is None:
                        stable_since = loop.time()
                    elif loop.time() - stable_since >= ss.stability_window_seconds:
                        return True
                else:
                    stable_since = None
            await asyncio.sleep(0.5)

        return False

    async def _wait_for_recovery(
        self,
        sw_collector: SoftwareMetricsCollector,
        sla_cfg: Any,
        timeout_seconds: int,
    ) -> bool:
        """
        Await recovery: SLA metrics are restored for 3 consecutive readings,
        or timeout_seconds elapses (RF10).
        """
        loop = asyncio.get_event_loop()
        deadline = loop.time() + timeout_seconds
        consecutive_ok = 0
        required_ok = 3

        while loop.time() < deadline:
            snapshot = sw_collector.latest
            if snapshot is not None:
                ok = (
                    snapshot.error_rate_percent <= sla_cfg.max_error_rate_percent
                    and snapshot.p99_latency_ms <= sla_cfg.max_p99_latency_ms
                )
                if ok:
                    consecutive_ok += 1
                    if consecutive_ok >= required_ok:
                        return True
                else:
                    consecutive_ok = 0
            await asyncio.sleep(0.5)

        return False
