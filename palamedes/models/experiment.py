from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

from palamedes.models.events import EventTimeline, ExperimentPhase


@dataclass
class MetricSnapshot:
    ts_ms: int
    phase: ExperimentPhase
    throughput_rps: float = 0.0
    p50_latency_ms: float = 0.0
    p95_latency_ms: float = 0.0
    p99_latency_ms: float = 0.0
    error_rate_percent: float = 0.0
    cpu_percent: float = 0.0
    memory_percent: float = 0.0
    network_bytes_sent: float = 0.0
    network_bytes_recv: float = 0.0
    active_vus: int = 0
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass
class PhaseRecord:
    phase: ExperimentPhase
    start_ts_ms: int
    end_ts_ms: Optional[int] = None
    snapshots: list[MetricSnapshot] = field(default_factory=list)

    @property
    def duration_ms(self) -> Optional[int]:
        if self.end_ts_ms is None:
            return None
        return self.end_ts_ms - self.start_ts_ms


@dataclass
class DependabilityMetrics:
    """Empirical dependability metrics computed from collected telemetry (RF12)."""

    mtrs_ms: Optional[float] = None
    unavailability_window_ms: Optional[float] = None
    performance_attenuation_pct: Optional[float] = None
    baseline_throughput_rps: Optional[float] = None
    fault_min_throughput_rps: Optional[float] = None
    recovery_complete_ts_ms: Optional[int] = None
    fault_injected_ts_ms: Optional[int] = None


@dataclass
class ExperimentResult:
    experiment_id: str
    config_path: str
    phases: dict[str, PhaseRecord] = field(default_factory=dict)
    timeline: Optional[EventTimeline] = None
    dependability: Optional[DependabilityMetrics] = None
    error: Optional[str] = None
    db_path: Optional[str] = None

    @property
    def success(self) -> bool:
        return self.error is None
