from __future__ import annotations

from typing import Annotated, Any, Literal, Optional, Union

from pydantic import BaseModel, Field, model_validator


# ---------------------------------------------------------------------------
# Steady-state detection
# ---------------------------------------------------------------------------


class SteadyStateConfig(BaseModel):
    metric: str = "throughput_rps"
    min_value: float = Field(gt=0)
    stability_window_seconds: int = Field(default=10, ge=5)


# ---------------------------------------------------------------------------
# Phase configuration
# ---------------------------------------------------------------------------


class WarmupPhaseConfig(BaseModel):
    duration_seconds: int = Field(default=60, ge=10)
    steady_state: Optional[SteadyStateConfig] = None


class BaselinePhaseConfig(BaseModel):
    duration_seconds: int = Field(default=120, ge=30)


class PhasesConfig(BaseModel):
    warmup: WarmupPhaseConfig = Field(default_factory=WarmupPhaseConfig)
    baseline: BaselinePhaseConfig = Field(default_factory=BaselinePhaseConfig)
    recovery_timeout_seconds: int = Field(default=300, ge=30)


# ---------------------------------------------------------------------------
# Load configuration  (RF04, RF05)
# ---------------------------------------------------------------------------


class LoadRampStep(BaseModel):
    duration: str  # e.g. "30s"
    target: int = Field(ge=1)


class K6LoadConfig(BaseModel):
    script: str
    vus: int = Field(default=10, ge=1)
    ramp: Optional[list[LoadRampStep]] = None


class AsyncioLoadConfig(BaseModel):
    target_url: str
    method: str = "GET"
    arrival_rate_rps: float = Field(default=10.0, gt=0)
    ramp: Optional[list[LoadRampStep]] = None


class LoadConfig(BaseModel):
    driver: Literal["k6", "asyncio"] = "asyncio"
    config: dict[str, Any] = Field(default_factory=dict)

    def get_driver_config(self) -> K6LoadConfig | AsyncioLoadConfig:
        if self.driver == "k6":
            return K6LoadConfig(**self.config)
        return AsyncioLoadConfig(**self.config)


# ---------------------------------------------------------------------------
# Trigger configuration  (RF06)
# ---------------------------------------------------------------------------


class TemporalTrigger(BaseModel):
    type: Literal["temporal"]
    offset_seconds: float = Field(ge=0)


class ReactiveTrigger(BaseModel):
    type: Literal["reactive"]
    metric: str
    threshold: float
    comparator: Literal["gt", "lt", "gte", "lte"] = "gt"


TriggerConfig = Annotated[
    Union[TemporalTrigger, ReactiveTrigger],
    Field(discriminator="type"),
]


# ---------------------------------------------------------------------------
# Fault configuration  (RF07)
# ---------------------------------------------------------------------------


FaultType = Literal[
    "container_stop",
    "container_pause",
    "container_kill",
    "network_latency",
    "network_loss",
    "network_partition",
    "cpu_stress",
    "memory_stress",
]


class FaultConfig(BaseModel):
    type: FaultType
    target_container: str
    trigger: TriggerConfig
    duration_seconds: float = Field(default=60.0, ge=0)
    parameters: dict[str, Any] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# SLA configuration  (RF10, RF12)
# ---------------------------------------------------------------------------


class SLAConfig(BaseModel):
    max_error_rate_percent: float = Field(default=1.0, ge=0, le=100)
    max_p99_latency_ms: float = Field(default=500.0, gt=0)


# ---------------------------------------------------------------------------
# Metrics configuration  (RF08, RF09)
# ---------------------------------------------------------------------------


SoftwareMetric = Literal[
    "throughput_rps",
    "p50_latency_ms",
    "p95_latency_ms",
    "p99_latency_ms",
    "error_rate_percent",
]

InfraMetric = Literal[
    "cpu_percent",
    "memory_percent",
    "network_bytes_sent",
    "network_bytes_recv",
]


class MetricsConfig(BaseModel):
    collection_interval_ms: int = Field(default=500, ge=100)
    software: list[SoftwareMetric] = Field(
        default=[
            "throughput_rps",
            "p95_latency_ms",
            "p99_latency_ms",
            "error_rate_percent",
        ]
    )
    infra: list[InfraMetric] = Field(
        default=["cpu_percent", "memory_percent"]
    )


# ---------------------------------------------------------------------------
# Target configuration
# ---------------------------------------------------------------------------


class TargetConfig(BaseModel):
    compose_file: Optional[str] = None
    container: str
    service: Optional[str] = None


# ---------------------------------------------------------------------------
# Batch / parameter-sweep configuration  (RF03)
# ---------------------------------------------------------------------------


class ParameterSweepConfig(BaseModel):
    parameter: str  # dotted path relative to experiment, e.g. "load.config.vus"
    values: list[Any] = Field(min_length=2)


class BatchConfig(BaseModel):
    parameter_sweep: ParameterSweepConfig
    repeat: int = Field(default=1, ge=1)


# ---------------------------------------------------------------------------
# Top-level models
# ---------------------------------------------------------------------------


class ExperimentConfig(BaseModel):
    id: str = Field(min_length=1, pattern=r"^[a-zA-Z0-9_\-]+$")
    description: Optional[str] = None
    target: TargetConfig
    phases: PhasesConfig = Field(default_factory=PhasesConfig)
    load: LoadConfig
    fault: FaultConfig
    sla: SLAConfig = Field(default_factory=SLAConfig)
    metrics: MetricsConfig = Field(default_factory=MetricsConfig)


class PalamedesConfig(BaseModel):
    experiment: ExperimentConfig
    batch: Optional[BatchConfig] = None
