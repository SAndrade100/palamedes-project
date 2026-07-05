from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class ExperimentPhase(str, Enum):
    IDLE = "IDLE"
    SETUP = "SETUP"
    WARMUP = "WARMUP"
    BASELINE = "BASELINE"
    FAULT_INJECTION = "FAULT_INJECTION"
    RECOVERY = "RECOVERY"
    TEARDOWN = "TEARDOWN"
    DONE = "DONE"
    ERROR = "ERROR"


class EventType(str, Enum):
    EXPERIMENT_START = "experiment_start"
    SETUP_COMPLETE = "setup_complete"
    WARMUP_START = "warmup_start"
    STEADY_STATE_REACHED = "steady_state_reached"
    BASELINE_START = "baseline_start"
    BASELINE_COMPLETE = "baseline_complete"
    FAULT_INJECTED = "fault_injected"
    DEGRADATION_DETECTED = "degradation_detected"
    RECOVERY_START = "recovery_start"
    RECOVERY_COMPLETE = "recovery_complete"
    TEARDOWN_START = "teardown_start"
    EXPERIMENT_END = "experiment_end"
    SLA_VIOLATION_START = "sla_violation_start"
    SLA_VIOLATION_END = "sla_violation_end"
    ERROR = "error"


@dataclass
class ExperimentEvent:
    experiment_id: str
    event_type: EventType
    ts_ms: int  # Unix timestamp in milliseconds
    phase: ExperimentPhase
    detail: Optional[str] = None


@dataclass
class EventTimeline:
    experiment_id: str
    events: list[ExperimentEvent] = field(default_factory=list)

    def record(
        self,
        event_type: EventType,
        phase: ExperimentPhase,
        detail: Optional[str] = None,
    ) -> ExperimentEvent:
        event = ExperimentEvent(
            experiment_id=self.experiment_id,
            event_type=event_type,
            ts_ms=int(time.time() * 1000),
            phase=phase,
            detail=detail,
        )
        self.events.append(event)
        return event

    def get(self, event_type: EventType) -> Optional[ExperimentEvent]:
        for e in self.events:
            if e.event_type == event_type:
                return e
        return None

    def get_ts(self, event_type: EventType) -> Optional[int]:
        ev = self.get(event_type)
        return ev.ts_ms if ev else None
