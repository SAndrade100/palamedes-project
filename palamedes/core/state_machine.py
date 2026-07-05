from __future__ import annotations

import asyncio
import logging

from palamedes.models.events import ExperimentPhase

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Valid phase transitions
# ---------------------------------------------------------------------------

_TRANSITIONS: dict[ExperimentPhase, set[ExperimentPhase]] = {
    ExperimentPhase.IDLE: {ExperimentPhase.SETUP, ExperimentPhase.ERROR},
    ExperimentPhase.SETUP: {
        ExperimentPhase.WARMUP,
        ExperimentPhase.ERROR,
        ExperimentPhase.TEARDOWN,
    },
    ExperimentPhase.WARMUP: {
        ExperimentPhase.BASELINE,
        ExperimentPhase.ERROR,
        ExperimentPhase.TEARDOWN,
    },
    ExperimentPhase.BASELINE: {
        ExperimentPhase.FAULT_INJECTION,
        ExperimentPhase.ERROR,
        ExperimentPhase.TEARDOWN,
    },
    ExperimentPhase.FAULT_INJECTION: {
        ExperimentPhase.RECOVERY,
        ExperimentPhase.ERROR,
        ExperimentPhase.TEARDOWN,
    },
    ExperimentPhase.RECOVERY: {ExperimentPhase.TEARDOWN, ExperimentPhase.ERROR},
    ExperimentPhase.TEARDOWN: {ExperimentPhase.DONE, ExperimentPhase.ERROR},
    ExperimentPhase.DONE: set(),
    ExperimentPhase.ERROR: {ExperimentPhase.TEARDOWN},
}


class InvalidTransitionError(Exception):
    pass


class ExperimentFSM:
    """
    Finite State Machine for the experiment lifecycle.

    Each successful transition fires an asyncio.Event so that any coroutine
    awaiting a specific phase is unblocked immediately.
    """

    def __init__(self) -> None:
        self._phase = ExperimentPhase.IDLE
        self._events: dict[ExperimentPhase, asyncio.Event] = {
            phase: asyncio.Event() for phase in ExperimentPhase
        }
        self._events[ExperimentPhase.IDLE].set()

    @property
    def phase(self) -> ExperimentPhase:
        return self._phase

    def transition(self, target: ExperimentPhase) -> None:
        allowed = _TRANSITIONS.get(self._phase, set())
        if target not in allowed:
            raise InvalidTransitionError(
                f"Cannot transition {self._phase.value} → {target.value}. "
                f"Allowed: {sorted(p.value for p in allowed)}"
            )
        logger.info("Phase: %s → %s", self._phase.value, target.value)
        self._phase = target
        self._events[target].set()

    async def wait_for(self, phase: ExperimentPhase) -> None:
        """Suspend until the FSM reaches *phase*."""
        await self._events[phase].wait()

    def reset(self) -> None:
        """Clear all events and return to IDLE (used between batch iterations)."""
        for event in self._events.values():
            event.clear()
        self._phase = ExperimentPhase.IDLE
        self._events[ExperimentPhase.IDLE].set()
