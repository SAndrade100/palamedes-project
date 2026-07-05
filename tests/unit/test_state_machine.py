import asyncio

import pytest

from palamedes.core.state_machine import ExperimentFSM, InvalidTransitionError
from palamedes.models.events import ExperimentPhase


def test_initial_state():
    fsm = ExperimentFSM()
    assert fsm.phase == ExperimentPhase.IDLE


def test_valid_transition_to_setup():
    fsm = ExperimentFSM()
    fsm.transition(ExperimentPhase.SETUP)
    assert fsm.phase == ExperimentPhase.SETUP


def test_invalid_transition_from_idle_raises():
    fsm = ExperimentFSM()
    with pytest.raises(InvalidTransitionError):
        fsm.transition(ExperimentPhase.RECOVERY)


def test_invalid_skip_raises():
    fsm = ExperimentFSM()
    fsm.transition(ExperimentPhase.SETUP)
    with pytest.raises(InvalidTransitionError):
        fsm.transition(ExperimentPhase.DONE)


def test_full_happy_path():
    fsm = ExperimentFSM()
    for phase in [
        ExperimentPhase.SETUP,
        ExperimentPhase.WARMUP,
        ExperimentPhase.BASELINE,
        ExperimentPhase.FAULT_INJECTION,
        ExperimentPhase.RECOVERY,
        ExperimentPhase.TEARDOWN,
        ExperimentPhase.DONE,
    ]:
        fsm.transition(phase)
    assert fsm.phase == ExperimentPhase.DONE


def test_error_from_any_phase():
    fsm = ExperimentFSM()
    fsm.transition(ExperimentPhase.SETUP)
    fsm.transition(ExperimentPhase.ERROR)
    assert fsm.phase == ExperimentPhase.ERROR
    # From ERROR, only TEARDOWN is allowed
    fsm.transition(ExperimentPhase.TEARDOWN)
    assert fsm.phase == ExperimentPhase.TEARDOWN


def test_reset_returns_to_idle():
    fsm = ExperimentFSM()
    fsm.transition(ExperimentPhase.SETUP)
    fsm.reset()
    assert fsm.phase == ExperimentPhase.IDLE


@pytest.mark.asyncio
async def test_wait_for_phase_resolves():
    fsm = ExperimentFSM()

    async def _advance():
        await asyncio.sleep(0.05)
        fsm.transition(ExperimentPhase.SETUP)

    asyncio.create_task(_advance())
    await asyncio.wait_for(fsm.wait_for(ExperimentPhase.SETUP), timeout=2.0)
    assert fsm.phase == ExperimentPhase.SETUP


@pytest.mark.asyncio
async def test_wait_for_already_reached():
    fsm = ExperimentFSM()
    # IDLE is already set at construction time
    await asyncio.wait_for(fsm.wait_for(ExperimentPhase.IDLE), timeout=0.1)
