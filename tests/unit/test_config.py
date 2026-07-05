import pytest
from pydantic import ValidationError

from palamedes.config.schema import PalamedesConfig

# ---------------------------------------------------------------------------
# Minimal valid config used across tests
# ---------------------------------------------------------------------------

_VALID_RAW = {
    "experiment": {
        "id": "test-01",
        "target": {"container": "my-container"},
        "load": {
            "driver": "asyncio",
            "config": {
                "target_url": "http://localhost:8080",
                "arrival_rate_rps": 10.0,
            },
        },
        "fault": {
            "type": "container_stop",
            "target_container": "my-container",
            "trigger": {"type": "temporal", "offset_seconds": 10},
            "duration_seconds": 30,
        },
    }
}


def test_valid_config_parses():
    cfg = PalamedesConfig.model_validate(_VALID_RAW)
    assert cfg.experiment.id == "test-01"
    assert cfg.experiment.load.driver == "asyncio"
    assert cfg.experiment.fault.type == "container_stop"


def test_defaults_are_applied():
    cfg = PalamedesConfig.model_validate(_VALID_RAW)
    assert cfg.experiment.sla.max_error_rate_percent == 1.0
    assert cfg.experiment.metrics.collection_interval_ms == 500
    assert cfg.experiment.phases.recovery_timeout_seconds == 300


def test_invalid_driver_raises():
    raw = {
        **_VALID_RAW,
        "experiment": {**_VALID_RAW["experiment"], "load": {"driver": "invalid", "config": {}}},
    }
    with pytest.raises(ValidationError):
        PalamedesConfig.model_validate(raw)


def test_id_with_spaces_raises():
    raw = {
        **_VALID_RAW,
        "experiment": {**_VALID_RAW["experiment"], "id": "bad id!"},
    }
    with pytest.raises(ValidationError):
        PalamedesConfig.model_validate(raw)


def test_reactive_trigger_parsed():
    raw_fault = {
        "type": "container_stop",
        "target_container": "c",
        "trigger": {
            "type": "reactive",
            "metric": "cpu_percent",
            "threshold": 85.0,
            "comparator": "gt",
        },
    }
    raw = {
        **_VALID_RAW,
        "experiment": {**_VALID_RAW["experiment"], "fault": raw_fault},
    }
    cfg = PalamedesConfig.model_validate(raw)
    from palamedes.config.schema import ReactiveTrigger

    assert isinstance(cfg.experiment.fault.trigger, ReactiveTrigger)
    assert cfg.experiment.fault.trigger.threshold == 85.0


def test_batch_section_parsed():
    raw = {
        **_VALID_RAW,
        "batch": {
            "parameter_sweep": {
                "parameter": "load.config.arrival_rate_rps",
                "values": [10.0, 20.0, 50.0],
            },
            "repeat": 3,
        },
    }
    cfg = PalamedesConfig.model_validate(raw)
    assert cfg.batch is not None
    assert cfg.batch.repeat == 3
    assert cfg.batch.parameter_sweep.values == [10.0, 20.0, 50.0]


def test_apply_parameter_override():
    from palamedes.config.loader import apply_parameter_override

    cfg = PalamedesConfig.model_validate(_VALID_RAW)
    cfg2 = apply_parameter_override(cfg, "load.config.arrival_rate_rps", 200.0)
    from palamedes.config.schema import AsyncioLoadConfig

    driver_cfg = AsyncioLoadConfig(**cfg2.experiment.load.config)
    assert driver_cfg.arrival_rate_rps == 200.0
    # Original must be unchanged
    driver_orig = AsyncioLoadConfig(**cfg.experiment.load.config)
    assert driver_orig.arrival_rate_rps == 10.0
