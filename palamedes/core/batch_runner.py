from __future__ import annotations

import logging

from palamedes.config.loader import apply_parameter_override
from palamedes.config.schema import PalamedesConfig
from palamedes.core.orchestrator import Orchestrator
from palamedes.models.experiment import ExperimentResult

logger = logging.getLogger(__name__)


class BatchRunner:
    """
    Executes a parameter sweep: runs the experiment N×M times,
    iterating over each value in the sweep and repeating each run (RF03).
    """

    def __init__(
        self,
        base_config: PalamedesConfig,
        results_dir: str = "results",
    ) -> None:
        self._base = base_config
        self._results_dir = results_dir

    async def run(self) -> list[ExperimentResult]:
        if self._base.batch is None:
            raise ValueError("BatchRunner requires a [batch] section in config")

        sweep = self._base.batch.parameter_sweep
        repeat = self._base.batch.repeat
        results: list[ExperimentResult] = []

        for value in sweep.values:
            for rep in range(1, repeat + 1):
                config = apply_parameter_override(
                    self._base, sweep.parameter, value
                )
                # Make the experiment ID unique per sweep slot
                raw = config.model_dump()
                param_key = sweep.parameter.split(".")[-1]
                raw["experiment"]["id"] = (
                    f"{self._base.experiment.id}__{param_key}_{value}__r{rep}"
                )
                config = PalamedesConfig.model_validate(raw)

                logger.info(
                    "Batch — %s=%s  repeat=%d/%d  id=%s",
                    sweep.parameter,
                    value,
                    rep,
                    repeat,
                    config.experiment.id,
                )

                orchestrator = Orchestrator(config, results_dir=self._results_dir)
                result = await orchestrator.run()
                results.append(result)

                if not result.success:
                    logger.error(
                        "Batch run failed: %s — %s",
                        config.experiment.id,
                        result.error,
                    )

        return results
