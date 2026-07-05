from __future__ import annotations

from typing import Protocol, runtime_checkable

from palamedes.models.events import ExperimentPhase
from palamedes.models.experiment import MetricSnapshot


@runtime_checkable
class LoadDriver(Protocol):
    """
    Abstract interface for load generators (RF04).

    All implementations must be usable as async context managers:
    ``__aenter__`` calls ``start()`` and ``__aexit__`` calls ``stop()``.
    """

    async def start(self) -> None:
        """Start the load generation process."""
        ...

    async def stop(self) -> None:
        """Stop the load generation process gracefully."""
        ...

    async def get_metrics(self, phase: ExperimentPhase) -> MetricSnapshot:
        """Return the latest aggregated metric snapshot."""
        ...

    async def set_target_rps(self, rps: float) -> None:
        """Dynamically adjust the target request rate (RF05)."""
        ...

    async def __aenter__(self) -> "LoadDriver":
        await self.start()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.stop()
