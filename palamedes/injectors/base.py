from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class FaultInjector(Protocol):
    """
    Abstract interface for fault injectors (RF07).

    Implementations must guarantee that ``restore()`` is called even on
    exceptions by being used as async context managers.
    """

    async def inject(self) -> None:
        """Apply the fault to the target."""
        ...

    async def restore(self) -> None:
        """Undo the fault and return the target to normal operation."""
        ...

    async def verify_injected(self) -> bool:
        """Return True if the fault is currently active."""
        ...

    async def __aenter__(self) -> "FaultInjector":
        await self.inject()
        return self

    async def __aexit__(self, exc_type: object, *_: object) -> None:
        await self.restore()
