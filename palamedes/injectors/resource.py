from __future__ import annotations

import asyncio
import logging
from typing import Optional

import docker
import docker.errors
from docker.models.containers import Container

from palamedes.config.schema import FaultConfig

logger = logging.getLogger(__name__)


class ResourceFaultInjector:
    """
    Injects resource-exhaustion faults via stress-ng executed inside the
    target container through docker exec (RF07):
    - cpu_stress:    N workers spinning on CPU for duration seconds
    - memory_stress: N workers allocating vm_bytes of RAM for duration seconds

    Requires stress-ng to be installed inside the target container.
    """

    def __init__(self, config: FaultConfig) -> None:
        self._config = config
        self._client = docker.from_env()
        self._container: Optional[Container] = None
        self._stress_task: Optional[asyncio.Task] = None

    async def inject(self) -> None:
        name = self._config.target_container
        try:
            self._container = self._client.containers.get(name)
        except docker.errors.NotFound:
            raise RuntimeError(f"Container not found: {name!r}")

        cmd = self._build_stress_cmd()
        logger.info("Resource fault on %r: %s", name, " ".join(cmd))
        # stress-ng self-terminates after --timeout; we run it as a background task
        self._stress_task = asyncio.create_task(
            self._run_stress(cmd), name="stress_ng"
        )

    async def restore(self) -> None:
        if self._stress_task and not self._stress_task.done():
            if self._container:
                try:
                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(
                        None,
                        lambda: self._container.exec_run(  # type: ignore[union-attr]
                            ["pkill", "-f", "stress-ng"], detach=True
                        ),
                    )
                except Exception as exc:
                    logger.warning("Failed to kill stress-ng: %s", exc)
            self._stress_task.cancel()
            try:
                await self._stress_task
            except asyncio.CancelledError:
                pass

    async def verify_injected(self) -> bool:
        if not self._container:
            return False
        try:
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None,
                lambda: self._container.exec_run(["pgrep", "-f", "stress-ng"]),  # type: ignore[union-attr]
            )
            return result.exit_code == 0
        except Exception:
            return False

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _build_stress_cmd(self) -> list[str]:
        params = self._config.parameters
        fault_type = self._config.type
        duration = int(self._config.duration_seconds)

        if fault_type == "cpu_stress":
            workers = params.get("workers", 1)
            return [
                "stress-ng", "--cpu", str(workers),
                "--timeout", f"{duration}s",
                "--metrics-brief",
            ]
        if fault_type == "memory_stress":
            workers = params.get("workers", 1)
            vm_bytes = params.get("vm_bytes", "256M")
            return [
                "stress-ng", "--vm", str(workers),
                "--vm-bytes", str(vm_bytes),
                "--timeout", f"{duration}s",
                "--metrics-brief",
            ]
        raise ValueError(f"Unsupported resource fault type: {fault_type!r}")

    async def _run_stress(self, cmd: list[str]) -> None:
        if not self._container:
            return
        loop = asyncio.get_event_loop()
        try:
            await loop.run_in_executor(
                None,
                lambda: self._container.exec_run(cmd, detach=False),  # type: ignore[union-attr]
            )
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            logger.error("stress-ng execution error: %s", exc)

    async def __aenter__(self) -> "ResourceFaultInjector":
        await self.inject()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.restore()
