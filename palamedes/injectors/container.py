from __future__ import annotations

import logging
from typing import Literal, Optional

import docker
import docker.errors
from docker.models.containers import Container

from palamedes.config.schema import FaultConfig

logger = logging.getLogger(__name__)

ContainerAction = Literal["stop", "pause", "kill"]

_ACTION_MAP: dict[str, ContainerAction] = {
    "container_stop": "stop",
    "container_pause": "pause",
    "container_kill": "kill",
}


class ContainerFaultInjector:
    """
    Injects container-level faults via the Docker SDK (RF07):
    stop, pause, or kill.  ``restore()`` restarts or unpauses the container.
    """

    def __init__(self, config: FaultConfig) -> None:
        self._config = config
        self._action: ContainerAction = _ACTION_MAP.get(config.type, "stop")  # type: ignore[assignment]
        self._client = docker.from_env()
        self._container: Optional[Container] = None
        self._was_running = False
        self._restored = False

    async def inject(self) -> None:
        name = self._config.target_container
        try:
            self._container = self._client.containers.get(name)
            self._was_running = self._container.status == "running"
            self._restored = False
            logger.info("Injecting %s on container %r", self._action, name)
            if self._action == "stop":
                timeout = int(self._config.parameters.get("timeout", 10))
                self._container.stop(timeout=timeout)
            elif self._action == "pause":
                self._container.pause()
            elif self._action == "kill":
                signal = self._config.parameters.get("signal", "SIGKILL")
                self._container.kill(signal=signal)
        except docker.errors.NotFound:
            raise RuntimeError(f"Container not found: {name!r}")
        except docker.errors.APIError as exc:
            raise RuntimeError(f"Docker API error (inject): {exc}") from exc

    async def restore(self) -> None:
        if self._container is None or self._restored:
            return
        name = self._config.target_container
        try:
            self._container.reload()
            status = self._container.status
            if self._action == "pause" and status == "paused":
                logger.info("Unpausing container %r", name)
                self._container.unpause()
            elif status in ("stopped", "exited", "dead") and self._was_running:
                logger.info("Starting container %r", name)
                self._container.start()
            self._restored = True
        except docker.errors.APIError as exc:
            logger.error("Docker API error (restore %r): %s", name, exc)

    async def verify_injected(self) -> bool:
        if self._container is None:
            return False
        try:
            self._container.reload()
            if self._action == "pause":
                return self._container.status == "paused"
            return self._container.status in ("stopped", "exited", "dead")
        except docker.errors.APIError:
            return False

    async def __aenter__(self) -> "ContainerFaultInjector":
        await self.inject()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.restore()
