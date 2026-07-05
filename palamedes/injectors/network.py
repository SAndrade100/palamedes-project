from __future__ import annotations

import asyncio
import logging
from typing import Optional

import docker
import docker.errors
from docker.models.containers import Container

from palamedes.config.schema import FaultConfig

logger = logging.getLogger(__name__)


class NetworkFaultInjector:
    """
    Injects network faults via Linux tc-netem inside the target container (RF07):
    - network_latency: artificial RTT delay with optional jitter
    - network_loss:    random packet loss
    - network_partition: 100% packet loss (simulates split-brain)

    Requires iproute2 (tc) inside the container and NET_ADMIN capability.
    """

    def __init__(self, config: FaultConfig) -> None:
        self._config = config
        self._client = docker.from_env()
        self._container: Optional[Container] = None
        self._interface = config.parameters.get("interface", "eth0")
        self._injected = False

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def inject(self) -> None:
        name = self._config.target_container
        try:
            self._container = self._client.containers.get(name)
        except docker.errors.NotFound:
            raise RuntimeError(f"Container not found: {name!r}")

        cmd = self._build_tc_add_cmd()
        logger.info("Network fault on %r: %s", name, " ".join(cmd))
        await self._exec(self._container, cmd)
        self._injected = True

    async def restore(self) -> None:
        if not self._container or not self._injected:
            return
        name = self._config.target_container
        try:
            cmd = self._build_tc_del_cmd()
            logger.info("Removing network fault on %r", name)
            await self._exec(self._container, cmd)
            self._injected = False
        except Exception as exc:
            logger.warning("Failed to restore network on %r: %s", name, exc)

    async def verify_injected(self) -> bool:
        if not self._container:
            return False
        try:
            out = await self._exec(
                self._container,
                ["tc", "qdisc", "show", "dev", self._interface],
            )
            return "netem" in out
        except Exception:
            return False

    # ------------------------------------------------------------------
    # Command builders
    # ------------------------------------------------------------------

    def _build_tc_add_cmd(self) -> list[str]:
        fault_type = self._config.type
        params = self._config.parameters
        iface = self._interface
        base = ["tc", "qdisc", "add", "dev", iface, "root", "netem"]

        if fault_type == "network_latency":
            delay_ms = params.get("latency_ms", 100)
            jitter_ms = params.get("jitter_ms", 0)
            if jitter_ms:
                return base + [
                    "delay", f"{delay_ms}ms", f"{jitter_ms}ms",
                    "distribution", "normal",
                ]
            return base + ["delay", f"{delay_ms}ms"]

        if fault_type == "network_loss":
            loss_pct = params.get("loss_percent", 10)
            return base + ["loss", f"{loss_pct}%"]

        # network_partition
        return base + ["loss", "100%"]

    def _build_tc_del_cmd(self) -> list[str]:
        return ["tc", "qdisc", "del", "dev", self._interface, "root"]

    # ------------------------------------------------------------------
    # Docker exec helper (runs in thread pool to avoid blocking the loop)
    # ------------------------------------------------------------------

    @staticmethod
    async def _exec(container: Container, cmd: list[str]) -> str:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None, lambda: container.exec_run(cmd, privileged=True)
        )
        output = result.output.decode(errors="replace").strip() if result.output else ""
        if result.exit_code not in (0, None):
            raise RuntimeError(
                f"Command {cmd!r} exited {result.exit_code}: {output}"
            )
        return output

    async def __aenter__(self) -> "NetworkFaultInjector":
        await self.inject()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.restore()
