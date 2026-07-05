from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError
from rich.console import Console

from palamedes.config.schema import PalamedesConfig

_console = Console(stderr=True)


def load_config(path: str | Path) -> PalamedesConfig:
    """Load and validate a YAML experiment configuration file (RF01)."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(
            f"Config file must be a YAML mapping, got {type(raw).__name__}"
        )

    try:
        config = PalamedesConfig.model_validate(raw)
    except ValidationError as exc:
        _console.print(f"[bold red]Config validation failed:[/bold red] {path}")
        for err in exc.errors():
            loc = " > ".join(str(s) for s in err["loc"])
            _console.print(f"  [yellow]{loc}[/yellow]: {err['msg']}")
        raise

    return config


def apply_parameter_override(
    config: PalamedesConfig, dotted_path: str, value: Any
) -> PalamedesConfig:
    """
    Return a new PalamedesConfig with one parameter overridden via dotted path.

    The path is relative to the ``experiment`` root.
    Examples:
        ``load.config.vus`` → sets experiment.load.config.vus
        ``id``              → sets experiment.id
    """
    raw = config.model_dump()
    _set_nested(raw["experiment"], dotted_path.split("."), value)
    return PalamedesConfig.model_validate(raw)


def export_json_schema(output_path: str | Path) -> None:
    """Write the Pydantic JSON Schema for PalamedesConfig to a file (RF01)."""
    schema = PalamedesConfig.model_json_schema()
    Path(output_path).write_text(
        json.dumps(schema, indent=2), encoding="utf-8"
    )


def _set_nested(obj: dict, keys: list[str], value: Any) -> None:
    for key in keys[:-1]:
        if key not in obj or not isinstance(obj[key], dict):
            obj[key] = {}
        obj = obj[key]
    obj[keys[-1]] = value
