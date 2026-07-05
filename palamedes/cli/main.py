from __future__ import annotations

import asyncio
import logging
import sys
from pathlib import Path

import typer
from rich.console import Console
from rich.logging import RichHandler
from rich.panel import Panel
from rich.table import Table

from palamedes.config.loader import load_config
from palamedes.core.batch_runner import BatchRunner
from palamedes.core.orchestrator import Orchestrator

app = typer.Typer(
    name="palamedes",
    help="Empirical Performability Framework — automated lifecycle for performance + dependability experiments.",
    add_completion=False,
    no_args_is_help=True,
)

console = Console()
err_console = Console(stderr=True)


def _setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(rich_tracebacks=True, markup=True)],
        force=True,
    )


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------


@app.command()
def run(
    config: Path = typer.Argument(..., help="Path to experiment YAML config file."),
    results_dir: str = typer.Option(
        "results", "--results-dir", "-o", help="Output directory for results."
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable debug logging."),
    export: bool = typer.Option(True, help="Export CSV, JSON and plots after the run."),
) -> None:
    """Run a single experiment defined by CONFIG."""
    _setup_logging(verbose)
    _check_config(config)

    cfg = load_config(config)
    console.print(
        Panel(
            f"[bold cyan]{cfg.experiment.id}[/bold cyan]\n"
            + (cfg.experiment.description or ""),
            title="[bold]Palamedes[/bold]",
        )
    )

    result = asyncio.run(Orchestrator(cfg, results_dir=results_dir).run())

    if not result.success:
        err_console.print(f"[red]✗ Experiment failed:[/red] {result.error}")
        raise typer.Exit(code=1)

    # Compute dependability metrics
    from palamedes.analytics.metrics import compute_dependability_metrics

    dm = compute_dependability_metrics(
        result,
        sla_max_error_rate_pct=cfg.experiment.sla.max_error_rate_percent,
        sla_max_p99_ms=cfg.experiment.sla.max_p99_latency_ms,
    )
    result.dependability = dm
    _print_dependability_table(result)

    if export:
        _export_artifacts(result, cfg)

    console.print("\n[bold green]✓ Experiment completed.[/bold green]")


@app.command()
def batch(
    config: Path = typer.Argument(
        ..., help="Path to YAML config file with a [batch] section."
    ),
    results_dir: str = typer.Option("results", "--results-dir", "-o"),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """Run a parameter sweep defined by the [batch] section of CONFIG."""
    _setup_logging(verbose)
    _check_config(config)

    cfg = load_config(config)
    if cfg.batch is None:
        err_console.print(
            "[red]Config has no [batch] section.[/red] Use 'run' for single experiments."
        )
        raise typer.Exit(code=1)

    sweep = cfg.batch.parameter_sweep
    console.print(
        Panel(
            f"Parameter: [cyan]{sweep.parameter}[/cyan]\n"
            f"Values:    {sweep.values}\n"
            f"Repeat:    {cfg.batch.repeat}",
            title="[bold]Batch Run[/bold]",
        )
    )

    results = asyncio.run(BatchRunner(cfg, results_dir=results_dir).run())
    _print_batch_summary(results)
    console.print(f"\n[bold green]✓ Batch complete.[/bold green]  ({len(results)} runs)")


@app.command()
def validate(
    config: Path = typer.Argument(..., help="Path to experiment YAML config file."),
) -> None:
    """Validate a YAML config file against the Palamedes schema."""
    _check_config(config)
    try:
        cfg = load_config(config)
        console.print(f"[green]✓[/green] Valid — experiment id: [bold]{cfg.experiment.id}[/bold]")
    except Exception as exc:
        err_console.print(f"[red]✗ Validation failed:[/red] {exc}")
        raise typer.Exit(code=1)


@app.command()
def report(
    db: Path = typer.Argument(
        ..., help="Path to a metrics.duckdb file from a previous run."
    ),
    output_dir: str = typer.Option(".", "--output-dir", "-o"),
) -> None:
    """Re-generate plots and exports from an existing metrics DuckDB file."""
    if not db.exists():
        err_console.print(f"[red]DB not found:[/red] {db}")
        raise typer.Exit(code=1)

    from palamedes.analytics.plotter import plot_performance_timeline

    exp_id = db.parent.name
    plots = plot_performance_timeline(str(db), exp_id, output_dir)
    for kind, path in plots.items():
        console.print(f"  [green]↳ {kind.upper()}[/green]: {path}")
    console.print("[green]✓ Report generated.[/green]")


@app.command("export-schema")
def export_schema(
    output: Path = typer.Argument(
        Path("docs/schema.json"), help="Output path for the JSON Schema file."
    ),
) -> None:
    """Export the Palamedes YAML config JSON Schema to a file."""
    from palamedes.config.loader import export_json_schema

    output.parent.mkdir(parents=True, exist_ok=True)
    export_json_schema(output)
    console.print(f"[green]✓[/green] Schema written to [bold]{output}[/bold]")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _check_config(path: Path) -> None:
    if not path.exists():
        err_console.print(f"[red]Config not found:[/red] {path}")
        raise typer.Exit(code=1)


def _export_artifacts(result: "ExperimentResult", cfg: "PalamedesConfig") -> None:  # type: ignore[name-defined]
    from palamedes.analytics.exporter import export_csv, export_json
    from palamedes.analytics.plotter import plot_performance_timeline
    from palamedes.models.events import EventType

    out = Path("results") / result.experiment_id
    export_csv(result, str(out))
    export_json(result, str(out))

    fault_ts = result.timeline.get_ts(EventType.FAULT_INJECTED) if result.timeline else None
    recovery_ts = (
        result.timeline.get_ts(EventType.RECOVERY_COMPLETE) if result.timeline else None
    )
    if result.db_path:
        plots = plot_performance_timeline(
            result.db_path,
            result.experiment_id,
            str(out),
            fault_ts_ms=fault_ts,
            recovery_ts_ms=recovery_ts,
        )
        for kind, path in plots.items():
            console.print(f"  [green]↳ {kind.upper()}[/green]: {path}")


def _print_dependability_table(result: "ExperimentResult") -> None:  # type: ignore[name-defined]
    table = Table(title="Dependability Metrics", show_lines=True)
    table.add_column("Metric", style="cyan", no_wrap=True)
    table.add_column("Value", justify="right")

    d = result.dependability
    if d is None:
        return

    rows = [
        ("MTRS", f"{d.mtrs_ms / 1000:.2f} s" if d.mtrs_ms is not None else "—"),
        (
            "Unavailability Window",
            f"{d.unavailability_window_ms / 1000:.2f} s"
            if d.unavailability_window_ms is not None
            else "—",
        ),
        (
            "Performance Attenuation",
            f"{d.performance_attenuation_pct:.1f} %"
            if d.performance_attenuation_pct is not None
            else "—",
        ),
        (
            "Baseline Throughput",
            f"{d.baseline_throughput_rps:.1f} rps"
            if d.baseline_throughput_rps is not None
            else "—",
        ),
        (
            "Fault Min Throughput",
            f"{d.fault_min_throughput_rps:.1f} rps"
            if d.fault_min_throughput_rps is not None
            else "—",
        ),
    ]
    for label, value in rows:
        table.add_row(label, value)

    console.print(table)


def _print_batch_summary(results: list) -> None:
    table = Table(title="Batch Summary", show_lines=True)
    table.add_column("ID", style="cyan")
    table.add_column("Status")
    table.add_column("MTRS (s)", justify="right")
    table.add_column("Attenuation (%)", justify="right")

    for r in results:
        status = "[green]OK[/green]" if r.success else "[red]FAIL[/red]"
        mtrs = (
            f"{r.dependability.mtrs_ms / 1000:.2f}"
            if r.dependability and r.dependability.mtrs_ms is not None
            else "—"
        )
        atten = (
            f"{r.dependability.performance_attenuation_pct:.1f}"
            if r.dependability and r.dependability.performance_attenuation_pct is not None
            else "—"
        )
        table.add_row(r.experiment_id, status, mtrs, atten)

    console.print(table)
