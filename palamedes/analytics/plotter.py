from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import duckdb
import plotly.graph_objects as go
from plotly.subplots import make_subplots

logger = logging.getLogger(__name__)


def plot_performance_timeline(
    db_path: str,
    experiment_id: str,
    output_dir: str,
    fault_ts_ms: Optional[int] = None,
    recovery_ts_ms: Optional[int] = None,
) -> dict[str, str]:
    """
    Generate performance-over-time plots with fault and recovery markers (RF13).

    Produces:
    - ``<id>_timeline.html``  — Plotly interactive report
    - ``<id>_timeline.pdf``   — Matplotlib vector PDF (Agg backend)
    - ``<id>_timeline.pgf``   — PGF/TikZ source file for LaTeX inclusion

    Returns a dict mapping format → file path for each successfully written file.
    """
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    outputs: dict[str, str] = {}

    conn = duckdb.connect(db_path, read_only=True)
    try:
        rows = conn.execute(
            """
            SELECT ts_ms, phase,
                   throughput_rps, p95_latency_ms, p99_latency_ms,
                   error_rate_percent, cpu_percent, memory_percent
            FROM metrics
            ORDER BY ts_ms
            """
        ).fetchall()
        cols = [
            "ts_ms", "phase", "throughput_rps", "p95_latency_ms",
            "p99_latency_ms", "error_rate_percent", "cpu_percent", "memory_percent",
        ]
    finally:
        conn.close()

    if not rows:
        logger.warning("No metrics data in %s", db_path)
        return outputs

    data = {col: [r[i] for r in rows] for i, col in enumerate(cols)}
    t0 = min(data["ts_ms"])
    t_s = [(ts - t0) / 1000.0 for ts in data["ts_ms"]]

    fault_s = (fault_ts_ms - t0) / 1000.0 if fault_ts_ms is not None else None
    recovery_s = (recovery_ts_ms - t0) / 1000.0 if recovery_ts_ms is not None else None

    # ── Plotly interactive HTML ────────────────────────────────────────────
    html_path = _plot_plotly(
        data, t_s, experiment_id, out, fault_s, recovery_s
    )
    if html_path:
        outputs["html"] = html_path

    # ── Matplotlib PDF + PGF ──────────────────────────────────────────────
    pdf_path, pgf_path = _plot_matplotlib(
        data, t_s, experiment_id, out, fault_s, recovery_s
    )
    if pdf_path:
        outputs["pdf"] = pdf_path
    if pgf_path:
        outputs["pgf"] = pgf_path

    return outputs


# ---------------------------------------------------------------------------
# Plotly
# ---------------------------------------------------------------------------


def _plot_plotly(
    data: dict,
    t_s: list[float],
    experiment_id: str,
    out: Path,
    fault_s: Optional[float],
    recovery_s: Optional[float],
) -> Optional[str]:
    try:
        fig = make_subplots(
            rows=3,
            cols=1,
            shared_xaxes=True,
            subplot_titles=["Throughput (req/s)", "Latency (ms)", "Error Rate (%)"],
            vertical_spacing=0.08,
        )
        fig.add_trace(
            go.Scatter(
                x=t_s, y=data["throughput_rps"],
                name="Throughput", line={"color": "#2196F3", "width": 1.2},
            ),
            row=1, col=1,
        )
        fig.add_trace(
            go.Scatter(
                x=t_s, y=data["p95_latency_ms"],
                name="P95 Latency", line={"color": "#FF9800", "width": 1.2},
            ),
            row=2, col=1,
        )
        fig.add_trace(
            go.Scatter(
                x=t_s, y=data["p99_latency_ms"],
                name="P99 Latency", line={"color": "#F44336", "width": 1.2, "dash": "dot"},
            ),
            row=2, col=1,
        )
        fig.add_trace(
            go.Scatter(
                x=t_s, y=data["error_rate_percent"],
                name="Error Rate", line={"color": "#9C27B0", "width": 1.2},
            ),
            row=3, col=1,
        )

        marker_kwargs = {"line_width": 1.5}
        for row in range(1, 4):
            if fault_s is not None:
                fig.add_vline(
                    x=fault_s,
                    line={"color": "red", "dash": "dash", **marker_kwargs},
                    annotation_text="Fault" if row == 1 else None,
                    row=row, col=1,
                )
            if recovery_s is not None:
                fig.add_vline(
                    x=recovery_s,
                    line={"color": "green", "dash": "dash", **marker_kwargs},
                    annotation_text="Recovery" if row == 1 else None,
                    row=row, col=1,
                )

        fig.update_layout(
            title=f"Experiment: {experiment_id}",
            xaxis3_title="Time (s)",
            height=720,
            template="plotly_white",
        )
        path = str(out / f"{experiment_id}_timeline.html")
        fig.write_html(path)
        logger.info("Plotly HTML → %s", path)
        return path
    except Exception:
        logger.exception("Plotly plot failed")
        return None


# ---------------------------------------------------------------------------
# Matplotlib
# ---------------------------------------------------------------------------


def _plot_matplotlib(
    data: dict,
    t_s: list[float],
    experiment_id: str,
    out: Path,
    fault_s: Optional[float],
    recovery_s: Optional[float],
) -> tuple[Optional[str], Optional[str]]:
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        plt.rcParams.update(
            {
                "font.family": "serif",
                "axes.labelsize": 10,
                "font.size": 10,
                "legend.fontsize": 9,
                "xtick.labelsize": 9,
                "ytick.labelsize": 9,
                "figure.figsize": (6.5, 7.0),
                "figure.dpi": 150,
            }
        )

        fig, axes = plt.subplots(3, 1, sharex=True)

        axes[0].plot(
            t_s, data["throughput_rps"],
            color="#2196F3", linewidth=0.9, label="Throughput",
        )
        axes[0].set_ylabel("Req/s")
        axes[0].legend(loc="upper right")

        axes[1].plot(
            t_s, data["p95_latency_ms"],
            color="#FF9800", linewidth=0.9, label="P95",
        )
        axes[1].plot(
            t_s, data["p99_latency_ms"],
            color="#F44336", linewidth=0.9, linestyle="--", label="P99",
        )
        axes[1].set_ylabel("Latency (ms)")
        axes[1].legend(loc="upper right")

        axes[2].plot(
            t_s, data["error_rate_percent"],
            color="#9C27B0", linewidth=0.9, label="Error Rate",
        )
        axes[2].set_ylabel("Error Rate (%)")
        axes[2].set_xlabel("Time (s)")
        axes[2].legend(loc="upper right")

        for ax in axes:
            if fault_s is not None:
                ax.axvline(
                    x=fault_s, color="red", linestyle="--",
                    linewidth=0.8, label="Fault injected",
                )
            if recovery_s is not None:
                ax.axvline(
                    x=recovery_s, color="green", linestyle="--",
                    linewidth=0.8, label="Recovery",
                )
            ax.grid(True, linestyle=":", linewidth=0.4, alpha=0.6)

        fig.suptitle(f"Experiment: {experiment_id}", fontsize=11)
        fig.tight_layout()

        pdf_path = str(out / f"{experiment_id}_timeline.pdf")
        fig.savefig(pdf_path, format="pdf")
        logger.info("Matplotlib PDF → %s", pdf_path)

        pgf_path = str(out / f"{experiment_id}_timeline.pgf")
        fig.savefig(pgf_path, format="pgf")
        logger.info("PGF/TikZ → %s", pgf_path)

        plt.close(fig)
        return pdf_path, pgf_path

    except Exception:
        logger.exception("Matplotlib plot failed")
        return None, None
