#!/usr/bin/env python3
import argparse
import json
import os
from pathlib import Path


def parse_run_arg(value):
    if "=" not in value:
        raise argparse.ArgumentTypeError("run must be LABEL=PATH")
    label, path = value.split("=", 1)
    label = label.strip()
    if not label:
        raise argparse.ArgumentTypeError("run label must not be empty")
    return label, Path(path)


def read_summary_json(path):
    points = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line.startswith("summary_json:"):
                continue
            raw = line.removeprefix("summary_json:").strip()
            points.append(json.loads(raw))
    if not points:
        raise ValueError(f"{path} contains no summary_json lines")
    return points


def series(points, key):
    return [point.get(key, 0.0) for point in points]


def first_error_time(points):
    for point in points:
        if (
            point.get("window_start_errors", 0) > 0
            or point.get("window_poll_errors", 0) > 0
            or point.get("window_completion_errors", 0) > 0
        ):
            return point.get("at_seconds")
    return None


def plot_completed_rps(ax, runs, show_started, mark_errors, title):
    for label, points in runs:
        completed_line = ax.plot(
            series(points, "at_seconds"),
            series(points, "window_completed_rps"),
            linewidth=1.8,
            label=f"{label} completed/sec",
        )[0]
        if show_started:
            ax.plot(
                series(points, "at_seconds"),
                series(points, "window_started_rps"),
                color=completed_line.get_color(),
                linestyle="--",
                linewidth=1.4,
                alpha=0.8,
                label=f"{label} started/sec",
            )
        if mark_errors:
            error_at = first_error_time(points)
            if error_at is not None:
                ax.axvline(error_at, color="tab:red", linestyle=":", linewidth=1.2, alpha=0.7)

    ax.set_title(title or "Cluster Completed Throughput Over Time")
    ax.set_xlabel("Time since start (s)")
    ax.set_ylabel("Completed workflows/sec")
    ax.grid(True, alpha=0.25)
    ax.legend()


def plot_p95_latency(ax, runs, mark_errors, title):
    for label, points in runs:
        ax.plot(
            series(points, "at_seconds"),
            series(points, "window_latency_p95_ms"),
            linewidth=1.8,
            label=label,
        )
        if mark_errors:
            error_at = first_error_time(points)
            if error_at is not None:
                ax.axvline(error_at, color="tab:red", linestyle=":", linewidth=1.2, alpha=0.7)

    ax.set_title(title or "Workflow Completion p95 Latency Over Time")
    ax.set_xlabel("Time since start (s)")
    ax.set_ylabel("p95 latency (ms)")
    ax.grid(True, alpha=0.25)
    ax.legend()


def main():
    parser = argparse.ArgumentParser(
        description="Plot matching-lab throughput and p95 latency from summary_json logs."
    )
    parser.add_argument(
        "--run",
        action="append",
        type=parse_run_arg,
        required=True,
        help="Run to plot as LABEL=PATH. Can be provided multiple times.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("environment/kind-lab/results/plots"),
        help="Directory for generated PNG files.",
    )
    parser.add_argument(
        "--prefix",
        default="matching-lab",
        help="Output filename prefix.",
    )
    parser.add_argument(
        "--no-started-line",
        action="store_true",
        help="Do not draw the first run's started/sec line on the throughput plot.",
    )
    parser.add_argument(
        "--mark-errors",
        action="store_true",
        help="Draw a vertical line when a run first reports start, poll, or completion errors.",
    )
    parser.add_argument(
        "--throughput-title",
        default="",
        help="Override the throughput plot title.",
    )
    parser.add_argument(
        "--latency-title",
        default="",
        help="Override the p95 latency plot title.",
    )
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    cache_dir = args.output_dir / ".cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    os.environ.setdefault("MPLCONFIGDIR", str(cache_dir / "matplotlib"))
    os.environ.setdefault("XDG_CACHE_HOME", str(cache_dir))
    os.environ.setdefault("MPLBACKEND", "Agg")

    import matplotlib.pyplot as plt

    runs = [(label, read_summary_json(path)) for label, path in args.run]

    throughput_path = args.output_dir / f"{args.prefix}-throughput.png"
    latency_path = args.output_dir / f"{args.prefix}-p95-latency.png"

    fig, ax = plt.subplots(figsize=(10, 5.5), constrained_layout=True)
    plot_completed_rps(
        ax,
        runs,
        not args.no_started_line,
        args.mark_errors,
        args.throughput_title,
    )
    fig.savefig(throughput_path, dpi=180)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(10, 5.5), constrained_layout=True)
    plot_p95_latency(ax, runs, args.mark_errors, args.latency_title)
    fig.savefig(latency_path, dpi=180)
    plt.close(fig)

    print(f"wrote {throughput_path}")
    print(f"wrote {latency_path}")


if __name__ == "__main__":
    main()
