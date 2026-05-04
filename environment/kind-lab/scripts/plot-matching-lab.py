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


def plot_completed_rps(ax, runs, show_started):
    if show_started and runs:
        first_label, first_points = runs[0]
        ax.plot(
            series(first_points, "at_seconds"),
            series(first_points, "window_started_rps"),
            color="black",
            linestyle="--",
            linewidth=1.6,
            label=f"{first_label} started/sec",
        )

    for label, points in runs:
        ax.plot(
            series(points, "at_seconds"),
            series(points, "window_completed_rps"),
            linewidth=1.8,
            label=f"{label} completed/sec",
        )

    ax.set_title("Cluster Completed Throughput Over Time")
    ax.set_xlabel("Time since start (s)")
    ax.set_ylabel("Completed workflows/sec")
    ax.grid(True, alpha=0.25)
    ax.legend()


def plot_p95_latency(ax, runs):
    for label, points in runs:
        ax.plot(
            series(points, "at_seconds"),
            series(points, "window_latency_p95_ms"),
            linewidth=1.8,
            label=label,
        )

    ax.set_title("Workflow Completion p95 Latency Over Time")
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
    plot_completed_rps(ax, runs, not args.no_started_line)
    fig.savefig(throughput_path, dpi=180)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(10, 5.5), constrained_layout=True)
    plot_p95_latency(ax, runs)
    fig.savefig(latency_path, dpi=180)
    plt.close(fig)

    print(f"wrote {throughput_path}")
    print(f"wrote {latency_path}")


if __name__ == "__main__":
    main()
