#!/usr/bin/env python3
import argparse
import csv
import math
import os
import re
from datetime import datetime, timezone
from pathlib import Path


MATCHING_LIMITS = {
    "cadence-matching-a-0": 1.0,
    "cadence-matching-b-0": 2.0,
    "cadence-matching-c-0": 3.0,
}

POD_LABELS = {
    "cadence-matching-a-0": "Matching A",
    "cadence-matching-b-0": "Matching B",
    "cadence-matching-c-0": "Matching C",
}

RUN_LABELS = {
    "off": "Greedy baseline",
    "greedy": "Greedy baseline",
    "baseline": "Greedy baseline",
    "latency": "Latency-aware greedy",
    "greedy-latency": "Latency-aware greedy",
    "cpu_seconds": "CPU-time-aware greedy",
    "cpu-seconds": "CPU-time-aware greedy",
    "cpuseconds": "CPU-time-aware greedy",
    "greedy-cpu-seconds": "CPU-time-aware greedy",
    "greedy baseline": "Greedy baseline",
    "latency-aware greedy": "Latency-aware greedy",
    "cpu-time-aware greedy": "CPU-time-aware greedy",
}


def clean_label(value):
    key = value.strip().lower().replace("_", "-")
    if key in RUN_LABELS:
        return RUN_LABELS[key]
    cleaned = value.replace("_", " ").replace("-", " ").strip()
    return " ".join(word.capitalize() if word.islower() else word for word in cleaned.split())


def pod_label(pod):
    return POD_LABELS.get(pod, pod.replace("cadence-", "").replace("-0", "").replace("-", " ").title())


def parse_run_arg(value):
    if "=" not in value:
        raise argparse.ArgumentTypeError("run must be LABEL=PATH")
    label, path = value.split("=", 1)
    label = label.strip()
    if not label:
        raise argparse.ArgumentTypeError("run label must not be empty")
    return clean_label(label), Path(path)


def parse_timestamp(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def read_utilization(path, matching_only):
    rows = []
    skipped_negative = 0
    with path.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            pod = row["pod"]
            if matching_only and not pod.startswith("cadence-matching-"):
                continue
            cpu_cores = float(row["cpu_cores"])
            throttled_cores = float(row["throttled_cores"])
            throttled_events = float(row["throttled_events"])
            if cpu_cores < 0 or throttled_cores < 0 or throttled_events < 0:
                skipped_negative += 1
                continue
            rows.append(
                {
                    "timestamp": parse_timestamp(row["timestamp"]),
                    "pod": pod,
                    "cpu_cores": cpu_cores,
                    "throttled_cores": throttled_cores,
                    "throttled_events": throttled_events,
                    "memory_mib": float(row["memory_mib"]),
                }
            )
    if not rows:
        raise ValueError(f"{path} contains no matching utilization rows")
    if skipped_negative:
        print(f"warning: skipped {skipped_negative} negative utilization rows from {path}")

    start = min(row["timestamp"] for row in rows)
    for row in rows:
        row["seconds"] = (row["timestamp"] - start).total_seconds()
    return rows


def group_by_pod(rows):
    grouped = {}
    for row in rows:
        grouped.setdefault(row["pod"], []).append(row)
    for pod_rows in grouped.values():
        pod_rows.sort(key=lambda row: row["seconds"])
    return dict(sorted(grouped.items()))


def x_values(rows):
    return [row["seconds"] / 60.0 for row in rows]


def plot_cpu(ax, runs, show_limits):
    multi_run = len(runs) > 1
    for label, rows in runs:
        for pod, pod_rows in group_by_pod(rows).items():
            legend = f"{label} — {pod_label(pod)}" if multi_run else pod_label(pod)
            ax.plot(
                x_values(pod_rows),
                [row["cpu_cores"] for row in pod_rows],
                linewidth=1.7,
                label=legend,
            )
    if show_limits:
        for pod, limit in MATCHING_LIMITS.items():
            ax.axhline(limit, color="black", linestyle=":", linewidth=0.9, alpha=0.35)
            ax.text(0, limit, f" {pod_label(pod)} limit", va="bottom", fontsize=8, alpha=0.65)
    ax.set_title("Matching CPU utilization")
    ax.set_xlabel("Time since start (min)")
    ax.set_ylabel("CPU cores")
    ax.grid(True, alpha=0.25)
    ax.legend()


def plot_throttling(ax, runs, metric):
    multi_run = len(runs) > 1
    value_key = "throttled_cores" if metric == "cores" else "throttled_events"
    ylabel = "Throttled CPU time (cores)" if metric == "cores" else "CPU throttling events per sample"
    for label, rows in runs:
        for pod, pod_rows in group_by_pod(rows).items():
            legend = f"{label} — {pod_label(pod)}" if multi_run else pod_label(pod)
            ax.plot(
                x_values(pod_rows),
                [row[value_key] for row in pod_rows],
                linewidth=1.7,
                label=legend,
            )
    ax.set_title("Matching CPU throttling")
    ax.set_xlabel("Time since start (min)")
    ax.set_ylabel(ylabel)
    ax.grid(True, alpha=0.25)
    ax.legend()


def apply_time_axis(ax, x_min, x_max):
    if x_min is not None or x_max is not None:
        left, right = ax.get_xlim()
        left = x_min / 60.0 if x_min is not None else left
        right = x_max / 60.0 if x_max is not None else right
        ax.set_xlim(left, right)
    left, right = ax.get_xlim()
    span = max(1.0, right - left)
    step = 5 if span <= 40 else 10
    tick_start = math.ceil(left / step) * step
    tick_end = math.floor(right / step) * step
    ax.set_xticks([tick for tick in range(int(tick_start), int(tick_end) + 1, step)])


def apply_cpu_axis(ax, y_max):
    if y_max is not None and y_max > 0:
        ax.set_ylim(0, y_max)


def safe_filename(value):
    return re.sub(r"[^A-Za-z0-9_.-]+", "-", value).strip("-")


def filter_rows(rows, pod):
    return [row for row in rows if row["pod"] == pod]


def pods_in_runs(runs):
    pods = set()
    for _, rows in runs:
        for row in rows:
            pods.add(row["pod"])
    return sorted(pods)


def main():
    parser = argparse.ArgumentParser(
        description="Plot matching executor utilization from sample-utilization.sh CSV files."
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
    parser.add_argument("--prefix", default="utilization", help="Output filename prefix.")
    parser.add_argument(
        "--all-pods",
        action="store_true",
        help="Plot all sampled pods instead of only cadence-matching-* pods.",
    )
    parser.add_argument(
        "--show-cpu-limits",
        action="store_true",
        help="Draw per-matching-executor CPU-limit reference lines. Useful for unequal CPU-limit experiments.",
    )
    parser.add_argument(
        "--pod",
        action="append",
        default=[],
        help="Only plot this pod. Can be provided multiple times.",
    )
    parser.add_argument(
        "--split-by-pod",
        action="store_true",
        help="Write one CPU/throttling plot pair per pod.",
    )
    parser.add_argument(
        "--x-min",
        type=float,
        default=None,
        help="Minimum x-axis value in seconds for generated plots.",
    )
    parser.add_argument(
        "--x-max",
        type=float,
        default=None,
        help="Maximum x-axis value in seconds for generated plots, e.g. 1800 for 30-minute figures.",
    )
    parser.add_argument(
        "--cpu-y-max",
        type=float,
        default=5.0,
        help="Maximum y-axis value for CPU utilization plots. Use 0 to auto-scale.",
    )
    parser.add_argument(
        "--throttling-metric",
        choices=["cores", "events"],
        default="cores",
        help="Plot throttled CPU time as cores or throttling event counts.",
    )
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    cache_dir = args.output_dir / ".cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    os.environ.setdefault("MPLCONFIGDIR", str(cache_dir / "matplotlib"))
    os.environ.setdefault("XDG_CACHE_HOME", str(cache_dir))
    os.environ.setdefault("MPLBACKEND", "Agg")

    import matplotlib.pyplot as plt

    runs = [
        (label, read_utilization(path, matching_only=not args.all_pods))
        for label, path in args.run
    ]

    if args.pod:
        include_pod_in_label = len(args.pod) > 1
        runs = [
            (f"{label} {pod}" if include_pod_in_label else label, filter_rows(rows, pod))
            for label, rows in runs
            for pod in args.pod
        ]
        runs = [(label, rows) for label, rows in runs if rows]
        if not runs:
            raise ValueError("selected pod filter matched no rows")

    if args.split_by_pod:
        for pod in pods_in_runs(runs):
            pod_runs = []
            for label, rows in runs:
                pod_rows = filter_rows(rows, pod)
                if pod_rows:
                    pod_runs.append((label, pod_rows))
            if not pod_runs:
                continue

            pod_prefix = f"{args.prefix}-{safe_filename(pod)}"
            cpu_path = args.output_dir / f"{pod_prefix}-cpu.png"
            throttling_path = args.output_dir / f"{pod_prefix}-throttling.png"

            fig, ax = plt.subplots(figsize=(10, 5.5), constrained_layout=True)
            plot_cpu(ax, pod_runs, show_limits=False)
            ax.set_title(f"{pod_label(pod)} CPU utilization")
            apply_time_axis(ax, args.x_min, args.x_max)
            apply_cpu_axis(ax, args.cpu_y_max)
            fig.savefig(cpu_path, dpi=180)
            plt.close(fig)

            fig, ax = plt.subplots(figsize=(10, 5.5), constrained_layout=True)
            plot_throttling(ax, pod_runs, args.throttling_metric)
            ax.set_title(f"{pod_label(pod)} CPU throttling")
            apply_time_axis(ax, args.x_min, args.x_max)
            fig.savefig(throttling_path, dpi=180)
            plt.close(fig)

            print(f"wrote {cpu_path}")
            print(f"wrote {throttling_path}")
        return

    cpu_path = args.output_dir / f"{args.prefix}-matching-cpu.png"
    throttling_path = args.output_dir / f"{args.prefix}-matching-throttling.png"

    fig, ax = plt.subplots(figsize=(10, 5.5), constrained_layout=True)
    plot_cpu(ax, runs, show_limits=args.show_cpu_limits and not args.all_pods)
    apply_time_axis(ax, args.x_min, args.x_max)
    apply_cpu_axis(ax, args.cpu_y_max)
    fig.savefig(cpu_path, dpi=180)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(10, 5.5), constrained_layout=True)
    plot_throttling(ax, runs, args.throttling_metric)
    apply_time_axis(ax, args.x_min, args.x_max)
    fig.savefig(throttling_path, dpi=180)
    plt.close(fig)

    print(f"wrote {cpu_path}")
    print(f"wrote {throttling_path}")


if __name__ == "__main__":
    main()
