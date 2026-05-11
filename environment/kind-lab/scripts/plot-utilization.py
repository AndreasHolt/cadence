#!/usr/bin/env python3
import argparse
import csv
import os
import re
from datetime import datetime, timezone
from pathlib import Path


MATCHING_LIMITS = {
    "cadence-matching-a-0": 1.0,
    "cadence-matching-b-0": 2.0,
    "cadence-matching-c-0": 3.0,
}


def parse_run_arg(value):
    if "=" not in value:
        raise argparse.ArgumentTypeError("run must be LABEL=PATH")
    label, path = value.split("=", 1)
    label = label.strip()
    if not label:
        raise argparse.ArgumentTypeError("run label must not be empty")
    return label, Path(path)


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


def plot_cpu(ax, runs, show_limits):
    multi_run = len(runs) > 1
    for label, rows in runs:
        for pod, pod_rows in group_by_pod(rows).items():
            legend = f"{label} {pod}" if multi_run else pod
            ax.plot(
                [row["seconds"] for row in pod_rows],
                [row["cpu_cores"] for row in pod_rows],
                linewidth=1.7,
                label=legend,
            )
    if show_limits:
        for pod, limit in MATCHING_LIMITS.items():
            ax.axhline(limit, color="black", linestyle=":", linewidth=0.9, alpha=0.35)
            ax.text(0, limit, f" {pod} limit", va="bottom", fontsize=8, alpha=0.65)
    ax.set_title("Matching Executor CPU Utilization Over Time")
    ax.set_xlabel("Time since first sample (s)")
    ax.set_ylabel("CPU cores")
    ax.grid(True, alpha=0.25)
    ax.legend()


def plot_throttling(ax, runs):
    multi_run = len(runs) > 1
    for label, rows in runs:
        for pod, pod_rows in group_by_pod(rows).items():
            legend = f"{label} {pod}" if multi_run else pod
            ax.plot(
                [row["seconds"] for row in pod_rows],
                [row["throttled_events"] for row in pod_rows],
                linewidth=1.7,
                label=legend,
            )
    ax.set_title("Matching Executor CPU Throttling Events Over Time")
    ax.set_xlabel("Time since first sample (s)")
    ax.set_ylabel("Throttled events per sample")
    ax.grid(True, alpha=0.25)
    ax.legend()


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
        "--no-cpu-limits",
        action="store_true",
        help="Do not draw matching executor CPU-limit reference lines.",
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
            ax.set_title(f"{pod} CPU Utilization Over Time")
            fig.savefig(cpu_path, dpi=180)
            plt.close(fig)

            fig, ax = plt.subplots(figsize=(10, 5.5), constrained_layout=True)
            plot_throttling(ax, pod_runs)
            ax.set_title(f"{pod} CPU Throttling Events Over Time")
            fig.savefig(throttling_path, dpi=180)
            plt.close(fig)

            print(f"wrote {cpu_path}")
            print(f"wrote {throttling_path}")
        return

    cpu_path = args.output_dir / f"{args.prefix}-matching-cpu.png"
    throttling_path = args.output_dir / f"{args.prefix}-matching-throttling.png"

    fig, ax = plt.subplots(figsize=(10, 5.5), constrained_layout=True)
    plot_cpu(ax, runs, show_limits=not args.no_cpu_limits and not args.all_pods)
    fig.savefig(cpu_path, dpi=180)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(10, 5.5), constrained_layout=True)
    plot_throttling(ax, runs)
    fig.savefig(throttling_path, dpi=180)
    plt.close(fig)

    print(f"wrote {cpu_path}")
    print(f"wrote {throttling_path}")


if __name__ == "__main__":
    main()
