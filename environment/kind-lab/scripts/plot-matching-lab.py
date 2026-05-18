#!/usr/bin/env python3
import argparse
import csv
import json
import math
import os
import sys
from pathlib import Path
from datetime import datetime, timezone


RUN_LABELS = {
    "off": "Greedy baseline",
    "greedy": "Greedy baseline",
    "baseline": "Greedy baseline",
    "latency": "Latency aware greedy",
    "greedy-latency": "Latency aware greedy",
    "cpu-seconds": "CPU utilization aware greedy",
    "cpu_seconds": "CPU utilization aware greedy",
    "cpuseconds": "CPU utilization aware greedy",
    "greedy-cpu-seconds": "CPU utilization aware greedy",
    "greedy baseline": "Greedy baseline",
    "latency-aware greedy": "Latency aware greedy",
    "latency aware greedy": "Latency aware greedy",
    "cpu-time-aware greedy": "CPU utilization aware greedy",
    "cpu utilization aware greedy": "CPU utilization aware greedy",
}


def clean_label(value):
    key = value.strip().lower().replace("_", "-")
    if key in RUN_LABELS:
        return RUN_LABELS[key]
    cleaned = value.replace("_", " ").replace("-", " ").strip()
    return " ".join(word.capitalize() if word.islower() else word for word in cleaned.split())


def parse_run_arg(value):
    if "=" not in value:
        raise argparse.ArgumentTypeError("run must be LABEL=PATH")
    label, path = value.split("=", 1)
    label = label.strip()
    if not label:
        raise argparse.ArgumentTypeError("run label must not be empty")
    return clean_label(label), Path(path)


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


def parse_timestamp(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def read_prometheus_series_csv(path):
    values_by_timestamp = {}
    with path.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        required = {"timestamp", "value"}
        if not required.issubset(reader.fieldnames or []):
            raise ValueError(f"{path} must contain timestamp and value columns")
        for row in reader:
            timestamp = parse_timestamp(row["timestamp"])
            values_by_timestamp[timestamp] = values_by_timestamp.get(timestamp, 0.0) + float(row["value"])

    if not values_by_timestamp:
        raise ValueError(f"{path} contains no Prometheus series rows")

    start = min(values_by_timestamp)
    points = [
        {
            "timestamp": timestamp,
            "seconds": (timestamp - start).total_seconds(),
            "value": value,
        }
        for timestamp, value in sorted(values_by_timestamp.items())
    ]
    return points


def series(points, key):
    return [point.get(key, 0.0) for point in points]


def completed_total(points):
    return points[-1].get("completed", 0) if points else 0


def first_error_time(points):
    for point in points:
        if (
            point.get("window_start_errors", 0) > 0
            or point.get("window_poll_errors", 0) > 0
            or point.get("window_completion_errors", 0) > 0
        ):
            return point.get("at_seconds")
    return None


def cumulative_counter_points(points):
    if not points:
        return []
    first = points[0]["value"]
    return [
        {
            "seconds": point["seconds"],
            "value": max(0.0, point["value"] - first),
        }
        for point in points
    ]


def rate_from_counter_points(points):
    rates = []
    previous = None
    for point in points:
        if previous is None:
            previous = point
            continue
        elapsed = point["seconds"] - previous["seconds"]
        if elapsed <= 0:
            previous = point
            continue
        delta = point["value"] - previous["value"]
        if delta < 0:
            delta = 0
        rates.append(
            {
                "seconds": point["seconds"],
                "value": delta / elapsed,
            }
        )
        previous = point
    return rates


def cumulative_from_rate_points(points):
    cumulative = []
    total = 0.0
    previous = None
    for point in points:
        if previous is not None:
            elapsed = point["seconds"] - previous["seconds"]
            if elapsed > 0:
                total += point["value"] * elapsed
        cumulative.append({"seconds": point["seconds"], "value": total})
        previous = point
    return cumulative


def infer_churn_kind(path, explicit):
    if explicit != "auto":
        return explicit
    name = path.name.lower()
    if "rate" in name or "per_sec" in name or "per-second" in name:
        return "rate"
    return "counter"


def candidate_churn_csv_paths(prometheus_dir, label):
    candidates = [label]
    candidates.extend(alias for alias, clean in RUN_LABELS.items() if clean == label)
    candidates.extend(safe_filename(candidate) for candidate in list(candidates))
    seen = set()
    for candidate in candidates:
        if candidate in seen:
            continue
        seen.add(candidate)
        yield prometheus_dir / candidate / "csv" / "sd_load_based_moves_total.csv"


def resolve_churn_run_args(run_args, explicit_churn_args, prometheus_dir, no_auto_churn):
    if explicit_churn_args or no_auto_churn:
        return explicit_churn_args

    churn_args = []
    for label, _ in run_args:
        for candidate in candidate_churn_csv_paths(prometheus_dir, label):
            if candidate.exists():
                churn_args.append((label, candidate))
                break

    if churn_args:
        found = ", ".join(f"{label}={path}" for label, path in churn_args)
        print(f"auto-detected churn CSV: {found}", file=sys.stderr)
    return churn_args


def x_series(points):
    return [point / 60.0 for point in series(points, "at_seconds")]


def rolling_average(values, window):
    if window <= 1:
        return values
    averaged = []
    running_sum = 0.0
    queue = []
    for value in values:
        queue.append(value)
        running_sum += value
        if len(queue) > window:
            running_sum -= queue.pop(0)
        averaged.append(running_sum / len(queue))
    return averaged


def plot_completed_rps(ax, runs, show_started, mark_errors, title, smooth_window):
    for label, points in runs:
        values = series(points, "window_completed_rps")
        ax.plot(
            x_series(points),
            rolling_average(values, smooth_window),
            linewidth=1.8,
            label=label,
            zorder=2,
        )
        if mark_errors:
            error_at = first_error_time(points)
            if error_at is not None:
                ax.axvline(error_at / 60.0, color="tab:red", linestyle=":", linewidth=1.2, alpha=0.7)

    if show_started and runs:
        _, first_points = runs[0]
        values = series(first_points, "window_started_rps")
        ax.plot(
            x_series(first_points),
            rolling_average(values, smooth_window),
            color="black",
            linestyle=(0, (6, 4)),
            linewidth=1.7,
            alpha=0.9,
            label="Offered starts",
            zorder=4,
        )

    title_suffix = "" if smooth_window <= 1 else f" ({smooth_window * 10}s rolling mean)"
    ax.set_title(title or f"Completed workflow throughput{title_suffix}")
    ax.set_xlabel("Time since start (min)")
    ax.set_ylabel("Completed workflows/s")
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


def plot_completed_cumulative(ax, runs, title):
    for label, points in runs:
        total = completed_total(points)
        ax.plot(
            x_series(points),
            series(points, "completed"),
            linewidth=1.8,
            label=f"{label} total={total:,}",
        )

    ax.set_title(title or "Cumulative completed workflows")
    ax.set_xlabel("Time since start (min)")
    ax.set_ylabel("Completed workflows")
    ax.grid(True, alpha=0.25)
    ax.legend()


def plot_p95_latency(ax, runs, mark_errors, title, y_max_seconds):
    for label, points in runs:
        ax.plot(
            x_series(points),
            [value / 1000.0 for value in series(points, "window_latency_p95_ms")],
            linewidth=1.8,
            label=label,
        )
        if mark_errors:
            error_at = first_error_time(points)
            if error_at is not None:
                ax.axvline(error_at / 60.0, color="tab:red", linestyle=":", linewidth=1.2, alpha=0.7)

    ax.set_title(title or "Workflow completion latency (p95)")
    ax.set_xlabel("Time since start (min)")
    ax.set_ylabel("p95 latency (s)")
    if y_max_seconds is not None and y_max_seconds > 0:
        ax.set_ylim(0, y_max_seconds)
    ax.grid(True, alpha=0.25)
    ax.legend()


def plot_churn_rate(ax, churn_runs, title):
    for label, points, kind in churn_runs:
        rate_points = points if kind == "rate" else rate_from_counter_points(points)
        ax.plot(
            [point["seconds"] / 60.0 for point in rate_points],
            [point["value"] for point in rate_points],
            linewidth=1.8,
            label=label,
        )

    ax.set_title(title or "Shard movement rate")
    ax.set_xlabel("Time since start (min)")
    ax.set_ylabel("Shard moves/s")
    ax.grid(True, alpha=0.25)
    ax.legend()


def plot_churn_total(ax, churn_runs, title):
    for label, points, kind in churn_runs:
        cumulative = cumulative_from_rate_points(points) if kind == "rate" else cumulative_counter_points(points)
        total = cumulative[-1]["value"] if cumulative else 0.0
        total_label = f"{label} total~{total:,.0f}" if kind == "rate" else f"{label} total={total:,.0f}"
        ax.plot(
            [point["seconds"] / 60.0 for point in cumulative],
            [point["value"] for point in cumulative],
            linewidth=1.8,
            label=total_label,
        )

    ax.set_title(title or "Cumulative shard moves")
    ax.set_xlabel("Time since start (min)")
    ax.set_ylabel("Shard moves")
    ax.grid(True, alpha=0.25)
    ax.legend()


def write_completed_totals(path, runs):
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["run", "completed_total", "started_total", "duration_seconds"])
        for label, points in runs:
            writer.writerow(
                [
                    label,
                    completed_total(points),
                    points[-1].get("started", 0),
                    points[-1].get("at_seconds", 0.0),
                ]
            )


def write_churn_totals(path, churn_runs):
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["run", "source_kind", "total_moves", "duration_seconds"])
        for label, points, kind in churn_runs:
            cumulative = cumulative_from_rate_points(points) if kind == "rate" else cumulative_counter_points(points)
            writer.writerow(
                [
                    label,
                    kind,
                    cumulative[-1]["value"] if cumulative else 0.0,
                    points[-1]["seconds"] if points else 0.0,
                ]
            )


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
        "--churn-run",
        action="append",
        type=parse_run_arg,
        default=[],
        help=(
            "Shard move churn CSV as LABEL=PATH. Use collect-prometheus-run.py output, "
            "usually csv/sd_load_based_moves_total.csv. Can be provided multiple times."
        ),
    )
    parser.add_argument(
        "--churn-kind",
        choices=["auto", "counter", "rate"],
        default="auto",
        help="Interpret --churn-run values as Prometheus counters or rates.",
    )
    parser.add_argument(
        "--prometheus-dir",
        type=Path,
        default=Path("environment/kind-lab/results/prometheus"),
        help=(
            "Directory containing collect-prometheus-run.py outputs. "
            "Used to auto-detect LABEL/csv/sd_load_based_moves_total.csv."
        ),
    )
    parser.add_argument(
        "--no-auto-churn",
        action="store_true",
        help="Do not auto-detect shard move churn CSVs from --prometheus-dir.",
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
        help="Do not draw the first run's offered-starts line on the throughput plot.",
    )
    parser.add_argument(
        "--throughput-smooth-samples",
        type=int,
        default=1,
        help=(
            "Rolling mean window, in summary samples, for throughput plots. "
            "With the default 10s summary interval, 6 means a 60s rolling mean."
        ),
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
    parser.add_argument(
        "--latency-y-max-seconds",
        type=float,
        default=None,
        help="Maximum y-axis value for p95 latency plots in seconds. Use 0 to auto-scale.",
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
        "--completed-total-title",
        default="",
        help="Override the cumulative completed plot title.",
    )
    parser.add_argument(
        "--churn-title",
        default="",
        help="Override the churn rate plot title.",
    )
    parser.add_argument(
        "--churn-total-title",
        default="",
        help="Override the cumulative churn plot title.",
    )
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    cache_dir = args.output_dir / ".cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    os.environ.setdefault("MPLCONFIGDIR", str(cache_dir / "matplotlib"))
    os.environ.setdefault("XDG_CACHE_HOME", str(cache_dir))
    os.environ.setdefault("MPLBACKEND", "Agg")

    import matplotlib.pyplot as plt

    churn_run_args = resolve_churn_run_args(
        args.run,
        args.churn_run,
        args.prometheus_dir,
        args.no_auto_churn,
    )

    runs = [(label, read_summary_json(path)) for label, path in args.run]
    churn_runs = [
        (label, read_prometheus_series_csv(path), infer_churn_kind(path, args.churn_kind))
        for label, path in churn_run_args
    ]

    throughput_path = args.output_dir / f"{args.prefix}-throughput.png"
    completed_total_path = args.output_dir / f"{args.prefix}-completed-total.png"
    latency_path = args.output_dir / f"{args.prefix}-p95-latency.png"
    completed_totals_csv_path = args.output_dir / f"{args.prefix}-completed-totals.csv"
    churn_rate_path = args.output_dir / f"{args.prefix}-churn-rate.png"
    churn_total_path = args.output_dir / f"{args.prefix}-churn-total.png"
    churn_totals_csv_path = args.output_dir / f"{args.prefix}-churn-totals.csv"

    fig, ax = plt.subplots(figsize=(10, 5.5), constrained_layout=True)
    plot_completed_rps(
        ax,
        runs,
        not args.no_started_line,
        args.mark_errors,
        args.throughput_title,
        max(1, args.throughput_smooth_samples),
    )
    apply_time_axis(ax, args.x_min, args.x_max)
    fig.savefig(throughput_path, dpi=180)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(10, 5.5), constrained_layout=True)
    plot_completed_cumulative(ax, runs, args.completed_total_title)
    apply_time_axis(ax, args.x_min, args.x_max)
    fig.savefig(completed_total_path, dpi=180)
    plt.close(fig)
    write_completed_totals(completed_totals_csv_path, runs)

    fig, ax = plt.subplots(figsize=(10, 5.5), constrained_layout=True)
    latency_y_max = args.latency_y_max_seconds
    if latency_y_max is not None and latency_y_max <= 0:
        latency_y_max = None
    plot_p95_latency(ax, runs, args.mark_errors, args.latency_title, latency_y_max)
    apply_time_axis(ax, args.x_min, args.x_max)
    fig.savefig(latency_path, dpi=180)
    plt.close(fig)

    if churn_runs:
        fig, ax = plt.subplots(figsize=(10, 5.5), constrained_layout=True)
        plot_churn_rate(ax, churn_runs, args.churn_title)
        apply_time_axis(ax, args.x_min, args.x_max)
        fig.savefig(churn_rate_path, dpi=180)
        plt.close(fig)

        fig, ax = plt.subplots(figsize=(10, 5.5), constrained_layout=True)
        plot_churn_total(ax, churn_runs, args.churn_total_title)
        apply_time_axis(ax, args.x_min, args.x_max)
        fig.savefig(churn_total_path, dpi=180)
        plt.close(fig)
        write_churn_totals(churn_totals_csv_path, churn_runs)

    print(f"wrote {throughput_path}")
    print(f"wrote {completed_total_path}")
    print(f"wrote {completed_totals_csv_path}")
    print(f"wrote {latency_path}")
    if churn_runs:
        print(f"wrote {churn_rate_path}")
        print(f"wrote {churn_total_path}")
        print(f"wrote {churn_totals_csv_path}")


if __name__ == "__main__":
    main()
