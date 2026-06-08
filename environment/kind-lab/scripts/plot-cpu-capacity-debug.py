#!/usr/bin/env python3
"""Plot CPU capacity debug logs from kind-lab cpu_seconds runs.

Reads CSV files collected by collect-cpu-debug.sh:
  - executor-raw.csv: cumulative process CPU seconds sent in heartbeats
  - observation.csv: shard-distributor CPU cost observations used for greedy weights

Greedy cpu_seconds balancing applies EWMA to CPU cost (busy_cores / load), then scales
executor capacity weights by 1/sqrt(relative_cost) where relative_cost is smoothed_cost
divided by the cluster average and clamped to [0.5, 2.0].
"""

from __future__ import annotations

import argparse
import csv
import math
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import matplotlib.pyplot as plt

MIN_RELATIVE_CPU_COST = 0.5
MAX_RELATIVE_CPU_COST = 2.0


EXECUTOR_RAW_COLUMNS = (
    "logged_at",
    "record_type",
    "executor_id",
    "process_cpu_seconds",
    "sample_unix_nanos",
)

OBSERVATION_COLUMNS = (
    "logged_at",
    "record_type",
    "executor_id",
    "busy_cores",
    "raw_cost",
    "smoothed_cost",
    "smoothed_busy_cores",
    "load",
    "sample_unix_nanos",
)


@dataclass(frozen=True)
class ExecutorRawPoint:
    logged_at: datetime
    executor_id: str
    process_cpu_seconds: float
    sample_time: datetime


@dataclass(frozen=True)
class ExecutorRatePoint:
    sample_time: datetime
    busy_cores: float


@dataclass(frozen=True)
class ObservationPoint:
    logged_at: datetime
    executor_id: str
    busy_cores: float
    raw_cost: float
    smoothed_cost: float
    smoothed_busy_cores: float
    load: float
    sample_time: datetime


@dataclass(frozen=True)
class CapacityWeightPoint:
    sample_time: datetime
    executor_id: str
    smoothed_cost: float
    relative_cost: float
    weight_factor: float


def parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def parse_unix_nanos(value: str) -> datetime:
    return datetime.fromtimestamp(int(value) / 1_000_000_000, tz=timezone.utc)


def read_executor_raw(path: Path) -> list[ExecutorRawPoint]:
    points: list[ExecutorRawPoint] = []
    with path.open("r", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        header = next(reader, None)
        if header is None:
            return points

        # Header omits the leading logged_at column written by the server.
        if header[0] == "record_type":
            columns = EXECUTOR_RAW_COLUMNS
        elif header[0] == "logged_at":
            columns = tuple(header)
        else:
            columns = EXECUTOR_RAW_COLUMNS

        for row in reader:
            if len(row) < len(columns):
                continue
            data = dict(zip(columns, row))
            if data.get("record_type") != "executor_raw":
                continue
            points.append(
                ExecutorRawPoint(
                    logged_at=parse_timestamp(data["logged_at"]),
                    executor_id=data["executor_id"],
                    process_cpu_seconds=float(data["process_cpu_seconds"]),
                    sample_time=parse_unix_nanos(data["sample_unix_nanos"]),
                )
            )

    points.sort(key=lambda point: point.sample_time)
    return points


def read_observations(path: Path) -> list[ObservationPoint]:
    points: list[ObservationPoint] = []
    with path.open("r", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        header = next(reader, None)
        if header is None:
            return points

        if header[0] == "record_type":
            columns = OBSERVATION_COLUMNS
        elif header[0] == "logged_at":
            columns = tuple(header)
        else:
            columns = OBSERVATION_COLUMNS

        for row in reader:
            if len(row) < len(columns):
                continue
            data = dict(zip(columns, row))
            if data.get("record_type") != "observation":
                continue
            points.append(
                ObservationPoint(
                    logged_at=parse_timestamp(data["logged_at"]),
                    executor_id=data["executor_id"],
                    busy_cores=float(data["busy_cores"]),
                    raw_cost=float(data["raw_cost"]),
                    smoothed_cost=float(data["smoothed_cost"]),
                    smoothed_busy_cores=float(data["smoothed_busy_cores"]),
                    load=float(data["load"]),
                    sample_time=parse_unix_nanos(data["sample_unix_nanos"]),
                )
            )

    points.sort(key=lambda point: point.sample_time)
    return points


def derive_executor_busy_cores(points: list[ExecutorRawPoint]) -> list[ExecutorRatePoint]:
    rates: list[ExecutorRatePoint] = []
    for previous, current in zip(points, points[1:]):
        delta_cpu = current.process_cpu_seconds - previous.process_cpu_seconds
        delta_seconds = (current.sample_time - previous.sample_time).total_seconds()
        if delta_cpu < 0 or delta_seconds <= 0:
            continue
        rates.append(
            ExecutorRatePoint(
                sample_time=current.sample_time,
                busy_cores=delta_cpu / delta_seconds,
            )
        )
    return rates


def minutes_since_start(times: list[datetime], origin: datetime) -> list[float]:
    return [(time - origin).total_seconds() / 60.0 for time in times]


def plot_executor_cumulative(ax, points: list[ExecutorRawPoint], title: str) -> None:
    origin = points[0].sample_time
    x = minutes_since_start([point.sample_time for point in points], origin)
    y = [point.process_cpu_seconds for point in points]
    ax.plot(x, y, linewidth=1.2, color="tab:blue")
    ax.set_title(title)
    ax.set_xlabel("Time since first sample (min)")
    ax.set_ylabel("Cumulative process CPU seconds")
    ax.grid(True, alpha=0.25)


def plot_executor_busy_cores(ax, raw_points: list[ExecutorRawPoint], title: str) -> None:
    rates = derive_executor_busy_cores(raw_points)
    if not rates:
        ax.set_title(f"{title} (no rate samples)")
        ax.grid(True, alpha=0.25)
        return

    origin = raw_points[0].sample_time
    x = minutes_since_start([point.sample_time for point in rates], origin)
    y = [point.busy_cores for point in rates]
    ax.plot(x, y, linewidth=1.0, color="tab:orange", alpha=0.9, label="executor delta rate")
    ax.set_title(title)
    ax.set_xlabel("Time since first sample (min)")
    ax.set_ylabel("Busy cores (delta CPU seconds / delta time)")
    ax.grid(True, alpha=0.25)
    ax.legend()


def compute_capacity_weight_points(points: list[ObservationPoint]) -> list[CapacityWeightPoint]:
    grouped: dict[datetime, list[ObservationPoint]] = defaultdict(list)
    for point in points:
        grouped[point.sample_time].append(point)

    weight_points: list[CapacityWeightPoint] = []
    for sample_time in sorted(grouped):
        group = grouped[sample_time]
        valid_costs = [
            point.smoothed_cost
            for point in group
            if point.smoothed_cost > 0 and not math.isnan(point.smoothed_cost) and not math.isinf(point.smoothed_cost)
        ]
        if not valid_costs:
            continue

        average_cost = sum(valid_costs) / len(valid_costs)
        for point in group:
            cost = point.smoothed_cost
            if cost <= 0 or math.isnan(cost) or math.isinf(cost):
                cost = average_cost
            relative_cost = min(max(cost / average_cost, MIN_RELATIVE_CPU_COST), MAX_RELATIVE_CPU_COST)
            weight_points.append(
                CapacityWeightPoint(
                    sample_time=sample_time,
                    executor_id=point.executor_id,
                    smoothed_cost=cost,
                    relative_cost=relative_cost,
                    weight_factor=1.0 / math.sqrt(relative_cost),
                )
            )

    return weight_points


def plot_capacity_cost_signal(ax, points: list[ObservationPoint], title: str) -> None:
    origin = points[0].sample_time
    x = minutes_since_start([point.sample_time for point in points], origin)
    ax.plot(
        x,
        [point.raw_cost for point in points],
        linewidth=1.0,
        color="tab:orange",
        alpha=0.85,
        label="raw cost (busy cores / load)",
    )
    ax.plot(
        x,
        [point.smoothed_cost for point in points],
        linewidth=1.8,
        color="tab:green",
        label="smoothed cost (EWMA input to weights)",
    )
    ax.set_title(title)
    ax.set_xlabel("Time since first observation (min)")
    ax.set_ylabel("CPU cost (busy cores per unit load)")
    ax.grid(True, alpha=0.25)
    ax.legend()


def plot_capacity_weight_factor(ax, weight_points: list[CapacityWeightPoint], title: str) -> None:
    if not weight_points:
        ax.set_title(f"{title} (no weight samples)")
        ax.grid(True, alpha=0.25)
        return

    origin = weight_points[0].sample_time
    executor_ids = sorted({point.executor_id for point in weight_points})
    for executor_id in executor_ids:
        series = [point for point in weight_points if point.executor_id == executor_id]
        x = minutes_since_start([point.sample_time for point in series], origin)
        y = [point.weight_factor for point in series]
        ax.plot(x, y, linewidth=1.8, label=f"{executor_id[:8]}… weight / sqrt(rel_cost)")

    ax.axhline(1.0, color="black", linewidth=0.8, linestyle="--", alpha=0.5)
    ax.set_title(title)
    ax.set_xlabel("Time since first observation (min)")
    ax.set_ylabel("Capacity weight factor")
    ax.set_ylim(MIN_RELATIVE_CPU_COST ** -0.5 * 0.95, MAX_RELATIVE_CPU_COST ** -0.5 * 1.05)
    ax.grid(True, alpha=0.25)
    ax.legend()


def plot_relative_cost(ax, weight_points: list[CapacityWeightPoint], title: str) -> None:
    if not weight_points:
        ax.set_title(f"{title} (no weight samples)")
        ax.grid(True, alpha=0.25)
        return

    origin = weight_points[0].sample_time
    executor_ids = sorted({point.executor_id for point in weight_points})
    for executor_id in executor_ids:
        series = [point for point in weight_points if point.executor_id == executor_id]
        x = minutes_since_start([point.sample_time for point in series], origin)
        y = [point.relative_cost for point in series]
        ax.plot(x, y, linewidth=1.4, label=f"{executor_id[:8]}… rel. cost")

    ax.axhline(1.0, color="black", linewidth=0.8, linestyle="--", alpha=0.5)
    ax.axhline(MIN_RELATIVE_CPU_COST, color="tab:red", linewidth=0.8, linestyle=":", alpha=0.6)
    ax.axhline(MAX_RELATIVE_CPU_COST, color="tab:red", linewidth=0.8, linestyle=":", alpha=0.6)
    ax.set_title(title)
    ax.set_xlabel("Time since first observation (min)")
    ax.set_ylabel("Relative CPU cost (clamped)")
    ax.grid(True, alpha=0.25)
    ax.legend()


def plot_load(ax, points: list[ObservationPoint], title: str) -> None:
    origin = points[0].sample_time
    x = minutes_since_start([point.sample_time for point in points], origin)
    ax.plot(x, [point.load for point in points], linewidth=1.4, color="tab:purple")
    ax.set_title(title)
    ax.set_xlabel("Time since first observation (min)")
    ax.set_ylabel("Assignment load")
    ax.grid(True, alpha=0.25)


def write_summary(path: Path, executor_raw: list[ExecutorRawPoint], observations: list[ObservationPoint]) -> None:
    lines = ["metric,value"]
    lines.append(f"executor_raw_samples,{len(executor_raw)}")
    lines.append(f"observation_samples,{len(observations)}")
    if executor_raw:
        rates = derive_executor_busy_cores(executor_raw)
        if rates:
            values = [point.busy_cores for point in rates]
            lines.append(f"executor_busy_cores_min,{min(values):.6f}")
            lines.append(f"executor_busy_cores_max,{max(values):.6f}")
            lines.append(f"executor_busy_cores_mean,{sum(values) / len(values):.6f}")
    if observations:
        raw_costs = [point.raw_cost for point in observations]
        smoothed_costs = [point.smoothed_cost for point in observations]
        lines.append(f"observation_raw_cost_min,{min(raw_costs):.6f}")
        lines.append(f"observation_raw_cost_max,{max(raw_costs):.6f}")
        lines.append(f"observation_smoothed_cost_min,{min(smoothed_costs):.6f}")
        lines.append(f"observation_smoothed_cost_max,{max(smoothed_costs):.6f}")
        weight_points = compute_capacity_weight_points(observations)
        if weight_points:
            relative_costs = [point.relative_cost for point in weight_points]
            weight_factors = [point.weight_factor for point in weight_points]
            lines.append(f"capacity_relative_cost_min,{min(relative_costs):.6f}")
            lines.append(f"capacity_relative_cost_max,{max(relative_costs):.6f}")
            lines.append(f"capacity_weight_factor_min,{min(weight_factors):.6f}")
            lines.append(f"capacity_weight_factor_max,{max(weight_factors):.6f}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--cpu-debug-dir",
        type=Path,
        required=True,
        help="Directory containing executor-raw.csv and observation.csv",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Directory for plot output (default: CPU debug dir)",
    )
    parser.add_argument(
        "--prefix",
        default="cpu-capacity",
        help="Filename prefix for generated plots",
    )
    args = parser.parse_args()

    cpu_debug_dir = args.cpu_debug_dir
    output_dir = args.output_dir or cpu_debug_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    executor_raw_path = cpu_debug_dir / "executor-raw.csv"
    observation_path = cpu_debug_dir / "observation.csv"

    executor_raw = read_executor_raw(executor_raw_path) if executor_raw_path.exists() else []
    observations = read_observations(observation_path) if observation_path.exists() else []

    if not executor_raw and not observations:
        print(f"no CPU debug CSV files found in {cpu_debug_dir}", file=sys.stderr)
        sys.exit(1)

    if executor_raw:
        fig, ax = plt.subplots(figsize=(10, 5.5), constrained_layout=True)
        plot_executor_cumulative(ax, executor_raw, "Executor heartbeat: cumulative process CPU seconds")
        path = output_dir / f"{args.prefix}-executor-cumulative.png"
        fig.savefig(path, dpi=180)
        plt.close(fig)
        print(f"wrote {path}")

        fig, ax = plt.subplots(figsize=(10, 5.5), constrained_layout=True)
        plot_executor_busy_cores(ax, executor_raw, "Executor heartbeat: derived busy cores")
        path = output_dir / f"{args.prefix}-executor-busy-cores.png"
        fig.savefig(path, dpi=180)
        plt.close(fig)
        print(f"wrote {path}")
    else:
        print(f"warning: missing {executor_raw_path}", file=sys.stderr)

    if observations:
        weight_points = compute_capacity_weight_points(observations)

        fig, ax = plt.subplots(figsize=(10, 5.5), constrained_layout=True)
        plot_capacity_cost_signal(
            ax,
            observations,
            "Greedy capacity signal: smoothed CPU cost (busy cores per unit load)",
        )
        path = output_dir / f"{args.prefix}-capacity-cost.png"
        fig.savefig(path, dpi=180)
        plt.close(fig)
        print(f"wrote {path}")

        fig, ax = plt.subplots(figsize=(10, 5.5), constrained_layout=True)
        plot_relative_cost(
            ax,
            weight_points,
            "Greedy capacity weights: relative CPU cost (cluster average = 1.0)",
        )
        path = output_dir / f"{args.prefix}-capacity-relative-cost.png"
        fig.savefig(path, dpi=180)
        plt.close(fig)
        print(f"wrote {path}")

        fig, ax = plt.subplots(figsize=(10, 5.5), constrained_layout=True)
        plot_capacity_weight_factor(
            ax,
            weight_points,
            "Greedy capacity weights: executor weight factor (1 / sqrt(relative cost))",
        )
        path = output_dir / f"{args.prefix}-capacity-weight.png"
        fig.savefig(path, dpi=180)
        plt.close(fig)
        print(f"wrote {path}")

        fig, ax = plt.subplots(figsize=(10, 5.5), constrained_layout=True)
        plot_load(ax, observations, "Assignment load at observation time (divisor in cost, not smoothed)")
        path = output_dir / f"{args.prefix}-load.png"
        fig.savefig(path, dpi=180)
        plt.close(fig)
        print(f"wrote {path}")

        fig, axes = plt.subplots(3, 1, figsize=(10, 10), sharex=True, constrained_layout=True)
        plot_capacity_cost_signal(axes[0], observations, "Smoothed CPU cost")
        plot_capacity_weight_factor(axes[1], weight_points, "Capacity weight factor")
        plot_load(axes[2], observations, "Assignment load")
        path = output_dir / f"{args.prefix}-overview.png"
        fig.savefig(path, dpi=180)
        plt.close(fig)
        print(f"wrote {path}")
    else:
        print(f"warning: missing {observation_path}", file=sys.stderr)

    summary_path = output_dir / f"{args.prefix}-summary.csv"
    write_summary(summary_path, executor_raw, observations)
    print(f"wrote {summary_path}")


if __name__ == "__main__":
    main()
