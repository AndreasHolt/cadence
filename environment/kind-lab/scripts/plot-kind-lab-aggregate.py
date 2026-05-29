#!/usr/bin/env python3
"""Aggregate repeated kind-lab runs into report figures and summary tables.

The script intentionally does not require Prometheus exports. It consumes the
small artifacts produced by the controlled run script:

  * matching-lab .log files containing `summary_json:` lines
  * sample-utilization .csv files containing pod CPU/throttling samples
  * optional *-metadata.json files, currently only copied into the manifest

It discovers runs by directory, so the files inside `May21/Off/Off1` do not need
matching names such as `off-n1`. The method is inferred from the parent path by
default, and can be overridden with --method-dir.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
import statistics
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable

import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter


METHOD_ORDER = ["off", "latency", "latencycostaware", "cpuseconds"]
METHOD_LABELS = {
    "off": "Greedy baseline",
    "latency": "Latency-aware greedy",
    "latencycostaware": "Latency + cost-aware greedy",
    "cpuseconds": "CPU utilization aware greedy",
}
METHOD_COLORS = {
    "off": "#4c78a8",
    "latency": "#54a24b",
    "latencycostaware": "#b279a2",
    "cpuseconds": "#e45756",
}
POD_LABELS = {
    "cadence-matching-a-0": "Matching A",
    "cadence-matching-b-0": "Matching B",
    "cadence-matching-c-0": "Matching C",
}
POD_COLORS = {
    "cadence-matching-a-0": "#4c78a8",
    "cadence-matching-b-0": "#f58518",
    "cadence-matching-c-0": "#e45756",
}
IMBALANCE_COLORS = {
    "reported": "#4c78a8",
    "smoothed": "#e45756",
}
PROMETHEUS_STEP_SECONDS = 15.0


@dataclass
class Run:
    method: str
    index: int
    directory: Path
    log_path: Path
    csv_path: Path | None
    metadata_path: Path | None
    summaries: list[dict]
    cpu_rows: list[dict]
    start_time: datetime | None


def normalize(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", value.lower())


def infer_method(path: Path) -> str | None:
    parts = [normalize(part) for part in path.parts]
    for part in reversed(parts):
        if "latencycostaware" in part or "latencyca" in part:
            return "latencycostaware"
        if "latency" in part:
            return "latency"
        if "cpusecond" in part or part == "cpu" or part.startswith("cpu"):
            return "cpuseconds"
        if part == "off" or "baseline" in part or "greedybaseline" in part:
            return "off"
    return None


def method_label(method: str) -> str:
    return METHOD_LABELS.get(method, method)


def parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def parse_run_id_timestamp(value: str) -> datetime | None:
    try:
        date_part, frac_part = value.split(".", 1) if "." in value else (value, "")
        parsed = datetime.strptime(date_part, "%Y%m%dT%H%M%S").replace(tzinfo=timezone.utc)
        if frac_part:
            parsed = parsed.replace(microsecond=int(frac_part[:6].ljust(6, "0")))
        return parsed
    except ValueError:
        return None


def infer_run_start_from_log(path: Path) -> datetime | None:
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            match = re.search(r"^run id:\s+(\S+)", line.strip())
            if match:
                return parse_run_id_timestamp(match.group(1))
    return None


def parse_summary_log(path: Path) -> list[dict]:
    rows: list[dict] = []
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            if "summary_json:" not in line:
                continue
            payload = line.split("summary_json:", 1)[1].strip()
            try:
                item = json.loads(payload)
            except json.JSONDecodeError:
                continue
            if "at_seconds" not in item:
                continue
            rows.append(item)
    rows.sort(key=lambda item: float(item.get("at_seconds", 0)))
    return rows


def parse_utilization_csv(path: Path | None) -> list[dict]:
    if path is None:
        return []
    rows: list[dict] = []

    def append_row(raw: dict[str, str]) -> None:
        pod = raw.get("pod", "")
        if not pod.startswith("cadence-matching-"):
            return
        try:
            rows.append(
                {
                    "timestamp": parse_timestamp(raw.get("timestamp", "")),
                    "pod": pod,
                    "cpu_cores": float(raw.get("cpu_cores") or 0),
                    "throttled_cores": float(raw.get("throttled_cores") or 0),
                    "throttled_events": float(raw.get("throttled_events") or 0),
                }
            )
        except (ValueError, TypeError):
            return

    with path.open("r", encoding="utf-8", errors="replace", newline="") as handle:
        first_line = handle.readline()
        handle.seek(0)
        if first_line.startswith("timestamp,"):
            reader = csv.DictReader(handle)
            for row in reader:
                append_row(row)
        else:
            reader = csv.reader(handle)
            for row in reader:
                if len(row) < 5:
                    continue
                append_row(
                    {
                        "timestamp": row[0],
                        "pod": row[1],
                        "cpu_cores": row[2],
                        "throttled_cores": row[3],
                        "throttled_events": row[4],
                    }
                )
    return rows


def find_one(directory: Path, suffix: str, contains: str | None = None) -> Path | None:
    candidates = sorted(directory.glob(f"*{suffix}"))
    if contains:
        candidates = [p for p in candidates if contains in p.name]
    return candidates[0] if candidates else None


def discover_runs(root: Path, method_dirs: dict[str, Path]) -> list[Run]:
    runs: list[Run] = []
    search_dirs: list[tuple[str | None, Path]] = []
    if method_dirs:
        for method, directory in method_dirs.items():
            search_dirs.extend((method, child) for child in sorted(directory.iterdir()) if child.is_dir())
    else:
        search_dirs.extend(
            (None, child)
            for child in sorted(root.rglob("*"))
            if child.is_dir() and "compact" not in child.parts and not child.name.endswith("-compact")
        )

    seen_dirs: set[Path] = set()
    for forced_method, directory in search_dirs:
        directory = directory.resolve()
        if directory in seen_dirs:
            continue
        seen_dirs.add(directory)
        log_path = find_one(directory, ".log")
        if log_path is None:
            continue
        method = forced_method or infer_method(directory)
        if method is None:
            continue
        csv_path = find_one(directory, ".csv")
        metadata_path = find_one(directory, ".json", contains="metadata")
        summaries = parse_summary_log(log_path)
        if not summaries:
            print(f"warning: {log_path} has no summary_json rows; skipping")
            continue
        runs.append(
            Run(
                method=method,
                index=0,
                directory=directory,
                log_path=log_path,
                csv_path=csv_path,
                metadata_path=metadata_path,
                summaries=summaries,
                cpu_rows=parse_utilization_csv(csv_path),
                start_time=infer_run_start_from_log(log_path),
            )
        )

    by_method: dict[str, list[Run]] = defaultdict(list)
    for run in runs:
        by_method[run.method].append(run)
    ordered: list[Run] = []
    for method in METHOD_ORDER + sorted(set(by_method) - set(METHOD_ORDER)):
        method_runs = sorted(by_method.get(method, []), key=lambda r: str(r.directory))
        for i, run in enumerate(method_runs, start=1):
            run.index = i
            ordered.append(run)
    return ordered


def median(values: Iterable[float]) -> float:
    vals = [v for v in values if v is not None and not math.isnan(v)]
    return statistics.median(vals) if vals else math.nan


def mean(values: Iterable[float]) -> float:
    vals = [v for v in values if v is not None and not math.isnan(v)]
    return statistics.fmean(vals) if vals else math.nan


def stdev(values: Iterable[float]) -> float:
    vals = [v for v in values if v is not None and not math.isnan(v)]
    return statistics.stdev(vals) if len(vals) > 1 else 0.0


def time_key(seconds: float, step: float) -> float:
    return round(round(seconds / step) * step, 6)


def series_from_summaries(run: Run, metric: str, *, step: float, valid: Callable[[dict], bool] | None = None) -> dict[float, float]:
    out: dict[float, float] = {}
    for row in run.summaries:
        if valid is not None and not valid(row):
            continue
        value = row.get(metric)
        if value is None:
            continue
        try:
            value_f = float(value)
        except (TypeError, ValueError):
            continue
        out[time_key(float(row["at_seconds"]), step)] = value_f
    return out


def ratio_series(run: Run, *, step: float) -> dict[float, float]:
    out: dict[float, float] = {}
    for row in run.summaries:
        started = float(row.get("started") or 0)
        completed = float(row.get("completed") or 0)
        if started <= 0:
            continue
        out[time_key(float(row["at_seconds"]), step)] = completed / started
    return out


def utilization_start(run: Run) -> datetime | None:
    if run.start_time is not None:
        return run.start_time
    if not run.cpu_rows:
        return None
    return min(row["timestamp"] for row in run.cpu_rows)


def cpu_total_series(run: Run, *, step: float) -> dict[float, float]:
    if not run.cpu_rows:
        return {}
    start = utilization_start(run)
    if start is None:
        return {}
    per_sample: dict[float, float] = defaultdict(float)
    for row in run.cpu_rows:
        t = round((row["timestamp"] - start).total_seconds(), 6)
        per_sample[t] += float(row["cpu_cores"])
    return dict(per_sample)


def cpu_pod_series(run: Run, pod: str, *, step: float) -> dict[float, float]:
    if not run.cpu_rows:
        return {}
    start = utilization_start(run)
    if start is None:
        return {}
    out: dict[float, float] = {}
    for row in run.cpu_rows:
        if row["pod"] == pod:
            out[round((row["timestamp"] - start).total_seconds(), 6)] = float(row["cpu_cores"])
    return out


def throttling_events_total_series(run: Run, *, step: float) -> dict[float, float]:
    if not run.cpu_rows:
        return {}
    start = utilization_start(run)
    if start is None:
        return {}
    per_sample: dict[float, float] = defaultdict(float)
    for row in run.cpu_rows:
        t = round((row["timestamp"] - start).total_seconds(), 6)
        per_sample[t] += float(row["throttled_events"])
    return dict(per_sample)


def throttling_events_pod_series(run: Run, pod: str, *, step: float) -> dict[float, float]:
    if not run.cpu_rows:
        return {}
    start = utilization_start(run)
    if start is None:
        return {}
    out: dict[float, float] = {}
    for row in run.cpu_rows:
        if row["pod"] == pod:
            out[round((row["timestamp"] - start).total_seconds(), 6)] = float(row["throttled_events"])
    return out


def aggregate(series_list: list[dict[float, float]]) -> tuple[list[float], list[float], list[float], list[float]]:
    times = sorted(set().union(*(s.keys() for s in series_list))) if series_list else []
    xs: list[float] = []
    lows: list[float] = []
    means: list[float] = []
    highs: list[float] = []
    for t in times:
        vals = [s[t] for s in series_list if t in s and not math.isnan(s[t])]
        if not vals:
            continue
        xs.append(t / 60.0)
        lows.append(min(vals))
        means.append(statistics.fmean(vals))
        highs.append(max(vals))
    return xs, lows, means, highs


def apply_common_axes(ax, ylabel: str | None, x_max: float | None = None):
    if ylabel:
        ax.set_ylabel(ylabel)
    ax.grid(True, alpha=0.25)
    if x_max is not None:
        ax.set_xlim(0, x_max / 60.0)


def finish_shared_axes(fig, axes, *, xlabel: str, ylabel: str):
    for ax in axes[:-1]:
        ax.set_xlabel("")
        ax.tick_params(labelbottom=False)
    axes[-1].set_xlabel(xlabel)
    fig.supylabel(ylabel)


def add_shared_legend(fig, axes, *, ncol: int, y: float = 0.965, include_labels: set[str] | None = None):
    handles, labels = [], []
    seen = set()
    for ax in axes:
        ax_handles, ax_labels = ax.get_legend_handles_labels()
        for handle, label in zip(ax_handles, ax_labels):
            if include_labels is not None and label not in include_labels:
                continue
            if label in seen:
                continue
            seen.add(label)
            handles.append(handle)
            labels.append(label)
    if handles:
        fig.legend(handles, labels, loc="upper center", ncol=ncol, frameon=False, bbox_to_anchor=(0.5, y))


def plot_aggregate_metric(
    output: Path,
    title: str,
    ylabel: str,
    method_series: dict[str, list[dict[float, float]]],
    *,
    x_max: float | None,
    y_min: float | None = None,
    y_max: float | None = None,
    percent: bool = False,
):
    fig, ax = plt.subplots(figsize=(8.5, 4.8), constrained_layout=True)
    for method in METHOD_ORDER:
        if method not in method_series:
            continue
        xs, lows, meds, highs = aggregate(method_series[method])
        if not xs:
            continue
        if percent:
            lows = [v * 100 for v in lows]
            meds = [v * 100 for v in meds]
            highs = [v * 100 for v in highs]
        color = METHOD_COLORS.get(method)
        ax.plot(xs, meds, label=method_label(method), color=color, linewidth=2.2)
        if len(method_series[method]) > 1:
            ax.fill_between(xs, lows, highs, color=color, alpha=0.16, linewidth=0)
    ax.set_title(title)
    ax.set_xlabel("Time since start (min)")
    apply_common_axes(ax, ylabel, x_max)
    if y_min is not None or y_max is not None:
        ax.set_ylim(y_min, y_max)
    if percent:
        ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:.0f}%"))
    ax.legend(frameon=False)
    fig.savefig(output, dpi=180)
    plt.close(fig)


def plot_completed_cumulative(output: Path, runs_by_method: dict[str, list[Run]], *, step: float, x_max: float | None):
    fig, ax = plt.subplots(figsize=(8.5, 4.8), constrained_layout=True)
    for method in METHOD_ORDER:
        runs = runs_by_method.get(method, [])
        if not runs:
            continue
        completed = [series_from_summaries(r, "completed", step=step) for r in runs]
        xs, lows, meds, highs = aggregate(completed)
        if not xs:
            continue
        final_counts = [final_value(r, "completed") for r in runs]
        valid_counts = [v for v in final_counts if not math.isnan(v)]
        if valid_counts:
            final_mean = statistics.fmean(valid_counts)
            final_min = min(valid_counts)
            final_max = max(valid_counts)
            if final_min == final_max:
                legend = f"{method_label(method)} mean={final_mean:,.0f}"
            else:
                legend = f"{method_label(method)} mean={final_mean:,.0f} ({final_min:,.0f}-{final_max:,.0f})"
        else:
            legend = method_label(method)
        color = METHOD_COLORS.get(method)
        ax.plot(xs, meds, label=legend, color=color, linewidth=2.2)
        if len(completed) > 1:
            ax.fill_between(xs, lows, highs, color=color, alpha=0.16, linewidth=0)
    ax.set_title("Cumulative completed workflows")
    ax.set_xlabel("Time since start (min)")
    apply_common_axes(ax, "Completed workflows", x_max)
    ax.legend(frameon=False, fontsize=8)
    fig.savefig(output, dpi=180)
    plt.close(fig)


def plot_throughput(output: Path, runs_by_method: dict[str, list[Run]], *, step: float, x_max: float | None):
    fig, ax = plt.subplots(figsize=(8.5, 4.8), constrained_layout=True)
    for method in METHOD_ORDER:
        runs = runs_by_method.get(method, [])
        if not runs:
            continue
        completed = [series_from_summaries(r, "window_completed_rps", step=step) for r in runs]
        started = [series_from_summaries(r, "window_started_rps", step=step) for r in runs]
        xs, lows, meds, highs = aggregate(completed)
        color = METHOD_COLORS.get(method)
        ax.plot(xs, meds, label=f"{method_label(method)} completed", color=color, linewidth=2.2)
        if len(completed) > 1:
            ax.fill_between(xs, lows, highs, color=color, alpha=0.16, linewidth=0)
        xs_s, _, meds_s, _ = aggregate(started)
        if xs_s:
            ax.plot(xs_s, meds_s, color=color, linestyle="--", alpha=0.55, linewidth=1.4)
    ax.set_title("Started and completed workflow rate")
    ax.set_xlabel("Time since start (min)")
    apply_common_axes(ax, "Workflows/s", x_max)
    ax.legend(frameon=False, fontsize=9)
    fig.savefig(output, dpi=180)
    plt.close(fig)


def prometheus_csv_path(run: Run, name: str) -> Path | None:
    stem = run.log_path.stem
    candidates = [
        run.directory / stem / "csv" / name,
        run.directory / "csv" / name,
    ]
    for parent in [run.directory.parent, run.directory.parent.parent]:
        candidates.extend(parent.glob(f"compact/*/{stem}/csv/{name}"))
        candidates.extend(parent.glob(f"compact/{stem}-compact/{stem}/csv/{name}"))
    for candidate in candidates:
        if candidate.exists():
            return candidate
    nested = sorted(run.directory.glob(f"**/csv/{name}"))
    if nested:
        return nested[0]
    return None


def parse_labels(value: str) -> dict:
    if not value:
        return {}
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return {}


def prometheus_series(
    run: Run,
    name: str,
    *,
    label_filter: Callable[[dict], bool] | None = None,
    combine: str = "sum",
) -> dict[float, float]:
    path = prometheus_csv_path(run, name)
    if path is None:
        return {}
    start = run.start_time
    per_time: dict[float, list[float]] = defaultdict(list)
    with path.open("r", encoding="utf-8", errors="replace", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            labels = parse_labels(row.get("labels", "{}"))
            if label_filter is not None and not label_filter(labels):
                continue
            try:
                timestamp = parse_timestamp(row["timestamp"])
                value = float(row["value"])
            except (KeyError, TypeError, ValueError):
                continue
            if start is None:
                start = timestamp
            t = time_key((timestamp - start).total_seconds(), PROMETHEUS_STEP_SECONDS)
            per_time[t].append(value)
    out: dict[float, float] = {}
    for t, values in per_time.items():
        if combine == "max":
            out[t] = max(values)
        elif combine == "mean":
            out[t] = mean(values)
        else:
            out[t] = sum(values)
    return out


def prometheus_pod_series(run: Run, name: str, pod: str) -> dict[float, float]:
    return prometheus_series(run, name, label_filter=lambda labels: labels.get("pod") == pod, combine="sum")


def integrated_core_minutes(series: dict[float, float]) -> float:
    items = sorted(series.items())
    if len(items) < 2:
        return 0.0
    total_core_seconds = 0.0
    for (t0, v0), (t1, v1) in zip(items, items[1:]):
        if t1 <= t0:
            continue
        total_core_seconds += ((v0 + v1) / 2.0) * (t1 - t0)
    return total_core_seconds / 60.0


def plot_cpu_by_method(
    output: Path,
    runs_by_method: dict[str, list[Run]],
    *,
    step: float,
    x_max: float | None,
    y_max: float | None,
):
    methods = [m for m in METHOD_ORDER if runs_by_method.get(m)]
    if not methods:
        return
    fig, axes = plt.subplots(len(methods), 1, figsize=(9.5, 3.4 * len(methods)), sharex=True, sharey=True)
    fig.subplots_adjust(top=0.87, bottom=0.07, left=0.11, right=0.98, hspace=0.32)
    if len(methods) == 1:
        axes = [axes]
    for ax, method in zip(axes, methods):
        runs = runs_by_method[method]
        for pod in POD_LABELS:
            series = [cpu_pod_series(r, pod, step=step) for r in runs if r.cpu_rows]
            xs, lows, meds, highs = aggregate(series)
            if not xs:
                continue
            color = POD_COLORS[pod]
            ax.plot(xs, meds, label=POD_LABELS[pod], color=color, linewidth=1.9)
            if len(series) > 1:
                ax.fill_between(xs, lows, highs, color=color, alpha=0.12, linewidth=0)
        ax.set_title(method_label(method))
        apply_common_axes(ax, None, x_max)
        ax.set_ylim(0, y_max if y_max is not None else 2.2)
    fig.suptitle("Matching CPU usage by executor", fontsize=13, y=0.985)
    finish_shared_axes(fig, axes, xlabel="Time since start (min)", ylabel="CPU cores")
    add_shared_legend(fig, axes, ncol=3, y=0.965)
    fig.savefig(output, dpi=180)
    plt.close(fig)


def plot_total_by_method(
    output: Path,
    runs_by_method: dict[str, list[Run]],
    *,
    x_max: float | None,
    title: str,
    ylabel: str,
    series_builder: Callable[[Run], dict[float, float]],
    y_max: float | None = None,
):
    methods = [m for m in METHOD_ORDER if runs_by_method.get(m)]
    if not methods:
        return
    fig, axes = plt.subplots(len(methods), 1, figsize=(9.5, 3.4 * len(methods)), sharex=True, sharey=True)
    fig.subplots_adjust(top=0.87, bottom=0.07, left=0.11, right=0.98, hspace=0.32)
    if len(methods) == 1:
        axes = [axes]
    for ax, method in zip(axes, methods):
        series = [series_builder(r) for r in runs_by_method[method]]
        series = [s for s in series if s]
        xs, lows, meds, highs = aggregate(series)
        if xs:
            color = METHOD_COLORS.get(method)
            if len(series) > 1 and any(abs(lo - hi) > 1e-9 for lo, hi in zip(lows, highs)):
                ax.fill_between(xs, lows, highs, color=color, alpha=0.18, linewidth=0)
            ax.plot(xs, meds, color=color, linewidth=2.4)
        else:
            ax.text(0.5, 0.5, "No compact Prometheus CSVs found", transform=ax.transAxes, ha="center", va="center", alpha=0.65)
        ax.set_title(method_label(method))
        apply_common_axes(ax, None, x_max)
        ax.set_ylim(bottom=0)
        if y_max is not None:
            ax.set_ylim(0, y_max)
    fig.suptitle(title, fontsize=13, y=0.985)
    finish_shared_axes(fig, axes, xlabel="Time since start (min)", ylabel=ylabel)
    add_shared_legend(fig, axes, ncol=2, y=0.965)
    fig.savefig(output, dpi=180)
    plt.close(fig)


def plot_throttling_by_method(output: Path, runs_by_method: dict[str, list[Run]], *, x_max: float | None, y_max: float | None):
    methods = [m for m in METHOD_ORDER if runs_by_method.get(m)]
    if not methods:
        return
    fig, axes = plt.subplots(len(methods), 1, figsize=(9.5, 3.5 * len(methods)), sharex=True)
    fig.subplots_adjust(top=0.87, bottom=0.07, left=0.11, right=0.98, hspace=0.32)
    if len(methods) == 1:
        axes = [axes]
    for ax, method in zip(axes, methods):
        runs = runs_by_method[method]
        for pod in POD_LABELS:
            series = [throttling_events_pod_series(r, pod, step=PROMETHEUS_STEP_SECONDS) for r in runs]
            series = [s for s in series if s]
            xs, lows, meds, highs = aggregate(series)
            if not xs:
                continue
            color = POD_COLORS[pod]
            ax.plot(xs, meds, label=POD_LABELS[pod], color=color, linewidth=1.9)
            if len(series) > 1:
                ax.fill_between(xs, lows, highs, color=color, alpha=0.12, linewidth=0)
        ax.set_title(method_label(method))
        apply_common_axes(ax, None, x_max)
        if y_max is not None:
            ax.set_ylim(0, y_max)
        handles, labels = ax.get_legend_handles_labels()
        if not handles:
            ax.text(0.5, 0.5, "No utilization throttling samples found", transform=ax.transAxes, ha="center", va="center", alpha=0.65)
    fig.suptitle("Matching CPU throttling by executor", fontsize=13, y=0.985)
    finish_shared_axes(fig, axes, xlabel="Time since start (min)", ylabel="Throttling events per 10s sample")
    add_shared_legend(fig, axes, ncol=3, y=0.965)
    fig.savefig(output, dpi=180)
    plt.close(fig)


def plot_throttling_total(output: Path, runs_by_method: dict[str, list[Run]], *, x_max: float | None):
    plot_aggregate_metric(
        output,
        "Total Matching CPU throttling",
        "Throttling events per 10s sample",
        {m: [throttling_events_total_series(r, step=PROMETHEUS_STEP_SECONDS) for r in rs] for m, rs in runs_by_method.items()},
        x_max=x_max,
        y_min=0,
    )


def plot_shard_moves_total(output: Path, runs_by_method: dict[str, list[Run]], *, x_max: float | None):
    plot_aggregate_metric(
        output,
        "Cumulative load-based shard moves",
        "Shard moves",
        {
            m: [prometheus_series(r, "sd_load_based_moves_total.csv", combine="max") for r in rs]
            for m, rs in runs_by_method.items()
        },
        x_max=x_max,
        y_min=0,
    )


def plot_shard_single_moves_total(output: Path, runs_by_method: dict[str, list[Run]], *, x_max: float | None):
    plot_aggregate_metric(
        output,
        "Cumulative single-shard moves",
        "Shards moved (single moves)",
        {
            m: [prometheus_series(r, "sd_load_based_single_moves_total.csv", combine="max") for r in rs]
            for m, rs in runs_by_method.items()
        },
        x_max=x_max,
        y_min=0,
    )


def plot_shard_swap_moves_total(output: Path, runs_by_method: dict[str, list[Run]], *, x_max: float | None):
    plot_aggregate_metric(
        output,
        "Cumulative pairwise swap moves",
        "Swap operations",
        {
            m: [prometheus_series(r, "sd_load_based_swap_moves_total.csv", combine="max") for r in rs]
            for m, rs in runs_by_method.items()
        },
        x_max=x_max,
        y_min=0,
    )


def plot_shard_swap_fraction(output: Path, runs_by_method: dict[str, list[Run]], *, x_max: float | None):
    """Fraction of shard-moves attributable to swaps: 2*swap_ops / (single_shards + 2*swap_ops)."""
    method_series: dict[str, list[dict[float, float]]] = {}
    for method, runs in runs_by_method.items():
        fractions: list[dict[float, float]] = []
        for run in runs:
            single = prometheus_series(run, "sd_load_based_single_moves_total.csv", combine="max")
            swap = prometheus_series(run, "sd_load_based_swap_moves_total.csv", combine="max")
            if not single and not swap:
                continue
            keys = sorted(set(single) | set(swap))
            fractions.append(
                {
                    t: (2.0 * swap.get(t, 0.0)) / denom
                    for t in keys
                    if (denom := single.get(t, 0.0) + 2.0 * swap.get(t, 0.0)) > 0
                }
            )
        if fractions:
            method_series[method] = fractions
    plot_aggregate_metric(
        output,
        "Share of shard moves from swap operations",
        "Swap share of shard moves",
        method_series,
        x_max=x_max,
        y_min=0,
        y_max=1,
        percent=True,
    )


def prometheus_counter_final_delta(run: Run, name: str) -> float:
    series = prometheus_series(run, name, combine="max")
    if not series:
        return math.nan
    first = series[min(series)]
    last = series[max(series)]
    if last >= first:
        return last - first
    return last


def plot_moved_shard_load(output: Path, runs_by_method: dict[str, list[Run]], *, x_max: float | None):
    plot_aggregate_metric(
        output,
        "Moved shard load",
        "Moved shard load",
        {
            m: [prometheus_series(r, "sd_moved_shard_load.csv", combine="max") for r in rs]
            for m, rs in runs_by_method.items()
        },
        x_max=x_max,
        y_min=0,
    )


def scale_series(series: dict[float, float], factor: float) -> dict[float, float]:
    return {t: value * factor for t, value in series.items()}


def counter_delta_series(series: dict[float, float]) -> dict[float, float]:
    if not series:
        return {}
    first = series[min(series)]
    return {t: max(0.0, value - first) for t, value in series.items()}


def plot_moved_shard_load_total(output: Path, runs_by_method: dict[str, list[Run]], *, x_max: float | None):
    plot_aggregate_metric(
        output,
        "Cumulative moved shard load",
        "Moved shard load",
        {
            m: [
                scale_series(counter_delta_series(prometheus_series(r, "sd_moved_shard_load_total.csv", combine="max")), 0.001)
                for r in rs
            ]
            for m, rs in runs_by_method.items()
        },
        x_max=x_max,
        y_min=0,
    )


def plot_assignment_imbalance(output: Path, runs_by_method: dict[str, list[Run]], *, x_max: float | None, kind: str):
    methods = [m for m in METHOD_ORDER if runs_by_method.get(m)]
    if not methods:
        return
    if kind == "cv":
        reported_file = "sd_assignment_load_cv.csv"
        smoothed_file = "sd_assignment_smoothed_load_cv.csv"
        ylabel = "Coefficient of variation"
        title = "Assignment imbalance (CV)"
    else:
        reported_file = "sd_assignment_load_max_over_mean.csv"
        smoothed_file = "sd_assignment_smoothed_load_max_over_mean.csv"
        ylabel = "Max / mean load"
        title = "Assignment imbalance (max / mean)"
    fig, axes = plt.subplots(len(methods), 1, figsize=(9.5, 3.5 * len(methods)), sharex=True, sharey=True)
    fig.subplots_adjust(top=0.87, bottom=0.07, left=0.11, right=0.98, hspace=0.32)
    if len(methods) == 1:
        axes = [axes]
    for ax, method in zip(axes, methods):
        runs = runs_by_method[method]
        for label, filename, color_key in [
            ("Reported load", reported_file, "reported"),
            ("Smoothed load", smoothed_file, "smoothed"),
        ]:
            series = [prometheus_series(r, filename, combine="max") for r in runs]
            series = [s for s in series if s]
            xs, lows, meds, highs = aggregate(series)
            if not xs:
                continue
            color = IMBALANCE_COLORS[color_key]
            ax.plot(xs, meds, label=label, color=color, linewidth=2.0)
            if len(series) > 1:
                ax.fill_between(xs, lows, highs, color=color, alpha=0.13, linewidth=0)
        ax.set_title(method_label(method))
        apply_common_axes(ax, None, x_max)
        ax.set_ylim(bottom=0)
        handles, labels = ax.get_legend_handles_labels()
        if not handles:
            ax.text(0.5, 0.5, "No compact Prometheus CSVs found", transform=ax.transAxes, ha="center", va="center", alpha=0.65)
    fig.suptitle(title, fontsize=13, y=0.985)
    finish_shared_axes(fig, axes, xlabel="Time since start (min)", ylabel=ylabel)
    add_shared_legend(fig, axes, ncol=2, y=0.965)
    fig.savefig(output, dpi=180)
    plt.close(fig)


def final_value(run: Run, key: str) -> float:
    if not run.summaries:
        return math.nan
    value = run.summaries[-1].get(key)
    try:
        return float(value)
    except (TypeError, ValueError):
        return math.nan


def valid_latency_rows(run: Run) -> list[dict]:
    return [r for r in run.summaries if float(r.get("window_latency_samples") or 0) > 0]


def time_to_condition(run: Run, predicate: Callable[[dict], bool], consecutive: int = 1) -> float:
    streak = 0
    first = math.nan
    for row in run.summaries:
        if predicate(row):
            if streak == 0:
                first = float(row.get("at_seconds") or math.nan)
            streak += 1
            if streak >= consecutive:
                return first
        else:
            streak = 0
            first = math.nan
    return math.nan


def run_metrics(run: Run) -> dict[str, float | str]:
    final_started = final_value(run, "started")
    final_completed = final_value(run, "completed")
    completion_ratio = final_completed / final_started if final_started > 0 else math.nan
    valid_lat = valid_latency_rows(run)
    p95_values = [float(r.get("window_latency_p95_ms")) for r in valid_lat if r.get("window_latency_p95_ms") is not None]
    completed_rps = [float(r.get("window_completed_rps") or 0) for r in run.summaries]
    started_rps = [float(r.get("window_started_rps") or 0) for r in run.summaries]
    incomplete = [float(r.get("tracked_incomplete") or 0) for r in run.summaries]
    time_backlog_1000 = time_to_condition(run, lambda r: float(r.get("tracked_incomplete") or 0) > 1000)
    time_rps_gap = time_to_condition(
        run,
        lambda r: float(r.get("window_started_rps") or 0) > 0
        and float(r.get("window_completed_rps") or 0) < 0.95 * float(r.get("window_started_rps") or 0),
        consecutive=3,
    )
    return {
        "method": run.method,
        "run": run.index,
        "directory": str(run.directory),
        "started": final_started,
        "completed": final_completed,
        "completion_ratio": completion_ratio,
        "final_incomplete": final_value(run, "tracked_incomplete"),
        "max_incomplete": max(incomplete) if incomplete else math.nan,
        "mean_started_rps": mean(started_rps),
        "mean_completed_rps": mean(completed_rps),
        "mean_p95_ms": mean(p95_values),
        "max_p95_ms": max(p95_values) if p95_values else math.nan,
        "frac_valid_windows_p95_gt_2s": mean([1.0 if v > 2000 else 0.0 for v in p95_values]),
        "time_to_backlog_gt_1000_s": time_backlog_1000,
        "time_to_completed_lt_95pct_started_s": time_rps_gap,
        "single_shard_moves_total": prometheus_counter_final_delta(run, "sd_load_based_single_moves_total.csv"),
        "swap_move_ops_total": prometheus_counter_final_delta(run, "sd_load_based_swap_moves_total.csv"),
        "load_based_shard_moves_total": prometheus_counter_final_delta(run, "sd_load_based_moves_total.csv"),
    }


def enrich_move_type_metrics(row: dict[str, float | str]) -> dict[str, float | str]:
    single = row.get("single_shard_moves_total", math.nan)
    swap_ops = row.get("swap_move_ops_total", math.nan)
    total_moves = row.get("load_based_shard_moves_total", math.nan)
    try:
        single_f = float(single)
        swap_f = float(swap_ops)
        total_f = float(total_moves)
    except (TypeError, ValueError):
        single_f = swap_f = total_f = math.nan
    shard_moves_from_swaps = 2.0 * swap_f if math.isfinite(swap_f) else math.nan
    denom = single_f + shard_moves_from_swaps if math.isfinite(single_f) and math.isfinite(shard_moves_from_swaps) else math.nan
    row["swap_shard_move_share"] = shard_moves_from_swaps / denom if math.isfinite(denom) and denom > 0 else math.nan
    if math.isfinite(total_f) and total_f > 0 and math.isfinite(single_f) and math.isfinite(swap_f):
        row["move_counter_consistency_gap"] = abs(total_f - (single_f + shard_moves_from_swaps))
    else:
        row["move_counter_consistency_gap"] = math.nan
    return row


def fmt(value: float | str, digits: int = 2) -> str:
    if isinstance(value, str):
        return value
    if value is None or math.isnan(float(value)):
        return ""
    return f"{float(value):.{digits}f}"


def write_summary_tables(output_dir: Path, runs: list[Run]):
    per_run = [enrich_move_type_metrics(run_metrics(run)) for run in runs]
    fieldnames = list(per_run[0].keys()) if per_run else []
    with (output_dir / "aggregate-summary-runs.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(per_run)

    aggregate_rows = []
    for method in METHOD_ORDER:
        rows = [r for r in per_run if r["method"] == method]
        if not rows:
            continue
        agg = {"method": method_label(method), "runs": len(rows)}
        for key in fieldnames:
            if key in {"method", "run", "directory"}:
                continue
            values = [float(r[key]) for r in rows if r[key] != "" and not math.isnan(float(r[key]))]
            agg[f"{key}_mean"] = mean(values)
            agg[f"{key}_stdev"] = stdev(values)
        aggregate_rows.append(agg)

    agg_fields = list(aggregate_rows[0].keys()) if aggregate_rows else []
    with (output_dir / "aggregate-summary.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=agg_fields)
        writer.writeheader()
        writer.writerows(aggregate_rows)

    tex_lines = [
        r"\begin{tabular}{lrrrrr}",
        r"\toprule",
        r"Method & Completion & Final backlog & Max backlog & Mean p95 (ms) & Time to backlog $>$1000 (min) \\",
        r"\midrule",
    ]
    for row in aggregate_rows:
        def meanpm(key: str, digits: int = 1) -> str:
            m = row.get(f"{key}_mean", math.nan)
            s = row.get(f"{key}_stdev", math.nan)
            if math.isnan(float(m)):
                return "--"
            if float(s) == 0:
                return fmt(float(m), digits)
            return f"{fmt(float(m), digits)} $\\pm$ {fmt(float(s), digits)}"

        completion = meanpm("completion_ratio", 3)
        final_backlog = meanpm("final_incomplete", 0)
        max_backlog = meanpm("max_incomplete", 0)
        p95 = meanpm("mean_p95_ms", 0)
        t_backlog = meanpm("time_to_backlog_gt_1000_s", 1)
        if t_backlog != "--":
            # Convert only the mean±stdev string is annoying; use simple mean here.
            m = row.get("time_to_backlog_gt_1000_s_mean", math.nan)
            s = row.get("time_to_backlog_gt_1000_s_stdev", math.nan)
            t_backlog = "--" if math.isnan(float(m)) else f"{float(m)/60:.1f} $\\pm$ {float(s)/60:.1f}"
        tex_lines.append(f"{row['method']} & {completion} & {final_backlog} & {max_backlog} & {p95} & {t_backlog} \\")
    tex_lines.extend([r"\bottomrule", r"\end{tabular}", ""])
    (output_dir / "aggregate-summary.tex").write_text("\n".join(tex_lines), encoding="utf-8")

    manifest = {
        "runs": [
            {
                "method": run.method,
                "label": method_label(run.method),
                "index": run.index,
                "directory": str(run.directory),
                "log": str(run.log_path),
                "csv": str(run.csv_path) if run.csv_path else None,
                "metadata": str(run.metadata_path) if run.metadata_path else None,
                "summary_rows": len(run.summaries),
                "cpu_rows": len(run.cpu_rows),
                "start_time": run.start_time.isoformat() if run.start_time else None,
            }
            for run in runs
        ]
    }
    (output_dir / "aggregate-manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("May21"), help="Root containing method/run folders.")
    parser.add_argument("--output-dir", type=Path, default=None)
    parser.add_argument("--x-max", type=float, default=3600, help="Maximum x-axis value in seconds.")
    parser.add_argument("--summary-step", type=float, default=10, help="Expected matching-lab summary interval in seconds.")
    parser.add_argument("--utilization-step", type=float, default=10, help="Deprecated; utilization plots use CSV timestamps.")
    parser.add_argument("--cpu-pod-y-max", type=float, default=2.2, help="Y-axis maximum for per-pod CPU plots.")
    parser.add_argument("--cpu-total-y-max", type=float, default=None, help="Y-axis maximum for total Matching CPU plot.")
    parser.add_argument("--throttling-y-max", type=float, default=None, help="Y-axis maximum for per-pod throttling plots.")
    parser.add_argument(
        "--method-dir",
        action="append",
        default=[],
        metavar="METHOD=DIR",
        help="Override discovery, e.g. --method-dir off=May21/Off. Can be repeated.",
    )
    args = parser.parse_args()

    root = args.root
    output_dir = args.output_dir or (root / "figures")
    output_dir.mkdir(parents=True, exist_ok=True)

    method_dirs: dict[str, Path] = {}
    for item in args.method_dir:
        if "=" not in item:
            parser.error("--method-dir must be METHOD=DIR")
        method, directory = item.split("=", 1)
        method = normalize(method)
        if method in {"cpu", "cpusecond", "cpuseconds", "cpusecondsgreedy"}:
            method = "cpuseconds"
        if method in {"latencyca", "latencycostaware"}:
            method = "latencycostaware"
        if method not in METHOD_LABELS:
            parser.error(f"unknown method {method!r}; expected one of {sorted(METHOD_LABELS)}")
        method_dirs[method] = Path(directory)

    runs = discover_runs(root, method_dirs)
    if not runs:
        raise SystemExit(f"no runs found below {root}")

    runs_by_method: dict[str, list[Run]] = defaultdict(list)
    for run in runs:
        runs_by_method[run.method].append(run)
    print("Discovered runs:")
    for method in METHOD_ORDER:
        rs = runs_by_method.get(method, [])
        if rs:
            print(f"  {method_label(method)}: {len(rs)}")
            for run in rs:
                print(f"    {run.index}: {run.directory.name} ({run.log_path.name})")

    plot_throughput(output_dir / "aggregate-throughput.png", runs_by_method, step=args.summary_step, x_max=args.x_max)
    plot_completed_cumulative(
        output_dir / "aggregate-completed-total.png",
        runs_by_method,
        step=args.summary_step,
        x_max=args.x_max,
    )
    plot_aggregate_metric(
        output_dir / "aggregate-completion-ratio.png",
        "Cumulative completion ratio",
        "Completed / started",
        {m: [ratio_series(r, step=args.summary_step) for r in rs] for m, rs in runs_by_method.items()},
        x_max=args.x_max,
        y_min=0,
        y_max=105,
        percent=True,
    )
    plot_aggregate_metric(
        output_dir / "aggregate-incomplete.png",
        "Tracked incomplete workflows",
        "Workflows",
        {m: [series_from_summaries(r, "tracked_incomplete", step=args.summary_step) for r in rs] for m, rs in runs_by_method.items()},
        x_max=args.x_max,
        y_min=0,
    )
    plot_aggregate_metric(
        output_dir / "aggregate-p95-latency.png",
        "P95 workflow latency",
        "P95 latency (ms)",
        {
            m: [
                series_from_summaries(
                    r,
                    "window_latency_p95_ms",
                    step=args.summary_step,
                    valid=lambda row: float(row.get("window_latency_samples") or 0) > 0,
                )
                for r in rs
            ]
            for m, rs in runs_by_method.items()
        },
        x_max=args.x_max,
        y_min=0,
    )
    plot_aggregate_metric(
        output_dir / "aggregate-cpu-total.png",
        "Total Matching CPU usage",
        "CPU cores",
        {m: [cpu_total_series(r, step=args.utilization_step) for r in rs] for m, rs in runs_by_method.items()},
        x_max=args.x_max,
        y_min=0,
        y_max=args.cpu_total_y_max,
    )
    plot_total_by_method(
        output_dir / "aggregate-cpu-total-by-method.png",
        runs_by_method,
        x_max=args.x_max,
        title="Total Matching CPU usage",
        ylabel="CPU cores",
        series_builder=lambda run: cpu_total_series(run, step=args.utilization_step),
        y_max=args.cpu_total_y_max,
    )
    plot_cpu_by_method(
        output_dir / "aggregate-cpu-by-method.png",
        runs_by_method,
        step=args.utilization_step,
        x_max=args.x_max,
        y_max=args.cpu_pod_y_max,
    )
    plot_throttling_by_method(
        output_dir / "aggregate-throttling-by-method.png",
        runs_by_method,
        x_max=args.x_max,
        y_max=args.throttling_y_max,
    )
    plot_throttling_total(output_dir / "aggregate-throttling-total.png", runs_by_method, x_max=args.x_max)
    plot_shard_moves_total(output_dir / "aggregate-shard-moves-total.png", runs_by_method, x_max=args.x_max)
    plot_shard_single_moves_total(output_dir / "aggregate-shard-single-moves-total.png", runs_by_method, x_max=args.x_max)
    plot_shard_swap_moves_total(output_dir / "aggregate-shard-swap-moves-total.png", runs_by_method, x_max=args.x_max)
    plot_shard_swap_fraction(output_dir / "aggregate-shard-swap-fraction.png", runs_by_method, x_max=args.x_max)
    plot_moved_shard_load(output_dir / "aggregate-moved-shard-load.png", runs_by_method, x_max=args.x_max)
    plot_moved_shard_load_total(output_dir / "aggregate-moved-shard-load-total.png", runs_by_method, x_max=args.x_max)
    plot_assignment_imbalance(output_dir / "aggregate-assignment-imbalance-cv.png", runs_by_method, x_max=args.x_max, kind="cv")
    plot_assignment_imbalance(
        output_dir / "aggregate-assignment-imbalance-max-over-mean.png",
        runs_by_method,
        x_max=args.x_max,
        kind="max_over_mean",
    )
    write_summary_tables(output_dir, runs)

    print(f"Wrote aggregate figures and tables to {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
