#!/usr/bin/env python3
import argparse
import csv
import json
import math
import os
import re
import statistics
import sys
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path


DEFAULT_PROMETHEUS_URL = "http://localhost:9090"


@dataclass(frozen=True)
class QuerySpec:
    name: str
    expr: str
    kind: str


DEFAULT_QUERIES = [
    QuerySpec(
        "sd_load_based_moves_total",
        "shard_distributor_shard_assign_load_based_moves",
        "counter",
    ),
    QuerySpec(
        "sd_load_based_moves_rate_1m",
        "rate(shard_distributor_shard_assign_load_based_moves[1m])",
        "rate",
    ),
    QuerySpec(
        "sd_moved_shard_load",
        "shard_distributor_shard_assign_moved_shard_load",
        "gauge",
    ),
    QuerySpec(
        "sd_moved_shard_load_total",
        "shard_distributor_shard_assign_moved_shard_load_total",
        "counter",
    ),
    QuerySpec(
        "sd_assignment_load_max_over_mean",
        "shard_distributor_assignment_load_max_over_mean",
        "gauge",
    ),
    QuerySpec(
        "sd_assignment_load_cv",
        "shard_distributor_assignment_load_cv",
        "gauge",
    ),
    QuerySpec(
        "sd_assignment_smoothed_load_max_over_mean",
        "shard_distributor_assignment_smoothed_load_max_over_mean",
        "gauge",
    ),
    QuerySpec(
        "sd_assignment_smoothed_load_cv",
        "shard_distributor_assignment_smoothed_load_cv",
        "gauge",
    ),
    QuerySpec(
        "sd_assignment_smoothed_load_missing_ratio",
        "shard_distributor_assignment_smoothed_load_missing_ratio",
        "gauge",
    ),
    QuerySpec(
        "sd_assignment_smoothed_load_stale_ratio",
        "shard_distributor_assignment_smoothed_load_stale_ratio",
        "gauge",
    ),
    QuerySpec(
        "sd_total_executors",
        "shard_distributor_total_executors",
        "gauge",
    ),
    QuerySpec(
        "sd_active_shards",
        "shard_distributor_active_shards",
        "gauge",
    ),
    QuerySpec(
        "sd_oldest_executor_heartbeat_lag",
        "shard_distributor_oldest_executor_heartbeat_lag",
        "gauge",
    ),
    QuerySpec(
        "sd_executor_owned_shards",
        "shard_distributor_executor_owned_shards",
        "gauge",
    ),
    QuerySpec(
        "sd_executor_heartbeat_skipped_total",
        "shard_distributor_executor_heartbeat_skipped",
        "counter",
    ),
    QuerySpec(
        "sd_executor_shards_started_total",
        "shard_distributor_executor_shards_started",
        "counter",
    ),
    QuerySpec(
        "sd_executor_shards_stopped_total",
        "shard_distributor_executor_shards_stopped",
        "counter",
    ),
    QuerySpec(
        "sd_executor_processor_creation_failures_total",
        "shard_distributor_executor_processor_creation_failures",
        "counter",
    ),
    QuerySpec(
        "sd_executor_processor_start_timeout_total",
        "shard_distributor_executor_processor_start_timeout",
        "counter",
    ),
    QuerySpec(
        "sd_executor_processor_stop_timeout_total",
        "shard_distributor_executor_processor_stop_timeout",
        "counter",
    ),
    QuerySpec(
        "sd_executor_client_requests_total",
        "shard_distributor_executor_client_requests",
        "counter",
    ),
    QuerySpec(
        "sd_executor_client_failures_total",
        "shard_distributor_executor_client_failures",
        "counter",
    ),
    QuerySpec(
        "sd_store_requests_total",
        "shard_distributor_store_requests_per_namespace",
        "counter",
    ),
    QuerySpec(
        "sd_store_failures_total",
        "shard_distributor_store_failures_per_namespace",
        "counter",
    ),
    QuerySpec(
        "sd_watch_events_received_total",
        "shard_distributor_watch_events_received",
        "counter",
    ),
    QuerySpec(
        "sd_is_leader",
        "shard_distributor_is_leader",
        "gauge",
    ),
    QuerySpec(
        "matching_gomaxprocs",
        'gomaxprocs{cadence_service="cadence_matching"}',
        "gauge",
    ),
    QuerySpec(
        "matching_addtask_request_rate_1m",
        'rate(cadence_requests{cadence_service="cadence_matching",operation="AddTask"}[1m])',
        "rate",
    ),
    QuerySpec(
        "matching_addtask_error_rate_1m",
        'rate(cadence_errors{cadence_service="cadence_matching",operation="AddTask"}[1m])',
        "rate",
    ),
    QuerySpec(
        "matching_task_backlog",
        "task_backlog_per_tl",
        "gauge",
    ),
    QuerySpec(
        "matching_task_lag",
        "task_lag_per_tl",
        "gauge",
    ),
    QuerySpec(
        "matching_estimated_add_task_qps",
        "estimated_add_task_qps_per_tl",
        "gauge",
    ),
    QuerySpec(
        "matching_addtask_p95_latency_ns_by_instance",
        'histogram_quantile(0.95, sum by (le, instance, operation) (rate(cadence_latency_ns_bucket{cadence_service="cadence_matching",operation="AddTask"}[1m])))',
        "histogram_quantile",
    ),
    QuerySpec(
        "matching_service_p95_latency_ns_by_operation",
        'histogram_quantile(0.95, sum by (le, cadence_service, operation) (rate(cadence_latency_ns_bucket{cadence_service=~"cadence_matching|cadence_frontend|cadence_history"}[1m])))',
        "histogram_quantile",
    ),
    QuerySpec(
        "sd_service_p95_latency_ns_by_operation",
        "histogram_quantile(0.95, sum by (le, operation) (rate(shard_distributor_latency_ns_bucket[1m])))",
        "histogram_quantile",
    ),
    QuerySpec(
        "sd_shard_assignment_distribution_p95_latency",
        "histogram_quantile(0.95, sum by (le, operation) (rate(shard_distributor_shard_assignment_distribution_latency_bucket[1m])))",
        "histogram_quantile",
    ),
    QuerySpec(
        "sd_shard_handover_p95_latency",
        "histogram_quantile(0.95, sum by (le, operation) (rate(shard_distributor_shard_handover_latency_bucket[1m])))",
        "histogram_quantile",
    ),
    QuerySpec(
        "sd_watch_processing_p95_latency",
        "histogram_quantile(0.95, sum by (le, operation) (rate(shard_distributor_watch_processing_latency_bucket[1m])))",
        "histogram_quantile",
    ),
]


WIDE_QUERIES = [
    QuerySpec(
        "all_shard_distributor_metrics",
        '{__name__=~"shard_distributor_.*|shard_distrubutor_.*"}',
        "wide",
    ),
    QuerySpec(
        "all_matching_service_metrics",
        '{cadence_service="cadence_matching"}',
        "wide",
    ),
]


def safe_filename(value):
    return re.sub(r"[^A-Za-z0-9_.-]+", "-", value).strip("-")


def parse_time(value):
    if value is None:
        return None
    try:
        return datetime.fromtimestamp(float(value), tz=timezone.utc)
    except ValueError:
        pass

    normalized = value.strip().replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def infer_window_from_utilization_csv(path):
    timestamps = []
    with path.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            timestamps.append(parse_time(row["timestamp"]))

    if not timestamps:
        raise ValueError(f"{path} contains no utilization samples")

    return min(timestamps), max(timestamps)


def infer_duration_from_matching_log(path):
    max_seconds = None
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line.startswith("summary_json:"):
                continue
            payload = json.loads(line.removeprefix("summary_json:").strip())
            at_seconds = payload.get("at_seconds")
            if at_seconds is not None:
                max_seconds = max(float(at_seconds), max_seconds or 0.0)

    if max_seconds is None:
        raise ValueError(f"{path} contains no summary_json at_seconds values")
    return max_seconds


def resolve_window(args):
    start = parse_time(args.start)
    end = parse_time(args.end)

    if args.utilization_csv:
        inferred_start, inferred_end = infer_window_from_utilization_csv(args.utilization_csv)
        start = start or inferred_start
        end = end or inferred_end

    if start is not None and end is None and args.matching_log:
        end = start + timedelta(seconds=infer_duration_from_matching_log(args.matching_log))

    if start is None or end is None:
        raise ValueError(
            "run window is required: pass --utilization-csv, or pass --start and --end, "
            "or pass --start with --matching-log"
        )

    if end <= start:
        raise ValueError(f"end must be after start, got start={start.isoformat()} end={end.isoformat()}")

    padding = timedelta(seconds=args.pad_seconds)
    return start - padding, end + padding


def prometheus_query_range(prometheus_url, query, start, end, step, timeout):
    base = prometheus_url.rstrip("/")
    params = urllib.parse.urlencode(
        {
            "query": query,
            "start": f"{start.timestamp():.3f}",
            "end": f"{end.timestamp():.3f}",
            "step": step,
        }
    )
    url = f"{base}/api/v1/query_range?{params}"
    request = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        payload = json.loads(response.read().decode("utf-8"))

    if payload.get("status") != "success":
        error_type = payload.get("errorType", "unknown")
        error = payload.get("error", "unknown Prometheus error")
        raise RuntimeError(f"{error_type}: {error}")

    return payload


def finite_float(value):
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed):
        return None
    return parsed


def quantile(sorted_values, q):
    if not sorted_values:
        return None
    if len(sorted_values) == 1:
        return sorted_values[0]
    pos = (len(sorted_values) - 1) * q
    lower = math.floor(pos)
    upper = math.ceil(pos)
    if lower == upper:
        return sorted_values[int(pos)]
    weight = pos - lower
    return sorted_values[lower] * (1 - weight) + sorted_values[upper] * weight


def monotonic_delta(points):
    if len(points) < 2:
        return 0.0

    total = 0.0
    previous = points[0][1]
    for _, value in points[1:]:
        diff = value - previous
        if diff >= 0:
            total += diff
        else:
            total += value
        previous = value
    return total


def max_positive_rate(points):
    max_rate = 0.0
    for (prev_ts, prev_value), (ts, value) in zip(points, points[1:]):
        elapsed = ts - prev_ts
        if elapsed <= 0:
            continue
        diff = value - prev_value
        if diff < 0:
            diff = value
        max_rate = max(max_rate, diff / elapsed)
    return max_rate


def metric_name(metric):
    return metric.get("__name__", "")


def labels_json(metric):
    return json.dumps(dict(sorted(metric.items())), sort_keys=True, separators=(",", ":"))


def summarize_series(query_spec, result):
    points = []
    for raw_ts, raw_value in result.get("values", []):
        value = finite_float(raw_value)
        if value is None:
            continue
        points.append((float(raw_ts), value))

    if not points:
        return None

    values = [value for _, value in points]
    sorted_values = sorted(values)
    mean = statistics.fmean(values)
    stddev = statistics.pstdev(values) if len(values) > 1 else 0.0
    duration = points[-1][0] - points[0][0]
    raw_delta = values[-1] - values[0]
    counter_delta = monotonic_delta(points)
    mean_for_ratio = mean if mean != 0 else None

    return {
        "query": query_spec.name,
        "kind": query_spec.kind,
        "metric": metric_name(result.get("metric", {})),
        "labels": labels_json(result.get("metric", {})),
        "samples": len(points),
        "first_timestamp": datetime.fromtimestamp(points[0][0], tz=timezone.utc).isoformat(),
        "last_timestamp": datetime.fromtimestamp(points[-1][0], tz=timezone.utc).isoformat(),
        "first_value": values[0],
        "last_value": values[-1],
        "min": min(values),
        "max": max(values),
        "mean": mean,
        "stddev": stddev,
        "cv": (stddev / mean_for_ratio) if mean_for_ratio is not None else "",
        "temporal_max_over_mean": (max(values) / mean_for_ratio) if mean_for_ratio is not None else "",
        "p50": quantile(sorted_values, 0.50),
        "p95": quantile(sorted_values, 0.95),
        "p99": quantile(sorted_values, 0.99),
        "raw_delta": raw_delta,
        "counter_delta": counter_delta,
        "avg_counter_rate_per_sec": (counter_delta / duration) if duration > 0 else "",
        "max_counter_rate_per_sec": max_positive_rate(points),
    }


def write_series_csv(path, payload):
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["timestamp", "value", "metric", "labels"])
        for result in payload.get("data", {}).get("result", []):
            metric = result.get("metric", {})
            labels = labels_json(metric)
            name = metric_name(metric)
            for raw_ts, raw_value in result.get("values", []):
                value = finite_float(raw_value)
                if value is None:
                    continue
                timestamp = datetime.fromtimestamp(float(raw_ts), tz=timezone.utc).isoformat()
                writer.writerow([timestamp, value, name, labels])


def write_summary_csv(path, summaries):
    fieldnames = [
        "query",
        "kind",
        "metric",
        "labels",
        "samples",
        "first_timestamp",
        "last_timestamp",
        "first_value",
        "last_value",
        "min",
        "max",
        "mean",
        "stddev",
        "cv",
        "temporal_max_over_mean",
        "p50",
        "p95",
        "p99",
        "raw_delta",
        "counter_delta",
        "avg_counter_rate_per_sec",
        "max_counter_rate_per_sec",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for summary in summaries:
            writer.writerow(summary)


def parse_extra_query(value):
    if "=" not in value:
        raise argparse.ArgumentTypeError("extra query must be NAME=EXPR")
    name, expr = value.split("=", 1)
    name = name.strip()
    expr = expr.strip()
    if not name or not expr:
        raise argparse.ArgumentTypeError("extra query name and expression must not be empty")
    return QuerySpec(name, expr, "extra")


def main():
    parser = argparse.ArgumentParser(
        description="Collect important Prometheus time series for a completed kind-lab run."
    )
    parser.add_argument("--run", required=True, help="Run label used in output filenames.")
    parser.add_argument(
        "--prometheus-url",
        default=os.environ.get("PROMETHEUS_URL", DEFAULT_PROMETHEUS_URL),
        help="Prometheus base URL. Defaults to PROMETHEUS_URL or http://localhost:9090.",
    )
    parser.add_argument(
        "--utilization-csv",
        type=Path,
        help="sample-utilization.sh CSV used to infer the Prometheus query window.",
    )
    parser.add_argument(
        "--matching-log",
        type=Path,
        help="matching-lab log. Used with --start when no --end is provided.",
    )
    parser.add_argument("--start", help="Run start time as Unix seconds or ISO-8601.")
    parser.add_argument("--end", help="Run end time as Unix seconds or ISO-8601.")
    parser.add_argument(
        "--pad-seconds",
        type=float,
        default=30.0,
        help="Seconds to include before and after the inferred run window.",
    )
    parser.add_argument("--step", default="15s", help="Prometheus range-query step.")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("environment/kind-lab/results/prometheus"),
        help="Directory for Prometheus exports.",
    )
    parser.add_argument(
        "--wide",
        action="store_true",
        help="Also export broad shard-distributor and matching metric selectors.",
    )
    parser.add_argument(
        "--query",
        action="append",
        type=parse_extra_query,
        default=[],
        help="Additional query as NAME=EXPR. Can be provided multiple times.",
    )
    parser.add_argument("--timeout", type=float, default=30.0, help="HTTP timeout in seconds.")
    args = parser.parse_args()

    start, end = resolve_window(args)
    query_specs = list(DEFAULT_QUERIES)
    if args.wide:
        query_specs.extend(WIDE_QUERIES)
    query_specs.extend(args.query)

    run_dir = args.output_dir / safe_filename(args.run)
    raw_dir = run_dir / "raw"
    csv_dir = run_dir / "csv"
    raw_dir.mkdir(parents=True, exist_ok=True)
    csv_dir.mkdir(parents=True, exist_ok=True)

    metadata = {
        "run": args.run,
        "prometheus_url": args.prometheus_url,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "step": args.step,
        "pad_seconds": args.pad_seconds,
        "queries": [
            {"name": spec.name, "expr": spec.expr, "kind": spec.kind}
            for spec in query_specs
        ],
    }

    summaries = []
    errors = []
    for spec in query_specs:
        print(f"querying {spec.name}", file=sys.stderr)
        try:
            payload = prometheus_query_range(
                args.prometheus_url,
                spec.expr,
                start,
                end,
                args.step,
                args.timeout,
            )
        except Exception as err:
            errors.append({"query": spec.name, "expr": spec.expr, "error": str(err)})
            print(f"warning: {spec.name}: {err}", file=sys.stderr)
            continue

        raw_path = raw_dir / f"{safe_filename(spec.name)}.json"
        csv_path = csv_dir / f"{safe_filename(spec.name)}.csv"
        with raw_path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
        write_series_csv(csv_path, payload)

        for result in payload.get("data", {}).get("result", []):
            summary = summarize_series(spec, result)
            if summary is not None:
                summaries.append(summary)

    metadata["errors"] = errors
    with (run_dir / "metadata.json").open("w", encoding="utf-8") as handle:
        json.dump(metadata, handle, indent=2, sort_keys=True)
        handle.write("\n")

    with (run_dir / "summary.json").open("w", encoding="utf-8") as handle:
        json.dump(summaries, handle, indent=2, sort_keys=True)
        handle.write("\n")
    write_summary_csv(run_dir / "summary.csv", summaries)

    print(f"wrote {run_dir / 'metadata.json'}")
    print(f"wrote {run_dir / 'summary.json'}")
    print(f"wrote {run_dir / 'summary.csv'}")
    if errors:
        print(f"completed with {len(errors)} query errors; see metadata.json", file=sys.stderr)


if __name__ == "__main__":
    main()
