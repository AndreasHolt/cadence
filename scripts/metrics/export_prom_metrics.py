#!/usr/bin/env python3
import argparse
import csv
import datetime
import json
import os
import sys
import urllib.parse
import urllib.request


def parse_args():
    parser = argparse.ArgumentParser(
        description="Export shard distributor metrics from Prometheus to CSV files."
    )
    parser.add_argument("--prom-url", default="http://localhost:9090")
    parser.add_argument("--namespace", default="shard_distributor_replay")
    parser.add_argument("--namespace-type", default="fixed")
    parser.add_argument("--operation", default="ShardAssignLoop")
    parser.add_argument("--start", help="UTC start time (e.g. 2025-12-02T18:44:15Z)")
    parser.add_argument("--end", help="UTC end time (e.g. 2025-12-02T20:08:00Z)")
    parser.add_argument("--step", default="60s", help="Prometheus step (e.g. 60s)")
    parser.add_argument("--window", default="1m", help="Window for increase/max_over_time (e.g. 1m)")
    parser.add_argument("--out-dir", default="plots", help="Output directory for CSVs")
    return parser.parse_args()


def isoformat_utc(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def infer_time_range(start, end):
    if start and end:
        return start, end
    now = datetime.datetime.utcnow()
    end_time = now
    start_time = now - datetime.timedelta(hours=1)
    return isoformat_utc(start_time), isoformat_utc(end_time)


def query_range(prom_url, query, start, end, step):
    params = urllib.parse.urlencode(
        {"query": query, "start": start, "end": end, "step": step}
    )
    url = f"{prom_url}/api/v1/query_range?{params}"
    with urllib.request.urlopen(url) as resp:
        data = json.load(resp)
    if data.get("status") != "success":
        raise RuntimeError(f"query failed: {data}")
    return data["data"]["result"]


def write_csv(path, series):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "value"])
        if not series:
            return
        for ts, value in series:
            t = datetime.datetime.utcfromtimestamp(float(ts)).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            writer.writerow([t, value])


def main():
    args = parse_args()
    start, end = infer_time_range(args.start, args.end)

    labels = (
        f'namespace="{args.namespace}",'
        f'namespace_type="{args.namespace_type}",'
        f'operation="{args.operation}"'
    )

    window = args.window
    queries = {
        "smoothed_max_over_mean": f"shard_distributor_assignment_smoothed_load_max_over_mean{{{labels}}}",
        "reported_max_over_mean": f"shard_distributor_assignment_load_max_over_mean{{{labels}}}",
        "smoothed_cv": f"shard_distributor_assignment_smoothed_load_cv{{{labels}}}",
        "reported_cv": f"shard_distributor_assignment_load_cv{{{labels}}}",
        "moves_per_window": (
            f"increase(shard_distributor_load_balance_moves{{{labels}}}[{window}])"
        ),
        "cycles_per_window": (
            f"increase(shard_distributor_load_balance_cycles{{{labels}}}[{window}])"
        ),
        "avg_moves_per_cycle": (
            f"increase(shard_distributor_load_balance_moves{{{labels}}}[{window}])"
            f" / clamp_min(increase(shard_distributor_load_balance_cycles{{{labels}}}[{window}]), 1)"
        ),
        "sources_any": (
            f"max_over_time(shard_distributor_load_balance_source_executors_initial{{{labels}}}[{window}])"
        ),
        "destinations_any": (
            f"max_over_time(shard_distributor_load_balance_destination_executors_initial{{{labels}}}[{window}])"
        ),
        "stop_no_sources": (
            f"increase(shard_distributor_load_balance_stop_reason{{{labels},reason=\"no_sources\"}}[{window}])"
        ),
        "stop_no_eligible_shard": (
            f"increase(shard_distributor_load_balance_stop_reason{{{labels},reason=\"no_eligible_shard\"}}[{window}])"
        ),
        "stop_no_destinations": (
            f"increase(shard_distributor_load_balance_stop_reason{{{labels},reason=\"no_destinations_not_severe\"}}[{window}])"
        ),
        "missing_ratio": (
            f"shard_distributor_assignment_smoothed_load_missing_ratio{{{labels}}}"
        ),
        "stale_ratio": (
            f"shard_distributor_assignment_smoothed_load_stale_ratio{{{labels}}}"
        ),
        "active_shards": f"shard_distributor_active_shards{{{labels}}}",
        "active_executors": f"shard_distributor_active_executors{{{labels}}}",
    }

    errors = []
    for name, query in queries.items():
        try:
            result = query_range(args.prom_url, query, start, end, args.step)
            series = result[0]["values"] if result else []
            out_path = os.path.join(args.out_dir, f"{name}.csv")
            write_csv(out_path, series)
            print(f"wrote {out_path}")
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{name}: {exc}")

    if errors:
        for err in errors:
            print(f"error: {err}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
