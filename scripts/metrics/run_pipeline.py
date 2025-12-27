#!/usr/bin/env python3
import argparse
import os
import subprocess
import sys


def parse_args():
    parser = argparse.ArgumentParser(
        description="Export metrics, render config, and plot figures for a run."
    )
    parser.add_argument("--run-id", required=True, help="Run identifier (e.g. runA)")
    parser.add_argument("--start", required=True, help="UTC start time (ISO-8601)")
    parser.add_argument("--end", required=True, help="UTC end time (ISO-8601)")
    parser.add_argument("--out-root", default="plots", help="Root output directory")
    parser.add_argument("--prom-url", default="http://localhost:9090")
    parser.add_argument("--namespace", default="shard_distributor_replay")
    parser.add_argument("--namespace-type", default="fixed")
    parser.add_argument("--operation", default="ShardAssignLoop")
    parser.add_argument("--step", default="60s")
    parser.add_argument("--window", default="1m")
    parser.add_argument("--title", default="")
    parser.add_argument("--format", default="png")
    parser.add_argument("--executors", default="")
    parser.add_argument("--replay-speed", default="")
    parser.add_argument("--replay-csv", default="")
    parser.add_argument("--overwrite", action="store_true")
    return parser.parse_args()


def ensure_run_dir(path, overwrite):
    if os.path.isdir(path):
        if overwrite:
            return
        if os.listdir(path):
            raise RuntimeError(
                f"output directory not empty: {path} (use --overwrite or new --run-id)"
            )
    os.makedirs(path, exist_ok=True)


def run(cmd):
    subprocess.run(cmd, check=True)


def main():
    args = parse_args()
    run_dir = os.path.join(args.out_root, args.run_id)
    ensure_run_dir(run_dir, args.overwrite)

    py = sys.executable
    export_cmd = [
        py,
        "scripts/metrics/export_prom_metrics.py",
        "--prom-url",
        args.prom_url,
        "--namespace",
        args.namespace,
        "--namespace-type",
        args.namespace_type,
        "--operation",
        args.operation,
        "--start",
        args.start,
        "--end",
        args.end,
        "--step",
        args.step,
        "--window",
        args.window,
        "--out-dir",
        run_dir,
    ]

    render_cmd = [
        py,
        "scripts/metrics/render_run_config.py",
        "--namespace",
        args.namespace.replace("_", "-"),
        "--out",
        os.path.join(run_dir, "run_config.tex"),
        "--json-out",
        os.path.join(run_dir, "run_config.json"),
        "--start",
        args.start,
        "--end",
        args.end,
    ]

    if args.executors:
        render_cmd.extend(["--executors", args.executors])
    if args.replay_speed:
        render_cmd.extend(["--replay-speed", args.replay_speed])
    if args.replay_csv:
        render_cmd.extend(["--replay-csv", args.replay_csv])

    title = args.title or args.run_id
    plot_cmd = [
        py,
        "scripts/metrics/plot_run.py",
        "--run-dir",
        run_dir,
        "--title",
        title,
        "--format",
        args.format,
    ]

    run(export_cmd)
    run(render_cmd)
    run(plot_cmd)
    print(f"done: {run_dir}")


if __name__ == "__main__":
    main()
