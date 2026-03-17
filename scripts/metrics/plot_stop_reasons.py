#!/usr/bin/env python3
import argparse
import csv
import os
from datetime import datetime
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser(description="Plot shard distributor stop reasons.")
    parser.add_argument("--run-dir", required=True, help="Run directory with CSVs")
    parser.add_argument(
        "--out",
        default="stop_reasons.png",
        help="Output filename (relative to run dir)",
    )
    parser.add_argument(
        "--title",
        default="Run D (Only Tick, No Benefit Gating): Stop Reasons",
        help="Plot title",
    )
    parser.add_argument(
        "--minutes",
        type=float,
        default=180,
        help="X-axis range in minutes (default: 180)",
    )
    return parser.parse_args()


def load_series(path):
    xs = []
    ys = []
    with path.open() as f:
        rows = [r for r in csv.reader(f) if r]
    if rows and rows[0][0] == "timestamp":
        rows = rows[1:]
    for ts, val in rows:
        xs.append(datetime.strptime(ts, "%Y-%m-%d %H:%M:%S"))
        ys.append(float(val))
    return xs, ys


def main():
    args = parse_args()
    run_dir = Path(args.run_dir)

    # Keep matplotlib cache inside the run directory to avoid permission issues.
    mpl_config = run_dir / ".mplconfig"
    mpl_config.mkdir(parents=True, exist_ok=True)
    os.environ.setdefault("MPLCONFIGDIR", str(mpl_config))

    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    series = [
        ("stop_no_sources.csv", "No sources (within band)"),
        ("stop_no_eligible_shard.csv", "Cooldown/blocked"),
        ("stop_no_destinations.csv", "No destinations (not severe)"),
    ]

    plt.figure(figsize=(10, 4))
    start_time = None
    for filename, label in series:
        path = run_dir / filename
        if not path.exists():
            continue
        x, y = load_series(path)
        if not x:
            continue
        if start_time is None:
            start_time = x[0]
        minutes = [(ts - start_time).total_seconds() / 60.0 for ts in x]
        plt.plot(minutes, y, label=label, linewidth=1.6)

    plt.title(args.title)
    plt.ylabel("stops per minute")
    plt.xlabel("minutes since start")
    if args.minutes > 0:
        plt.xlim(0, args.minutes)
    plt.legend(frameon=False, ncol=1)
    plt.tight_layout()

    out_path = run_dir / args.out
    plt.savefig(out_path, dpi=160)
    print(f"wrote {out_path}")


if __name__ == "__main__":
    main()
