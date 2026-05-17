#!/usr/bin/env python3
"""Generate focused imbalance plots from balance-sim CSV outputs.

Usage:
    python3 plot_balance_focus.py --run-dir <dir> [--out-dir <dir>] [--format png|pdf]

Produces:
    - imbalance_focus.{fmt} : max/mean with greedy vs optimal (first 60 min only)

The smoothed line is drawn in a faint color to avoid overwhelming the plot.
"""

import argparse
import csv
import datetime
import os
import sys


def parse_args():
    parser = argparse.ArgumentParser(
        description="Plot balance-sim CSV outputs (first 60 min, max/mean only)."
    )
    parser.add_argument("--run-dir", required=True, help="Directory with CSVs")
    parser.add_argument(
        "--out-dir", help="Output directory (defaults to run-dir)"
    )
    parser.add_argument("--format", default="png", help="Output format (png/pdf)")
    parser.add_argument("--title", help="Optional title prefix")
    parser.add_argument(
        "--x-axis",
        choices=("elapsed", "timestamp"),
        default="elapsed",
        help="X-axis mode",
    )
    return parser.parse_args()


def load_series(path):
    if not os.path.exists(path):
        return []
    rows = []
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ts = row.get("timestamp", "").strip()
            val = row.get("value", "").strip()
            if not ts or not val or val.lower() == "nan":
                continue
            try:
                dt = datetime.datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                continue
            try:
                value = float(val)
            except ValueError:
                continue
            rows.append((dt, value))
    return rows


def split_xy(series, base_time, x_axis):
    if not series:
        return [], []
    xs, ys = zip(*series)
    if x_axis == "timestamp":
        return list(xs), list(ys)
    return [((x - base_time).total_seconds() / 60.0) for x in xs], list(ys)


def base_time_for(series_list):
    times = [s[0][0] for s in series_list if s]
    if not times:
        return None
    return min(times)


def ensure_out_dir(path):
    os.makedirs(path, exist_ok=True)


def plot_imbalance_focus(run_dir, out_dir, fmt, title_prefix, x_axis):
    """Plot max/mean with smoothed, reported, and optimal lines (first 60 min)."""
    smoothed_mm = load_series(os.path.join(run_dir, "smoothed_max_over_mean.csv"))
    reported_mm = load_series(os.path.join(run_dir, "reported_max_over_mean.csv"))
    optimal_mm = load_series(os.path.join(run_dir, "optimal_max_over_mean.csv"))

    if not any([smoothed_mm, reported_mm, optimal_mm]):
        print("warn: no imbalance series found", file=sys.stderr)
        return

    try:
        import matplotlib.pyplot as plt
    except ImportError:
        print("error: matplotlib not installed (pip install matplotlib)", file=sys.stderr)
        sys.exit(1)

    fig, ax = plt.subplots(figsize=(10, 4))
    base_time = base_time_for([smoothed_mm, reported_mm, optimal_mm])
    if base_time is None:
        print("warn: empty series for imbalance plot", file=sys.stderr)
        return

    def filter_first_60(series):
        if not series:
            return []
        if x_axis == "timestamp":
            cutoff = base_time + datetime.timedelta(minutes=60)
            return [(dt, val) for dt, val in series if dt <= cutoff]
        else:
            return [(dt, val) for dt, val in series if ((dt - base_time).total_seconds() / 60.0) <= 60]

    # Max/Mean
    for series, label, color, style, alpha in [
        (smoothed_mm, "smoothed", "#2ca02c", "-", 0.35),
        (reported_mm, "reported", "#f2c84b", "-", 1.0),
        (optimal_mm, "optimal", "#1f77b4", "--", 1.0),
    ]:
        filtered = filter_first_60(series)
        xs, ys = split_xy(filtered, base_time, x_axis)
        if xs:
            ax.plot(xs, ys, label=label, color=color, linestyle=style, linewidth=1.5, alpha=alpha)
    ax.set_title("Imbalance (Max/Mean)")
    ax.set_ylabel("max / mean")
    ax.set_xlabel("minutes since start" if x_axis == "elapsed" else "timestamp")
    ax.grid(True, alpha=0.3)
    ax.legend(loc="upper right")
    ax.axhline(y=1.0, color="gray", linestyle=":", linewidth=0.8, alpha=0.5)

    if title_prefix:
        fig.suptitle(title_prefix)
    fig.tight_layout()
    ensure_out_dir(out_dir)
    out_path = os.path.join(out_dir, f"imbalance_focus.{fmt}")
    fig.savefig(out_path, dpi=160)
    plt.close(fig)
    print(f"wrote {out_path}")


def main():
    args = parse_args()
    run_dir = args.run_dir
    out_dir = args.out_dir or run_dir
    title_prefix = args.title
    plot_imbalance_focus(run_dir, out_dir, args.format, title_prefix, args.x_axis)


if __name__ == "__main__":
    main()
