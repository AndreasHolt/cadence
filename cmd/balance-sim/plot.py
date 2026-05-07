#!/usr/bin/env python3
"""Generate comparison plots from balance-sim CSV outputs.

Usage:
    python3 plot.py --run-dir <dir> [--out-dir <dir>] [--format png|pdf]

Produces:
    - imbalance.{fmt}    : max/mean + CV with greedy vs optimal
    - churn.{fmt}        : moves per window (greedy vs optimal)
    - gap.{fmt}          : optimality gap (reported - optimal)
"""

import argparse
import csv
import datetime
import os
import sys


def parse_args():
    parser = argparse.ArgumentParser(
        description="Plot balance-sim CSV outputs."
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


def plot_imbalance(run_dir, out_dir, fmt, title_prefix, x_axis):
    """Plot max/mean and CV with smoothed, reported, and optimal lines."""
    smoothed_mm = load_series(os.path.join(run_dir, "smoothed_max_over_mean.csv"))
    reported_mm = load_series(os.path.join(run_dir, "reported_max_over_mean.csv"))
    optimal_mm = load_series(os.path.join(run_dir, "optimal_max_over_mean.csv"))
    smoothed_cv = load_series(os.path.join(run_dir, "smoothed_cv.csv"))
    reported_cv = load_series(os.path.join(run_dir, "reported_cv.csv"))
    optimal_cv = load_series(os.path.join(run_dir, "optimal_cv.csv"))

    if not any([smoothed_mm, reported_mm, optimal_mm, smoothed_cv, reported_cv, optimal_cv]):
        print("warn: no imbalance series found", file=sys.stderr)
        return

    try:
        import matplotlib.pyplot as plt
    except ImportError:
        print("error: matplotlib not installed (pip install matplotlib)", file=sys.stderr)
        sys.exit(1)

    fig, axes = plt.subplots(2, 1, figsize=(10, 6), sharex=True)
    base_time = base_time_for([smoothed_mm, reported_mm, optimal_mm, smoothed_cv, reported_cv, optimal_cv])
    if base_time is None:
        print("warn: empty series for imbalance plot", file=sys.stderr)
        return

    # Max/Mean
    for series, label, color, style in [
        (smoothed_mm, "smoothed", "#2ca02c", "-"),
        (reported_mm, "reported", "#f2c84b", "-"),
        (optimal_mm, "optimal", "#1f77b4", "--"),
    ]:
        xs, ys = split_xy(series, base_time, x_axis)
        if xs:
            axes[0].plot(xs, ys, label=label, color=color, linestyle=style, linewidth=1.5)
    axes[0].set_title("Imbalance (Max/Mean)")
    axes[0].set_ylabel("max / mean")
    axes[0].grid(True, alpha=0.3)
    axes[0].legend(loc="upper right")
    axes[0].axhline(y=1.0, color="gray", linestyle=":", linewidth=0.8, alpha=0.5)

    # CV
    for series, label, color, style in [
        (smoothed_cv, "smoothed", "#2ca02c", "-"),
        (reported_cv, "reported", "#f2c84b", "-"),
        (optimal_cv, "optimal", "#1f77b4", "--"),
    ]:
        xs, ys = split_xy(series, base_time, x_axis)
        if xs:
            axes[1].plot(xs, ys, label=label, color=color, linestyle=style, linewidth=1.5)
    axes[1].set_title("Imbalance (CV)")
    axes[1].set_ylabel("coefficient of variation")
    axes[1].grid(True, alpha=0.3)
    axes[1].legend(loc="upper right")
    axes[1].set_xlabel("minutes since start" if x_axis == "elapsed" else "timestamp")

    if title_prefix:
        fig.suptitle(title_prefix)
    fig.tight_layout()
    ensure_out_dir(out_dir)
    out_path = os.path.join(out_dir, f"imbalance.{fmt}")
    fig.savefig(out_path, dpi=160)
    plt.close(fig)
    print(f"wrote {out_path}")


def plot_churn(run_dir, out_dir, fmt, title_prefix, x_axis):
    """Plot moves per window: greedy vs optimal."""
    moves = load_series(os.path.join(run_dir, "moves_per_window.csv"))
    opt_moves = load_series(os.path.join(run_dir, "optimal_moves.csv"))

    if not any([moves, opt_moves]):
        print("warn: no churn series found", file=sys.stderr)
        return

    try:
        import matplotlib.pyplot as plt
    except ImportError:
        print("error: matplotlib not installed", file=sys.stderr)
        sys.exit(1)

    fig, axes = plt.subplots(2, 1, figsize=(10, 6), sharex=True)
    base_time = base_time_for([moves, opt_moves])
    if base_time is None:
        print("warn: empty series for churn plot", file=sys.stderr)
        return

    # Greedy moves
    xs, ys = split_xy(moves, base_time, x_axis)
    if xs:
        axes[0].plot(xs, ys, color="#1f77b4", linewidth=1.5, label="greedy")
    axes[0].set_title("Greedy Moves per Window")
    axes[0].grid(True, alpha=0.3)
    axes[0].legend(loc="upper right")

    # Optimal moves
    xs, ys = split_xy(opt_moves, base_time, x_axis)
    if xs:
        axes[1].plot(xs, ys, color="#9467bd", linewidth=1.5, label="optimal")
    axes[1].set_title("Optimal Moves per Window")
    axes[1].grid(True, alpha=0.3)
    axes[1].legend(loc="upper right")
    axes[1].set_xlabel("minutes since start" if x_axis == "elapsed" else "timestamp")

    if title_prefix:
        fig.suptitle(title_prefix)
    fig.tight_layout()
    ensure_out_dir(out_dir)
    out_path = os.path.join(out_dir, f"churn.{fmt}")
    fig.savefig(out_path, dpi=160)
    plt.close(fig)
    print(f"wrote {out_path}")


def plot_gap(run_dir, out_dir, fmt, title_prefix, x_axis):
    """Plot the gap between greedy (reported) and optimal."""
    reported_mm = load_series(os.path.join(run_dir, "reported_max_over_mean.csv"))
    optimal_mm = load_series(os.path.join(run_dir, "optimal_max_over_mean.csv"))
    reported_cv = load_series(os.path.join(run_dir, "reported_cv.csv"))
    optimal_cv = load_series(os.path.join(run_dir, "optimal_cv.csv"))

    if not any([reported_mm, optimal_mm, reported_cv, optimal_cv]):
        print("warn: no gap series found", file=sys.stderr)
        return

    try:
        import matplotlib.pyplot as plt
    except ImportError:
        print("error: matplotlib not installed", file=sys.stderr)
        sys.exit(1)

    def compute_gap(reported, optimal):
        """Return list of (timestamp, reported - optimal)."""
        opt_dict = {dt: val for dt, val in optimal}
        return [(dt, val - opt_dict.get(dt, val)) for dt, val in reported if dt in opt_dict]

    mm_gap = compute_gap(reported_mm, optimal_mm)
    cv_gap = compute_gap(reported_cv, optimal_cv)

    fig, axes = plt.subplots(2, 1, figsize=(10, 6), sharex=True)
    base_time = base_time_for([mm_gap, cv_gap])
    if base_time is None:
        print("warn: empty series for gap plot", file=sys.stderr)
        return

    xs, ys = split_xy(mm_gap, base_time, x_axis)
    if xs:
        axes[0].plot(xs, ys, color="#d62728", linewidth=1.5)
    axes[0].set_title("Optimality Gap (Max/Mean)")
    axes[0].set_ylabel("reported - optimal")
    axes[0].grid(True, alpha=0.3)
    axes[0].axhline(y=0.0, color="gray", linestyle=":", linewidth=0.8, alpha=0.5)

    xs, ys = split_xy(cv_gap, base_time, x_axis)
    if xs:
        axes[1].plot(xs, ys, color="#d62728", linewidth=1.5)
    axes[1].set_title("Optimality Gap (CV)")
    axes[1].set_ylabel("reported - optimal")
    axes[1].grid(True, alpha=0.3)
    axes[1].set_xlabel("minutes since start" if x_axis == "elapsed" else "timestamp")
    axes[1].axhline(y=0.0, color="gray", linestyle=":", linewidth=0.8, alpha=0.5)

    if title_prefix:
        fig.suptitle(title_prefix)
    fig.tight_layout()
    ensure_out_dir(out_dir)
    out_path = os.path.join(out_dir, f"gap.{fmt}")
    fig.savefig(out_path, dpi=160)
    plt.close(fig)
    print(f"wrote {out_path}")


def main():
    args = parse_args()
    run_dir = args.run_dir
    out_dir = args.out_dir or run_dir
    title_prefix = args.title
    plot_imbalance(run_dir, out_dir, args.format, title_prefix, args.x_axis)
    plot_churn(run_dir, out_dir, args.format, title_prefix, args.x_axis)
    plot_gap(run_dir, out_dir, args.format, title_prefix, args.x_axis)


if __name__ == "__main__":
    main()
