#!/usr/bin/env python3
import argparse
import csv
import datetime
import os
import sys


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate per-run plots from exported CSV metrics."
    )
    parser.add_argument("--run-dir", required=True, help="Directory with CSVs")
    parser.add_argument(
        "--out-dir",
        help="Output directory for figures (defaults to run-dir)",
    )
    parser.add_argument("--format", default="png", help="Output format (png/pdf)")
    parser.add_argument("--title", help="Optional title prefix for figures")
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


def split_xy(series):
    if not series:
        return [], []
    xs, ys = zip(*series)
    return list(xs), list(ys)


def ensure_out_dir(path):
    os.makedirs(path, exist_ok=True)


def plot_imbalance(run_dir, out_dir, fmt, title_prefix):
    smoothed_mm = load_series(os.path.join(run_dir, "smoothed_max_over_mean.csv"))
    reported_mm = load_series(os.path.join(run_dir, "reported_max_over_mean.csv"))
    smoothed_cv = load_series(os.path.join(run_dir, "smoothed_cv.csv"))
    reported_cv = load_series(os.path.join(run_dir, "reported_cv.csv"))

    if not any([smoothed_mm, reported_mm, smoothed_cv, reported_cv]):
        print("warn: no imbalance series found", file=sys.stderr)
        return

    try:
        import matplotlib.pyplot as plt
    except ImportError:  # pragma: no cover
        print("error: matplotlib not installed", file=sys.stderr)
        sys.exit(1)

    fig, axes = plt.subplots(2, 1, figsize=(10, 6), sharex=True)

    for series, label, color in [
        (smoothed_mm, "smoothed", "#2ca02c"),
        (reported_mm, "reported", "#f2c84b"),
    ]:
        xs, ys = split_xy(series)
        if xs:
            axes[0].plot(xs, ys, label=label, color=color, linewidth=1.5)
    axes[0].set_title("Imbalance (Max/Mean)")
    axes[0].grid(True, alpha=0.3)
    axes[0].legend(loc="upper right")

    for series, label, color in [
        (smoothed_cv, "smoothed", "#2ca02c"),
        (reported_cv, "reported", "#f2c84b"),
    ]:
        xs, ys = split_xy(series)
        if xs:
            axes[1].plot(xs, ys, label=label, color=color, linewidth=1.5)
    axes[1].set_title("Imbalance (CV)")
    axes[1].grid(True, alpha=0.3)
    axes[1].legend(loc="upper right")

    if title_prefix:
        fig.suptitle(title_prefix)
    fig.tight_layout()
    ensure_out_dir(out_dir)
    out_path = os.path.join(out_dir, f"imbalance.{fmt}")
    fig.savefig(out_path, dpi=160)
    plt.close(fig)
    print(f"wrote {out_path}")


def plot_churn(run_dir, out_dir, fmt, title_prefix):
    moves = load_series(os.path.join(run_dir, "moves_per_window.csv"))
    avg_moves = load_series(os.path.join(run_dir, "avg_moves_per_cycle.csv"))

    if not any([moves, avg_moves]):
        print("warn: no churn series found", file=sys.stderr)
        return

    try:
        import matplotlib.pyplot as plt
    except ImportError:  # pragma: no cover
        print("error: matplotlib not installed", file=sys.stderr)
        sys.exit(1)

    fig, axes = plt.subplots(2, 1, figsize=(10, 6), sharex=True)

    xs, ys = split_xy(moves)
    if xs:
        axes[0].plot(xs, ys, color="#1f77b4", linewidth=1.5)
    axes[0].set_title("Moves per Window")
    axes[0].grid(True, alpha=0.3)

    xs, ys = split_xy(avg_moves)
    if xs:
        axes[1].plot(xs, ys, color="#9467bd", linewidth=1.5)
    axes[1].set_title("Avg Moves per Cycle")
    axes[1].grid(True, alpha=0.3)

    if title_prefix:
        fig.suptitle(title_prefix)
    fig.tight_layout()
    ensure_out_dir(out_dir)
    out_path = os.path.join(out_dir, f"churn.{fmt}")
    fig.savefig(out_path, dpi=160)
    plt.close(fig)
    print(f"wrote {out_path}")


def main():
    args = parse_args()
    run_dir = args.run_dir
    out_dir = args.out_dir or run_dir
    title_prefix = args.title
    plot_imbalance(run_dir, out_dir, args.format, title_prefix)
    plot_churn(run_dir, out_dir, args.format, title_prefix)


if __name__ == "__main__":
    main()
