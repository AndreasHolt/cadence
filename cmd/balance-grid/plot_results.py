#!/usr/bin/env python3
"""
plot_results.py reads grid_results.csv and plots three metrics against move_penalty_coefficient.

Usage:
    .venv/bin/python3 cmd/balance-grid/plot_results.py [csv_path] [output_dir] [upper_band] [lower_band] [penalty_max]

Filters by upper_band and lower_band (e.g. 1.15 0.90).
penalty_max filters cost-aware rows to penalty <= penalty_max (e.g. 1 for a zoomed view).

When penalty_max is omitted, writes the full-range plot and an additional plot with
penalty coefficient up to DEFAULT_ZOOM_PENALTY_MAX (suffix _max1.0).
"""
import csv
import os
import sys

# When penalty_max is not passed on the CLI, also emit a zoomed plot with this cap.
DEFAULT_ZOOM_PENALTY_MAX = 1.0

# Okabe–Ito: blue for cost-aware series, orange for benefit baseline.
COST_COLOR = "#0072B2"
BENEFIT_COLOR = "#D55E00"
LINEWIDTH = 2.5
FONT_SIZE = 18  # 50% larger than the previous default of 15


def penalty_max_variants(explicit_penalty_max):
    if explicit_penalty_max is not None:
        return [explicit_penalty_max]
    return [None, DEFAULT_ZOOM_PENALTY_MAX]


def load_config_data(csv_path, upper_band, lower_band, penalty_max):
    cost_aware_rows = []
    benefit_row = None

    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if float(row["upper_band"]) != upper_band or float(row["lower_band"]) != lower_band:
                continue
            mode = row["move_scoring_mode"]
            penalty = float(row["move_penalty_coefficient"])
            row_data = {
                "fixed": penalty,
                "moves": int(row["total_moves"]),
                "load": float(row["total_load_moved"]),
                "mm": float(row["avg_mm_reported"]),
            }
            if mode == "cost_aware":
                if penalty_max is not None and penalty > penalty_max:
                    continue
                cost_aware_rows.append(row_data)
            elif benefit_row is None:
                benefit_row = row_data

    cost_aware_rows.sort(key=lambda r: r["fixed"])
    return cost_aware_rows, benefit_row


def _import_plt():
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib not found; use .venv/bin/python3 after pip install matplotlib")
        sys.exit(1)
    plt.rc("font", size=FONT_SIZE)
    plt.rc("axes", labelsize=FONT_SIZE)
    plt.rc("xtick", labelsize=FONT_SIZE)
    plt.rc("ytick", labelsize=FONT_SIZE)
    plt.rc("legend", fontsize=FONT_SIZE)
    return plt


def plot_single(csv_path, out_dir, upper_band, lower_band, penalty_max):
    plt = _import_plt()
    cost_aware_rows, benefit_row = load_config_data(
        csv_path, upper_band, lower_band, penalty_max,
    )

    ca_fixed = [r["fixed"] for r in cost_aware_rows]
    ca_moves = [r["moves"] for r in cost_aware_rows]
    ca_load = [r["load"] for r in cost_aware_rows]
    ca_mm = [r["mm"] for r in cost_aware_rows]

    fig, axes = plt.subplots(1, 3, figsize=(15, 5), sharex=True)
    ax1, ax2, ax3 = axes

    def plot_benefit_baseline(ax, y_value):
        if benefit_row is None or y_value is None or not ca_fixed:
            return
        ax.hlines(
            y_value, min(ca_fixed), max(ca_fixed),
            colors=BENEFIT_COLOR, linestyles="--",
            linewidth=LINEWIDTH, label="Benefit",
        )

    plot_kw = dict(
        linestyle="-",
        color=COST_COLOR,
        linewidth=LINEWIDTH,
        label="Cost",
    )

    ax1.plot(ca_fixed, ca_moves, **plot_kw)
    plot_benefit_baseline(ax1, benefit_row["moves"] if benefit_row else None)
    ax1.set_xlabel("Penalty Coefficient")
    ax1.set_ylabel("Total Moves")
    ax1.legend()
    ax1.grid(True, alpha=0.3)

    ax2.plot(ca_fixed, ca_load, **plot_kw)
    plot_benefit_baseline(ax2, benefit_row["load"] if benefit_row else None)
    ax2.set_xlabel("Penalty Coefficient")
    ax2.set_ylabel("Total Moved Load")
    ax2.legend()
    ax2.grid(True, alpha=0.3)

    ax3.plot(ca_fixed, ca_mm, **plot_kw)
    plot_benefit_baseline(ax3, benefit_row["mm"] if benefit_row else None)
    ax3.set_xlabel("Penalty Coefficient")
    ax3.set_ylabel("Average Max/Mean")
    ax3.legend()
    ax3.grid(True, alpha=0.3)

    plt.tight_layout()
    band_suffix = f"_{upper_band}_{lower_band}"
    zoom_suffix = f"_max{penalty_max}" if penalty_max is not None else ""
    out_path = os.path.join(out_dir, f"grid_results{band_suffix}{zoom_suffix}.png")
    plt.savefig(out_path, dpi=150)
    plt.close()
    print(f"Plot saved to {out_path}")


def main():
    csv_path = sys.argv[1] if len(sys.argv) > 1 else "grid_results.csv"
    out_dir = sys.argv[2] if len(sys.argv) > 2 else "plots"
    os.makedirs(out_dir, exist_ok=True)

    upper_band = float(sys.argv[3]) if len(sys.argv) > 3 else None
    lower_band = float(sys.argv[4]) if len(sys.argv) > 4 else None
    explicit_penalty_max = float(sys.argv[5]) if len(sys.argv) > 5 else None

    if upper_band is None or lower_band is None:
        print("Specify upper_band and lower_band.")
        sys.exit(1)

    for penalty_max in penalty_max_variants(explicit_penalty_max):
        plot_single(csv_path, out_dir, upper_band, lower_band, penalty_max)


if __name__ == "__main__":
    main()
