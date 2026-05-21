#!/usr/bin/env python3
"""
plot_results.py reads grid_results.csv and plots three metrics against move_penalty_coefficient.

Usage:
    python3 cmd/balance-grid/plot_results.py [csv_path] [output_dir] [--no-narrow]

If no arguments are given, it reads grid_results.csv and writes to ./plots/.
Use --no-narrow to exclude the narrow-band cost-aware series from the plot.
"""
import argparse
import csv
import os

def main():
    parser = argparse.ArgumentParser(
        description="Plot grid_results.csv metrics against move_penalty_coefficient."
    )
    parser.add_argument("csv_path", nargs="?", default="grid_results.csv", help="Path to CSV file")
    parser.add_argument("output_dir", nargs="?", default="plots", help="Output directory for plots")
    parser.add_argument("--no-narrow", action="store_true", help="Exclude narrow band results from the plot")
    args = parser.parse_args()

    csv_path = args.csv_path
    out_dir = args.output_dir
    include_narrow = not args.no_narrow
    os.makedirs(out_dir, exist_ok=True)

    cost_aware_rows = []
    cost_aware_narrow_rows = []
    benefit_rows = []

    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            mode = row["move_scoring_mode"]
            upper = float(row["upper_band"])
            lower = float(row["lower_band"])
            row_data = {
                "fixed": float(row["move_penalty_coefficient"]),
                "moves": int(row["total_moves"]),
                "load": float(row["total_load_moved"]),
                "mm": float(row["avg_mm_reported"]),
            }
            if mode == "benefit":
                benefit_rows.append(row_data)
            elif upper == 1.05 and lower == 0.95:
                cost_aware_narrow_rows.append(row_data)
            else:
                cost_aware_rows.append(row_data)

    # Sort by fixed cost
    cost_aware_rows.sort(key=lambda r: r["fixed"])
    cost_aware_narrow_rows.sort(key=lambda r: r["fixed"])
    benefit_rows.sort(key=lambda r: r["fixed"])

    # Extract series for cost_aware (default bands)
    ca_fixed = [r["fixed"] for r in cost_aware_rows]
    ca_moves = [r["moves"] for r in cost_aware_rows]
    ca_load = [r["load"] for r in cost_aware_rows]
    ca_mm = [r["mm"] for r in cost_aware_rows]

    # Extract series for cost_aware (narrow bands)
    can_fixed = [r["fixed"] for r in cost_aware_narrow_rows]
    can_moves = [r["moves"] for r in cost_aware_narrow_rows]
    can_load = [r["load"] for r in cost_aware_narrow_rows]
    can_mm = [r["mm"] for r in cost_aware_narrow_rows]

    # Extract series for benefit
    b_fixed = [r["fixed"] for r in benefit_rows]
    b_moves = [r["moves"] for r in benefit_rows]
    b_load = [r["load"] for r in benefit_rows]
    b_mm = [r["mm"] for r in benefit_rows]

    try:
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib not found.")

    plt.rc("font", size=15)
    plt.rc("axes", labelsize=15)
    plt.rc("xtick", labelsize=15)
    plt.rc("ytick", labelsize=15)
    plt.rc("legend", fontsize=15)

    fig, axes = plt.subplots(1, 3, figsize=(15, 5), sharex=True)

    ax1, ax2, ax3 = axes

    ax1.plot(ca_fixed, ca_moves, marker="o", linestyle="-", color="tab:orange", label="Cost (default)")
    if include_narrow:
        ax1.plot(can_fixed, can_moves, marker="s", linestyle="-", color="tab:green", label="Cost (narrow)")
    ax1.plot(b_fixed, b_moves, marker="x", linestyle="--", color="tab:blue", label="Benefit")
    ax1.set_xlabel("Penalty Coefficient")
    ax1.set_ylabel("Total Moves")
    ax1.set_ylim(bottom=0)
    ax1.legend()
    ax1.grid(True)

    ax2.plot(ca_fixed, ca_load, marker="o", linestyle="-", color="tab:orange", label="Cost (default)")
    if include_narrow:
        ax2.plot(can_fixed, can_load, marker="s", linestyle="-", color="tab:green", label="Cost (narrow)")
    ax2.plot(b_fixed, b_load, marker="x", linestyle="--", color="tab:blue", label="Benefit")
    ax2.set_xlabel("Penalty Coefficient")
    ax2.set_ylabel("Total Moved Load")
    ax2.set_ylim(bottom=0)
    ax2.legend()
    ax2.grid(True)

    ax3.plot(ca_fixed, ca_mm, marker="o", linestyle="-", color="tab:orange", label="Cost (default)")
    if include_narrow:
        ax3.plot(can_fixed, can_mm, marker="s", linestyle="-", color="tab:green", label="Cost (narrow)")
    ax3.plot(b_fixed, b_mm, marker="x", linestyle="--", color="tab:blue", label="Benefit")
    ax3.set_xlabel("Penalty Coefficient")
    ax3.set_ylabel("Average Max/Mean")
    ax3.legend()
    ax3.grid(True)

    plt.tight_layout()
    out_path = os.path.join(out_dir, "grid_results.png")
    plt.savefig(out_path, dpi=150)
    print(f"Plot saved to {out_path}")


if __name__ == "__main__":
    main()
