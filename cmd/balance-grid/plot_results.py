#!/usr/bin/env python3
"""
plot_results.py reads grid_results.csv and plots three metrics against move_penalty_coefficient.

Usage:
    python3 cmd/balance-grid/plot_results.py [csv_path] [output_dir]

If no arguments are given, it reads grid_results.csv and writes to ./plots/.
"""
import csv
import sys
import os

def main():
    csv_path = sys.argv[1] if len(sys.argv) > 1 else "grid_results.csv"
    out_dir = sys.argv[2] if len(sys.argv) > 2 else "plots"
    os.makedirs(out_dir, exist_ok=True)

    cost_aware_rows = []
    benefit_rows = []

    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            mode = row["move_scoring_mode"]
            row_data = {
                "fixed": float(row["move_penalty_coefficient"]),
                "moves": int(row["total_moves"]),
                "load": float(row["total_load_moved"]),
                "mm": float(row["avg_mm_reported"]),
            }
            if mode == "cost_aware":
                cost_aware_rows.append(row_data)
            else:
                benefit_rows.append(row_data)

    # Sort by fixed cost
    cost_aware_rows.sort(key=lambda r: r["fixed"])
    benefit_rows.sort(key=lambda r: r["fixed"])

    # Extract series for cost_aware
    ca_fixed = [r["fixed"] for r in cost_aware_rows]
    ca_moves = [r["moves"] for r in cost_aware_rows]
    ca_load = [r["load"] for r in cost_aware_rows]
    ca_mm = [r["mm"] for r in cost_aware_rows]

    # Extract series for benefit (flat line across all fixed costs)
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

    ax1.plot(ca_fixed, ca_moves, marker="o", linestyle="-", color="tab:orange", label="Cost")
    ax1.plot(b_fixed, b_moves, marker="x", linestyle="--", color="tab:blue", label="Benefit")
    ax1.set_xlabel("Penalty Coefficient")
    ax1.set_ylabel("Total Moves")
    ax1.legend()
    ax1.grid(True, alpha=0.3)

    ax2.plot(ca_fixed, ca_load, marker="o", linestyle="-", color="tab:orange", label="Cost")
    ax2.plot(b_fixed, b_load, marker="x", linestyle="--", color="tab:blue", label="Benefit")
    ax2.set_xlabel("Penalty Coefficient")
    ax2.set_ylabel("Total Moved Load")
    ax2.legend()
    ax2.grid(True, alpha=0.3)

    ax3.plot(ca_fixed, ca_mm, marker="o", linestyle="-", color="tab:orange", label="Cost")
    ax3.plot(b_fixed, b_mm, marker="x", linestyle="--", color="tab:blue", label="Benefit")
    ax3.set_xlabel("Penalty Coefficient")
    ax3.set_ylabel("Average Max/Mean")
    ax3.legend()
    ax3.grid(True, alpha=0.3)

    plt.tight_layout()
    out_path = os.path.join(out_dir, "grid_results.png")
    plt.savefig(out_path, dpi=150)
    print(f"Plot saved to {out_path}")


if __name__ == "__main__":
    main()
