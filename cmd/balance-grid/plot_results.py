#!/usr/bin/env python3
"""
plot_results.py reads grid_results.csv produced by balance-grid and produces
plots + a ranked list of configurations that achieve a good trade-off between
few moves and low max-over-mean (reported).

Usage:
    python3 cmd/balance-grid/plot_results.py [csv_path] [output_dir]

If no arguments are given, it reads grid_results.csv and writes to ./plots/.
"""
import csv
import sys
import os
import math

try:
    import matplotlib.pyplot as plt
    import matplotlib.colors as mcolors
    import numpy as np
    HAS_DEPS = True
except ImportError:
    HAS_DEPS = False


def load_data(csv_path):
    rows = []
    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append({
                "upper_band": float(row["upper_band"]),
                "lower_band": float(row["lower_band"]),
                "severe_ratio": float(row["severe_ratio"]),
                "cooldown_ms": float(row["cooldown_ms"]),
                "move_budget": float(row["move_budget"]),
                "tau_ms": float(row["tau_ms"]),
                "use_smoothed_load": row["use_smoothed_load"].lower() == "true",
                "total_moves": int(row["total_moves"]),
                "total_load_moved": float(row["total_load_moved"]),
                "avg_mm_smooth": float(row["avg_mm_smooth"]),
                "worst_mm_smooth": float(row["worst_mm_smooth"]),
                "avg_mm_reported": float(row["avg_mm_reported"]),
                "worst_mm_reported": float(row["worst_mm_reported"]),
                "avg_cv_smooth": float(row["avg_cv_smooth"]),
                "worst_cv_smooth": float(row["worst_cv_smooth"]),
                "avg_cv_reported": float(row["avg_cv_reported"]),
                "worst_cv_reported": float(row["worst_cv_reported"]),
            })
    return rows


def normalize(values):
    min_v = min(values)
    max_v = max(values)
    if max_v == min_v:
        return [0.0] * len(values)
    return [(v - min_v) / (max_v - min_v) for v in values]


def pareto_frontier(rows, x_key, y_key, minimize_x=True, minimize_y=True):
    """Return indices of points on the Pareto frontier."""
    points = [(i, r[x_key], r[y_key]) for i, r in enumerate(rows)]
    frontier = []
    for i, x, y in points:
        dominated = False
        for j, x2, y2 in points:
            if i == j:
                continue
            better_x = x2 <= x if minimize_x else x2 >= x
            better_y = y2 <= y if minimize_y else y2 >= y
            if better_x and better_y and (x2 < x or y2 < y):
                dominated = True
                break
        if not dominated:
            frontier.append(i)
    return frontier


def marginal_means(rows, param_key, metric_key):
    """Return a dict {param_value: mean_metric}."""
    groups = {}
    for r in rows:
        groups.setdefault(r[param_key], []).append(r[metric_key])
    return {k: sum(v) / len(v) for k, v in groups.items()}


def plot_pareto(rows, out_dir):
    moves = [r["total_moves"] for r in rows]
    mm = [r["avg_mm_reported"] for r in rows]

    # Composite score: lower is better
    norm_moves = normalize(moves)
    norm_mm = normalize(mm)
    composite = [0.5 * nm + 0.5 * nv for nm, nv in zip(norm_moves, norm_mm)]
    best_idx = min(range(len(rows)), key=lambda i: composite[i])

    frontier = pareto_frontier(rows, "total_moves", "avg_mm_reported")

    fig, ax = plt.subplots(figsize=(8, 6))

    # All points
    scatter = ax.scatter(moves, mm, c=[r["use_smoothed_load"] for r in rows],
                         cmap="coolwarm", alpha=0.6, edgecolors="none", s=40)

    # Pareto frontier
    fx = [rows[i]["total_moves"] for i in frontier]
    fy = [rows[i]["avg_mm_reported"] for i in frontier]
    order = sorted(range(len(fx)), key=lambda i: fx[i])
    fx = [fx[i] for i in order]
    fy = [fy[i] for i in order]
    ax.plot(fx, fy, "k--", linewidth=1.5, label="Pareto frontier")

    # Best composite
    ax.scatter(rows[best_idx]["total_moves"], rows[best_idx]["avg_mm_reported"],
               c="gold", edgecolors="black", s=200, marker="*", zorder=5,
               label="Best trade-off (50/50 score)")

    # Top 5 by composite
    top5 = sorted(range(len(rows)), key=lambda i: composite[i])[:5]
    for i, idx in enumerate(top5):
        ax.annotate(str(i + 1),
                    (rows[idx]["total_moves"], rows[idx]["avg_mm_reported"]),
                    fontsize=8, color="darkgreen")

    ax.set_xlabel("Total Moves")
    ax.set_ylabel("Average Max/Mean (reported)")
    ax.set_title("Moves vs Balance Quality")
    ax.legend(loc="upper right")
    ax.grid(True, alpha=0.3)
    cbar = plt.colorbar(scatter, ax=ax)
    cbar.set_label("Use Smoothed Load")

    out_path = os.path.join(out_dir, "pareto_moves_vs_mm.png")
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()
    print(f"Saved: {out_path}")

    return best_idx, top5


def plot_param_effects(rows, out_dir):
    params = [
        ("upper_band", "Upper Band"),
        ("lower_band", "Lower Band"),
        ("severe_ratio", "Severe Ratio"),
        ("cooldown_ms", "Cooldown (ms)"),
        ("move_budget", "Move Budget"),
        ("tau_ms", "Tau (ms)"),
    ]

    fig, axes = plt.subplots(2, len(params), figsize=(18, 8), sharex=False)

    for col, (key, label) in enumerate(params):
        mm_vals = marginal_means(rows, key, "avg_mm_reported")
        move_vals = marginal_means(rows, key, "total_moves")

        xs_mm = sorted(mm_vals.keys())
        ys_mm = [mm_vals[x] for x in xs_mm]
        xs_mv = sorted(move_vals.keys())
        ys_mv = [move_vals[x] for x in xs_mv]

        ax_top = axes[0, col]
        ax_top.plot(xs_mm, ys_mm, marker="o", color="tab:blue")
        ax_top.set_title(label)
        ax_top.set_ylabel("Avg Max/Mean (reported)")
        ax_top.grid(True, alpha=0.3)

        ax_bot = axes[1, col]
        ax_bot.plot(xs_mv, ys_mv, marker="o", color="tab:orange")
        ax_bot.set_ylabel("Avg Total Moves")
        ax_bot.set_xlabel(label)
        ax_bot.grid(True, alpha=0.3)

    plt.tight_layout()
    out_path = os.path.join(out_dir, "param_effects.png")
    plt.savefig(out_path, dpi=150)
    plt.close()
    print(f"Saved: {out_path}")


def plot_heatmaps(rows, out_dir):
    # Heatmap: upper_band x lower_band -> avg_mm_reported and avg total_moves
    param_pairs = [
        (("upper_band", "Upper Band"), ("lower_band", "Lower Band")),
        (("move_budget", "Move Budget"), ("cooldown_ms", "Cooldown (ms)")),
        (("tau_ms", "Tau (ms)"), ("use_smoothed_load", "Use Smoothed Load")),
    ]

    for (x_key, x_label), (y_key, y_label) in param_pairs:
        x_vals = sorted({r[x_key] for r in rows})
        y_vals = sorted({r[y_key] for r in rows})

        mm_grid = np.zeros((len(y_vals), len(x_vals)))
        move_grid = np.zeros((len(y_vals), len(x_vals)))
        count_grid = np.zeros((len(y_vals), len(x_vals)))

        for r in rows:
            xi = x_vals.index(r[x_key])
            yi = y_vals.index(r[y_key])
            mm_grid[yi, xi] += r["avg_mm_reported"]
            move_grid[yi, xi] += r["total_moves"]
            count_grid[yi, xi] += 1

        mm_grid /= np.maximum(count_grid, 1)
        move_grid /= np.maximum(count_grid, 1)

        fig, axes = plt.subplots(1, 2, figsize=(12, 5))

        for ax, grid, title, cmap in [
            (axes[0], mm_grid, "Avg Max/Mean (reported)", "viridis_r"),
            (axes[1], move_grid, "Avg Total Moves", "plasma_r"),
        ]:
            im = ax.imshow(grid, aspect="auto", origin="lower", cmap=cmap)
            ax.set_xticks(range(len(x_vals)))
            ax.set_xticklabels([str(v) for v in x_vals], rotation=45, ha="right")
            ax.set_yticks(range(len(y_vals)))
            ax.set_yticklabels([str(v) for v in y_vals])
            ax.set_xlabel(x_label)
            ax.set_ylabel(y_label)
            ax.set_title(title)
            plt.colorbar(im, ax=ax)

        out_path = os.path.join(out_dir, f"heatmap_{x_key}_{y_key}.png")
        plt.tight_layout()
        plt.savefig(out_path, dpi=150)
        plt.close()
        print(f"Saved: {out_path}")


def print_rankings(rows, best_idx, top5, out_dir):
    out_path = os.path.join(out_dir, "top_configs.txt")
    with open(out_path, "w") as f:
        f.write("=" * 80 + "\n")
        f.write("TOP CONFIGURATIONS (best trade-off: few moves + low max/mean reported)\n")
        f.write("=" * 80 + "\n\n")

        f.write("SCORING METHOD\n")
        f.write("Each config is scored by normalizing total_moves and avg_mm_reported\n")
        f.write("to [0,1] across the grid, then combining with equal 50/50 weights.\n")
        f.write("Lower score = better.\n\n")

        f.write("BEST OVERALL CONFIGURATION\n")
        f.write("-" * 80 + "\n")
        best = rows[best_idx]
        for k, v in sorted(best.items()):
            f.write(f"  {k}: {v}\n")
        f.write("\n")

        f.write("TOP 5 CONFIGURATIONS BY COMPOSITE SCORE\n")
        f.write("-" * 80 + "\n")
        for rank, idx in enumerate(top5, 1):
            r = rows[idx]
            f.write(f"\n#{rank}\n")
            for k, v in sorted(r.items()):
                f.write(f"  {k}: {v}\n")

        # Also print configurations with very few moves (bottom 10%)
        moves_sorted = sorted(rows, key=lambda r: r["total_moves"])
        cutoff = max(1, len(moves_sorted) // 10)
        few_moves = moves_sorted[:cutoff]
        few_moves.sort(key=lambda r: r["avg_mm_reported"])

        f.write("\n\n")
        f.write("TOP CONFIGURATIONS AMONG FEWEST-MOVES (bottom 10% moves, sorted by mm)\n")
        f.write("-" * 80 + "\n")
        for rank, r in enumerate(few_moves[:10], 1):
            f.write(f"\n#{rank}\n")
            for k, v in sorted(r.items()):
                f.write(f"  {k}: {v}\n")

        # Also print configurations with lowest mm (bottom 10%)
        mm_sorted = sorted(rows, key=lambda r: r["avg_mm_reported"])
        cutoff = max(1, len(mm_sorted) // 10)
        low_mm = mm_sorted[:cutoff]
        low_mm.sort(key=lambda r: r["total_moves"])

        f.write("\n\n")
        f.write("TOP CONFIGURATIONS AMONG LOWEST MAX/MEAN (bottom 10% mm, sorted by moves)\n")
        f.write("-" * 80 + "\n")
        for rank, r in enumerate(low_mm[:10], 1):
            f.write(f"\n#{rank}\n")
            for k, v in sorted(r.items()):
                f.write(f"  {k}: {v}\n")

    print(f"Saved: {out_path}")


def main():
    csv_path = sys.argv[1] if len(sys.argv) > 1 else "grid_results.csv"
    out_dir = sys.argv[2] if len(sys.argv) > 2 else "plots"
    os.makedirs(out_dir, exist_ok=True)

    rows = load_data(csv_path)
    print(f"Loaded {len(rows)} rows from {csv_path}")

    if not HAS_DEPS:
        print("ERROR: matplotlib and numpy are required but not installed.")
        print("Install them in your virtual environment:")
        print("    pip install matplotlib numpy")
        sys.exit(1)

    best_idx, top5 = plot_pareto(rows, out_dir)
    plot_param_effects(rows, out_dir)
    plot_heatmaps(rows, out_dir)
    print_rankings(rows, best_idx, top5, out_dir)

    print("\nDone.")


if __name__ == "__main__":
    main()
