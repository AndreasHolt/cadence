#!/usr/bin/env python3
"""
Parse balance-sim JSON logs and plot swap/single-move analysis.

Usage (logs go to stderr, progress bar to stdout):
    go run ./cmd/balance-sim -csv history.csv 2> >(python3 cmd/balance-sim/plot_moves.py)

Or save stderr to a file first:
    go run ./cmd/balance-sim -csv history.csv 2> moves.log
    python3 cmd/balance-sim/plot_moves.py < moves.log
"""

import json
import sys
import re
from collections import defaultdict
from pathlib import Path

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import numpy as np
    _HAS_MATPLOTLIB = True
except ImportError:
    _HAS_MATPLOTLIB = False


def _require_matplotlib():
    if not _HAS_MATPLOTLIB:
        print("ERROR: matplotlib and numpy are required for plotting.")
        print("Install with:  pip install matplotlib numpy")
        sys.exit(1)


def parse_shard_list(shards):
    """Parse ['shard:load', ...] into dict {shard: load}."""
    result = {}
    if not shards:
        return result
    for s in shards:
        m = re.match(r"(.+):([\d.]+)", s)
        if m:
            result[m.group(1)] = float(m.group(2))
    return result


def extract_json(line):
    """Try to extract a JSON object from a line that may have progress-bar garbage.
    Returns (msg, dict) or (None, None)."""
    # Progress bar uses \r to overwrite; in a pipe the last segment after \r is what counts.
    if "\r" in line:
        line = line.rsplit("\r", 1)[-1]
    # Zap development encoder format: timestamp\tLEVEL\tcaller\tmsg\t{json...}
    # Split by tab; message is the 4th field (index 3), JSON is the 5th (index 4).
    parts = line.split("\t")
    if len(parts) < 5:
        return None, None
    msg = parts[3].strip()
    json_text = parts[4].strip()
    try:
        return msg, json.loads(json_text)
    except json.JSONDecodeError:
        pass
    # Try progressively shorter suffixes in case of trailing garbage.
    start = json_text.find("{")
    if start == -1:
        return None, None
    for end in range(len(json_text), start, -1):
        try:
            return msg, json.loads(json_text[start:end])
        except json.JSONDecodeError:
            continue
    return None, None


def parse_log_file(f):
    """Yield parsed events from JSON log lines."""
    events = []
    planned = {}  # key -> planned event, waiting for applied
    for raw in f:
        msg, rec = extract_json(raw)
        if rec is None:
            continue
        if msg == "load balance move planned":
            key = (
                rec.get("source_executor"),
                rec.get("destination_executor"),
            )
            planned[key] = rec
        elif msg == "load balance rejected single move":
            key = (
                rec.get("source_executor"),
                rec.get("destination_executor"),
            )
            if key in planned:
                planned[key]["best_single_move"] = rec.get("best_single_move", [])
        elif msg == "load balance move applied":
            key = (
                rec.get("source_executor"),
                rec.get("destination_executor"),
            )
            if key in planned:
                p = planned.pop(key)
                events.append({
                    "move_type": p.get("move_type", "unknown"),
                    "source": p.get("source_executor"),
                    "dest": p.get("destination_executor"),
                    "source_load_before": p.get("source_load_before", 0.0),
                    "dest_load_before": p.get("destination_load_before", 0.0),
                    "source_shards_before": parse_shard_list(p.get("source_shards_before", [])),
                    "dest_shards_before": parse_shard_list(p.get("destination_shards_before", [])),
                    "planned_moves": p.get("planned_moves", []),
                    "best_single_move": p.get("best_single_move", []),
                    "source_load_after": rec.get("source_load_after", 0.0),
                    "dest_load_after": rec.get("destination_load_after", 0.0),
                    "source_shards_after": parse_shard_list(rec.get("source_shards_after", [])),
                    "dest_shards_after": parse_shard_list(rec.get("destination_shards_after", [])),
                    "all_executors": p.get("all_executors", {}),
                })
    return events


def plot_loads_over_time(events, outdir):
    """Plot executor loads before each move, colored by move type."""
    _require_matplotlib()
    fig, ax = plt.subplots(figsize=(12, 6))

    exec_loads = defaultdict(lambda: {"single": [], "swap": [], "all": []})
    tick = 0
    for ev in events:
        mt = ev["move_type"]
        for ex, load in [(ev["source"], ev["source_load_before"]),
                          (ev["dest"], ev["dest_load_before"])]:
            exec_loads[ex]["all"].append((tick, load))
            exec_loads[ex][mt].append((tick, load))
        tick += 1

    for ex in sorted(exec_loads):
        xs, ys = zip(*exec_loads[ex]["all"]) if exec_loads[ex]["all"] else ([], [])
        ax.plot(xs, ys, label=ex, alpha=0.6, marker="o", markersize=3)

    ax.set_xlabel("Move #")
    ax.set_ylabel("Executor Load")
    ax.set_title("Executor Loads at Move Planning Time")
    ax.legend(loc="upper right")
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(outdir / "loads_over_time.png", dpi=150)
    plt.close(fig)


def plot_move_types(events, outdir):
    """Bar chart of single vs swap move counts."""
    _require_matplotlib()
    counts = defaultdict(int)
    for ev in events:
        counts[ev["move_type"]] += 1

    fig, ax = plt.subplots(figsize=(6, 4))
    types = ["single", "swap"]
    vals = [counts.get(t, 0) for t in types]
    colors = ["#3498db", "#e74c3c"]
    bars = ax.bar(types, vals, color=colors)
    ax.set_ylabel("Count")
    ax.set_title(f"Move Types (total moves: {len(events)})")
    for bar, v in zip(bars, vals):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.5,
                str(v), ha="center", va="bottom", fontsize=12)
    fig.tight_layout()
    fig.savefig(outdir / "move_types.png", dpi=150)
    plt.close(fig)


def plot_swap_examples(events, outdir, max_swaps=6):
    """Visualize individual swap moves: before/after shard loads."""
    _require_matplotlib()
    swaps = [e for e in events if e["move_type"] == "swap"]
    if not swaps:
        print("No swap moves found — skipping swap_examples plot")
        return

    n = min(max_swaps, len(swaps))
    fig, axes = plt.subplots(n, 1, figsize=(10, 2.5 * n), squeeze=False)

    for idx, ev in enumerate(swaps[:n]):
        ax = axes[idx, 0]
        src, dst = ev["source"], ev["dest"]

        # Before
        src_before = list(ev["source_shards_before"].values())
        dst_before = list(ev["dest_shards_before"].values())
        # After
        src_after = list(ev["source_shards_after"].values())
        dst_after = list(ev["dest_shards_after"].values())

        x = np.arange(2)
        width = 0.35
        before = [sum(src_before), sum(dst_before)]
        after = [sum(src_after), sum(dst_after)]

        ax.bar(x - width / 2, before, width, label="Before", color="#e74c3c", alpha=0.8)
        ax.bar(x + width / 2, after, width, label="After", color="#2ecc71", alpha=0.8)

        ax.set_xticks(x)
        ax.set_xticklabels([src, dst])
        ax.set_ylabel("Total Load")
        ax.set_title(
            f"Swap #{idx + 1}: {src} ({ev['source_load_before']:.1f}→{ev['source_load_after']:.1f})  "
            f"↔ {dst} ({ev['dest_load_before']:.1f}→{ev['dest_load_after']:.1f})"
        )
        ax.legend()
        ax.grid(True, alpha=0.3, axis="y")

    fig.tight_layout()
    fig.savefig(outdir / "swap_examples.png", dpi=150)
    plt.close(fig)


def plot_swap_heatmap(events, outdir):
    """Heatmap showing which executor pairs swapped most often."""
    _require_matplotlib()
    pair_counts = defaultdict(int)
    for ev in events:
        if ev["move_type"] == "swap":
            pair = tuple(sorted([ev["source"], ev["dest"]]))
            pair_counts[pair] += 1

    if not pair_counts:
        print("No swaps — skipping swap heatmap")
        return

    executors = sorted({ex for pair in pair_counts for ex in pair})
    n = len(executors)
    mat = np.zeros((n, n), dtype=int)
    for (a, b), c in pair_counts.items():
        i, j = executors.index(a), executors.index(b)
        mat[i, j] = c
        mat[j, i] = c

    fig, ax = plt.subplots(figsize=(8, 6))
    im = ax.imshow(mat, cmap="YlOrRd", aspect="auto")
    ax.set_xticks(np.arange(n))
    ax.set_yticks(np.arange(n))
    ax.set_xticklabels(executors, rotation=45, ha="right")
    ax.set_yticklabels(executors)
    ax.set_title("Swap Frequency by Executor Pair")

    for i in range(n):
        for j in range(n):
            text = ax.text(j, i, mat[i, j], ha="center", va="center",
                           color="black" if mat[i, j] < mat.max() / 2 else "white")

    fig.colorbar(im, ax=ax, label="Swap Count")
    fig.tight_layout()
    fig.savefig(outdir / "swap_heatmap.png", dpi=150)
    plt.close(fig)


def plot_load_imbalance(events, outdir):
    """Plot load difference (source - dest) before vs after, colored by move type."""
    _require_matplotlib()
    fig, ax = plt.subplots(figsize=(8, 6))

    single_before = []
    single_after = []
    swap_before = []
    swap_after = []

    for ev in events:
        diff_before = abs(ev["source_load_before"] - ev["dest_load_before"])
        diff_after = abs(ev["source_load_after"] - ev["dest_load_after"])
        if ev["move_type"] == "single":
            single_before.append(diff_before)
            single_after.append(diff_after)
        else:
            swap_before.append(diff_before)
            swap_after.append(diff_after)

    ax.scatter(single_before, single_after, c="#3498db", alpha=0.6, s=50, label="single")
    ax.scatter(swap_before, swap_after, c="#e74c3c", alpha=0.6, s=50, label="swap")

    # Reference line y=x
    all_vals = single_before + single_after + swap_before + swap_after
    if all_vals:
        lim = max(all_vals) * 1.05
        ax.plot([0, lim], [0, lim], "k--", alpha=0.4, label="no change")
        ax.set_xlim(0, lim)
        ax.set_ylim(0, lim)

    ax.set_xlabel("|Source - Dest| Load Before")
    ax.set_ylabel("|Source - Dest| Load After")
    ax.set_title("Load Imbalance: Before vs After Move")
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(outdir / "load_imbalance.png", dpi=150)
    plt.close(fig)


def _stacked_bar(ax, x, shards, highlighted, color_muted, color_highlight, edgecolor="#95a5a6"):
    """Draw a single stacked bar at position x. shards is {id: load}."""
    bottom = 0.0
    for shard_id, load in sorted(shards.items(), key=lambda kv: kv[1], reverse=True):
        is_highlight = shard_id in highlighted
        color = color_highlight if is_highlight else color_muted
        ec = "black" if is_highlight else edgecolor
        lw = 2.0 if is_highlight else 0.5
        ax.bar(x, load, bottom=bottom, color=color, edgecolor=ec, linewidth=lw, width=0.6)
        bottom += load
    return bottom


def _compute_global_mean_load(ev):
    """Compute the average load across all executors at the time of a single event.
    Uses the all_executors field logged from the Go side."""
    all_ex = ev.get("all_executors", {})
    if not all_ex:
        return 0.0
    total = sum(ex.get("load", 0.0) for ex in all_ex.values())
    return total / len(all_ex)


def plot_first_swap_detail(events, outdir):
    """Detailed stacked-bar view of the first swap move."""
    _require_matplotlib()
    swaps = [e for e in events if e["move_type"] == "swap"]
    if not swaps:
        print("No swap moves found — skipping swap_detail plot")
        return

    ev = swaps[0]
    src, dst = ev["source"], ev["dest"]

    # Identify which shards move in each direction.
    moved_src_to_dst = set()
    moved_dst_to_src = set()
    for m in ev["planned_moves"]:
        if m["From"] == src and m["To"] == dst:
            moved_src_to_dst.add(m["ShardID"])
        elif m["From"] == dst and m["To"] == src:
            moved_dst_to_src.add(m["ShardID"])

    # Compute global average load across all executors.
    global_mean = _compute_global_mean_load(ev)

    fig, ax = plt.subplots(figsize=(10, 8))

    # Positions: Source-Before, Source-After, Dest-Before, Dest-After
    positions = [0, 1, 2.5, 3.5]
    labels = [f"{src}\n(Before)", f"{src}\n(After)", f"{dst}\n(Before)", f"{dst}\n(After)"]

    # Source bars
    _stacked_bar(ax, positions[0], ev["source_shards_before"],
                 moved_src_to_dst, "#bdc3c7", "#e74c3c")
    _stacked_bar(ax, positions[1], ev["source_shards_after"],
                 moved_dst_to_src, "#bdc3c7", "#2ecc71")

    # Dest bars
    _stacked_bar(ax, positions[2], ev["dest_shards_before"],
                 moved_dst_to_src, "#bdc3c7", "#2ecc71")
    _stacked_bar(ax, positions[3], ev["dest_shards_after"],
                 moved_src_to_dst, "#bdc3c7", "#e74c3c")

    # Global average line
    ax.axhline(global_mean, color="#34495e", linestyle="--", linewidth=1.5,
               label=f"Avg load all executors ({global_mean:.1f})")

    # Styling
    ax.set_xticks(positions)
    ax.set_xticklabels(labels, fontsize=11)
    ax.set_ylabel("Load", fontsize=12)
    ax.set_title(
        f"First Swap Detail: {src} ↔ {dst}\n"
        f"{src}: {ev['source_load_before']:.1f} → {ev['source_load_after']:.1f}   |   "
        f"{dst}: {ev['dest_load_before']:.1f} → {ev['dest_load_after']:.1f}",
        fontsize=13
    )

    # Build legend
    from matplotlib.patches import Patch
    legend_items = [
        Patch(facecolor="#e74c3c", edgecolor="black", label=f"Shard moving {src} → {dst}"),
        Patch(facecolor="#2ecc71", edgecolor="black", label=f"Shard moving {dst} → {src}"),
        Patch(facecolor="#bdc3c7", edgecolor="#95a5a6", label="Unchanged shard"),
    ]
    ax.legend(handles=legend_items, loc="upper right")
    ax.grid(True, alpha=0.2, axis="y")

    # Add a small text box with move details
    move_lines = [f"{m['ShardID']}: {m['From']} → {m['To']}" for m in ev["planned_moves"]]
    textstr = "Planned moves:\n" + "\n".join(move_lines)
    ax.text(0.02, 0.98, textstr, transform=ax.transAxes, fontsize=9,
            verticalalignment="top", bbox=dict(boxstyle="round", facecolor="wheat", alpha=0.5))

    fig.tight_layout()
    fig.savefig(outdir / "swap_detail_first.png", dpi=150)
    plt.close(fig)


def _compute_single_after_state(ev):
    """Compute the hypothetical after-state if the best single move had been applied."""
    src, dst = ev["source"], ev["dest"]
    src_before = ev["source_shards_before"]
    dst_before = ev["dest_shards_before"]
    src_load_before = ev["source_load_before"]
    dst_load_before = ev["dest_load_before"]

    single_shard_id = ev["best_single_move"][0]["ShardID"]
    single_from = ev["best_single_move"][0]["From"]
    single_to = ev["best_single_move"][0]["To"]
    single_load = src_before.get(single_shard_id, 0.0) if single_from == src else dst_before.get(single_shard_id, 0.0)

    single_src_after = dict(src_before)
    single_dst_after = dict(dst_before)
    if single_from == src and single_to == dst:
        single_src_after.pop(single_shard_id, None)
        single_dst_after[single_shard_id] = single_load
    elif single_from == dst and single_to == src:
        single_dst_after.pop(single_shard_id, None)
        single_src_after[single_shard_id] = single_load

    single_src_load_after = src_load_before - single_load if single_from == src else src_load_before + single_load
    single_dst_load_after = dst_load_before + single_load if single_to == dst else dst_load_before - single_load

    return {
        "single_shard_id": single_shard_id,
        "single_from": single_from,
        "single_to": single_to,
        "single_load": single_load,
        "single_src_after": single_src_after,
        "single_dst_after": single_dst_after,
        "single_src_load_after": single_src_load_after,
        "single_dst_load_after": single_dst_load_after,
    }


def _plot_one_swap_vs_single(ax_swap, ax_single, ev, single_info, idx):
    """Render one swap-vs-single comparison row on the given axes."""
    src, dst = ev["source"], ev["dest"]
    src_before = ev["source_shards_before"]
    dst_before = ev["dest_shards_before"]
    src_load_before = ev["source_load_before"]
    dst_load_before = ev["dest_load_before"]

    # Identify shards in the actual swap
    swap_src_to_dst = set()
    swap_dst_to_src = set()
    for m in ev["planned_moves"]:
        if m["From"] == src and m["To"] == dst:
            swap_src_to_dst.add(m["ShardID"])
        elif m["From"] == dst and m["To"] == src:
            swap_dst_to_src.add(m["ShardID"])

    single_shard_id = single_info["single_shard_id"]
    single_from = single_info["single_from"]
    single_to = single_info["single_to"]
    single_load = single_info["single_load"]
    single_src_after = single_info["single_src_after"]
    single_dst_after = single_info["single_dst_after"]
    single_src_load_after = single_info["single_src_load_after"]
    single_dst_load_after = single_info["single_dst_load_after"]

    positions = [0, 1, 2.5, 3.5]
    labels = [f"{src}\n(Before)", f"{dst}\n(Before)", f"{src}\n(After)", f"{dst}\n(After)"]

    # --- LEFT: Actual Swap ---
    _stacked_bar(ax_swap, positions[0], src_before, swap_src_to_dst, "#bdc3c7", "#e74c3c")
    _stacked_bar(ax_swap, positions[1], dst_before, swap_dst_to_src, "#bdc3c7", "#2ecc71")
    _stacked_bar(ax_swap, positions[2], ev["source_shards_after"], swap_dst_to_src, "#bdc3c7", "#2ecc71")
    _stacked_bar(ax_swap, positions[3], ev["dest_shards_after"], swap_src_to_dst, "#bdc3c7", "#e74c3c")
    ax_swap.set_xticks(positions)
    ax_swap.set_xticklabels(labels, fontsize=9)
    ax_swap.set_ylabel("Load")
    ax_swap.set_title(
        f"SWAP #{idx+1}: {src} ↔ {dst}\n"
        f"{src}: {src_load_before:.1f}→{ev['source_load_after']:.1f}  |  "
        f"{dst}: {dst_load_before:.1f}→{ev['dest_load_after']:.1f}",
        fontsize=10
    )
    ax_swap.grid(True, alpha=0.2, axis="y")

    # --- RIGHT: Best Single Move ---
    single_highlight_src_before = {single_shard_id} if single_from == src else set()
    single_highlight_dst_before = {single_shard_id} if single_from == dst else set()
    single_highlight_src_after = {single_shard_id} if single_to == src else set()
    single_highlight_dst_after = {single_shard_id} if single_to == dst else set()
    _stacked_bar(ax_single, positions[0], src_before, single_highlight_src_before, "#bdc3c7", "#f39c12")
    _stacked_bar(ax_single, positions[1], dst_before, single_highlight_dst_before, "#bdc3c7", "#f39c12")
    _stacked_bar(ax_single, positions[2], single_src_after, single_highlight_src_after, "#bdc3c7", "#f39c12")
    _stacked_bar(ax_single, positions[3], single_dst_after, single_highlight_dst_after, "#bdc3c7", "#f39c12")
    ax_single.set_xticks(positions)
    ax_single.set_xticklabels(labels, fontsize=9)
    ax_single.set_ylabel("Load")
    ax_single.set_title(
        f"BEST SINGLE #{idx+1}: {single_from} → {single_to} (shard {single_shard_id}, load {single_load:.1f})\n"
        f"{src}: {src_load_before:.1f}→{single_src_load_after:.1f}  |  "
        f"{dst}: {dst_load_before:.1f}→{single_dst_load_after:.1f}",
        fontsize=10
    )
    ax_single.grid(True, alpha=0.2, axis="y")


def plot_swap_vs_single_comparison(events, outdir, max_swaps=5):
    """For the N swaps with the biggest advantage over the best single move."""
    _require_matplotlib()
    swaps = [e for e in events if e["move_type"] == "swap" and e.get("best_single_move")]
    if not swaps:
        print("No swaps with rejected single moves found — skipping comparison plot")
        return

    # Compute advantage score for each swap: how much more the swap reduces
    # |src-dst| imbalance compared to the best single move.
    scored = []
    for ev in swaps:
        single_info = _compute_single_after_state(ev)
        swap_diff = abs(ev["source_load_after"] - ev["dest_load_after"])
        single_diff = abs(single_info["single_src_load_after"] - single_info["single_dst_load_after"])
        advantage = single_diff - swap_diff  # bigger = swap was much better
        scored.append((advantage, ev, single_info))

    scored.sort(key=lambda x: x[0], reverse=True)
    top = scored[:max_swaps]

    n = len(top)
    fig, axes = plt.subplots(n, 2, figsize=(16, 4 * n), squeeze=False)

    for idx, (advantage, ev, single_info) in enumerate(top):
        _plot_one_swap_vs_single(axes[idx, 0], axes[idx, 1], ev, single_info, idx)

    # Shared legend + title
    from matplotlib.patches import Patch
    legend_items = [
        Patch(facecolor="#e74c3c", edgecolor="black", label="Shard src→dst (swap)"),
        Patch(facecolor="#2ecc71", edgecolor="black", label="Shard dst→src (swap)"),
        Patch(facecolor="#f39c12", edgecolor="black", label="Shard in single move"),
        Patch(facecolor="#bdc3c7", edgecolor="#95a5a6", label="Unchanged shard"),
    ]
    fig.legend(handles=legend_items, loc="lower center", ncol=4, bbox_to_anchor=(0.5, -0.02))
    fig.suptitle(
        f"Top {n} Swaps by Advantage over Best Single Move "
        f"(sorted by reduction in |src-dst| imbalance)",
        fontsize=14, y=1.0
    )
    fig.tight_layout(rect=[0, 0.03, 1, 1])
    fig.savefig(outdir / "swap_vs_single_comparison.png", dpi=150)
    plt.close(fig)


def plot_biggest_swap_vs_single(events, outdir):
    """Plot the single swap with the biggest advantage over the best single move."""
    _require_matplotlib()
    swaps = [e for e in events if e["move_type"] == "swap" and e.get("best_single_move")]
    if not swaps:
        print("No swaps with rejected single moves found — skipping biggest_swap_vs_single plot")
        return

    scored = []
    for ev in swaps:
        single_info = _compute_single_after_state(ev)
        swap_diff = abs(ev["source_load_after"] - ev["dest_load_after"])
        single_diff = abs(single_info["single_src_load_after"] - single_info["single_dst_load_after"])
        advantage = single_diff - swap_diff
        scored.append((advantage, ev, single_info))

    scored.sort(key=lambda x: x[0], reverse=True)
    advantage, ev, single_info = scored[0]

    fig, axes = plt.subplots(1, 2, figsize=(16, 5))
    _plot_one_swap_vs_single(axes[0], axes[1], ev, single_info, 0)

    from matplotlib.patches import Patch
    legend_items = [
        Patch(facecolor="#e74c3c", edgecolor="black", label="Shard src→dst (swap)"),
        Patch(facecolor="#2ecc71", edgecolor="black", label="Shard dst→src (swap)"),
        Patch(facecolor="#f39c12", edgecolor="black", label="Shard in single move"),
        Patch(facecolor="#bdc3c7", edgecolor="#95a5a6", label="Unchanged shard"),
    ]
    fig.legend(handles=legend_items, loc="lower center", ncol=4, bbox_to_anchor=(0.5, -0.02))
    fig.suptitle(
        f"Biggest Swap Advantage over Best Single Move (advantage={advantage:.1f})",
        fontsize=14, y=1.0
    )
    fig.tight_layout(rect=[0, 0.03, 1, 1])
    fig.savefig(outdir / "biggest_swap_vs_single.png", dpi=150)
    plt.close(fig)


def print_summary(events):
    """Print text summary to stdout."""
    total = len(events)
    swaps = sum(1 for e in events if e["move_type"] == "swap")
    singles = total - swaps

    print(f"\n{'='*50}")
    print(f"  Total moves planned: {total}")
    print(f"  Single moves:        {singles} ({singles/total*100:.1f}%)")
    print(f"  Swap moves:          {swaps} ({swaps/total*100:.1f}%)")
    print(f"{'='*50}\n")

    if swaps:
        print("Swap details:")
        for i, ev in enumerate([e for e in events if e["move_type"] == "swap"][:10]):
            moves = ev["planned_moves"]
            move_str = "  ".join(f"{m['ShardID']}:{m['From']}→{m['To']}" for m in moves)
            print(f"  [{i+1}] {ev['source']}({ev['source_load_before']:.1f})"
                  f" ↔ {ev['dest']}({ev['dest_load_before']:.1f})  |  {move_str}")
        if swaps > 10:
            print(f"  ... and {swaps - 10} more")
        print()


def main():
    outdir = Path("plots")
    outdir.mkdir(exist_ok=True)

    print("Reading JSON logs from stdin...")
    events = parse_log_file(sys.stdin)
    print(f"Parsed {len(events)} paired move events.")

    if not events:
        print("No move events found.")
        print("  Recommended: go run ./cmd/balance-sim -csv history.csv 2> >(python3 cmd/balance-sim/plot_moves.py)")
        print("  Or:          go run ./cmd/balance-sim -csv history.csv 2> moves.log && python3 cmd/balance-sim/plot_moves.py < moves.log")
        sys.exit(1)

    print_summary(events)

    print("Generating plots...")
    plot_loads_over_time(events, outdir)
    plot_move_types(events, outdir)
    plot_swap_examples(events, outdir)
    plot_swap_heatmap(events, outdir)
    plot_load_imbalance(events, outdir)
    plot_first_swap_detail(events, outdir)
    plot_swap_vs_single_comparison(events, outdir)
    plot_biggest_swap_vs_single(events, outdir)

    print(f"Done. Plots saved to ./{outdir}/")
    for f in sorted(outdir.iterdir()):
        if f.suffix == ".png":
            print(f"  - {f.name}")


if __name__ == "__main__":
    main()
