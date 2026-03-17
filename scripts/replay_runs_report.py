#!/usr/bin/env python3
"""Summarize latest replay devloop run against recent and best prior runs."""

from __future__ import annotations

import argparse
import csv
import math
import os
from typing import Dict, List, Optional, Sequence, Tuple


def parse_args() -> argparse.Namespace:
    default_runs_root = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "plots", "devloop_runs")
    )
    parser = argparse.ArgumentParser(
        description="Report latest replay run vs recent and best prior runs."
    )
    parser.add_argument(
        "--runs-root",
        default=default_runs_root,
        help="Root directory containing devloop run artifacts (default: %(default)s).",
    )
    parser.add_argument(
        "--run-id",
        default="",
        help="Anchor report to a specific run_id (defaults to newest run).",
    )
    parser.add_argument(
        "--recent",
        type=int,
        default=3,
        help="How many previous runs in same scenario to include (default: %(default)s).",
    )
    return parser.parse_args()


def as_float(value: str) -> Optional[float]:
    if value is None:
        return None
    text = str(value).strip()
    if text == "" or text.lower() == "na":
        return None
    try:
        return float(text)
    except ValueError:
        return None


def score(row: Dict[str, str]) -> float:
    cv = as_float(row.get("smoothed_load_cv", ""))
    moves_per_cycle = as_float(row.get("moves_per_cycle", ""))
    max_over_mean = as_float(row.get("smoothed_load_max_over_mean", ""))
    if cv is None or moves_per_cycle is None or max_over_mean is None:
        return math.inf

    # Lower is better. Penalize churn and high max/mean while prioritizing CV.
    max_penalty = max(0.0, max_over_mean - 1.0)
    return cv + 0.25 * moves_per_cycle + 0.15 * max_penalty


def load_rows(index_file: str) -> List[Dict[str, str]]:
    if not os.path.isfile(index_file):
        raise FileNotFoundError(f"Missing index file: {index_file}")

    with open(index_file, "r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    if not rows:
        raise ValueError(f"Index file has no data rows: {index_file}")
    return rows


def find_anchor(rows: Sequence[Dict[str, str]], run_id: str) -> Dict[str, str]:
    if not run_id:
        return rows[-1]
    for row in rows:
        if row.get("run_id", "") == run_id:
            return row
    raise ValueError(f"run_id '{run_id}' not found in index")


def scenario_key(row: Dict[str, str]) -> Tuple[str, str, str, str]:
    return (
        row.get("csv", ""),
        row.get("executors", ""),
        row.get("replay_speed", ""),
        row.get("run_seconds", ""),
    )


def fmt_num(value: Optional[float], width: int = 8, precision: int = 4) -> str:
    if value is None or math.isinf(value):
        return f"{'-':>{width}}"
    return f"{value:>{width}.{precision}f}"


def row_run_dir(runs_root: str, row: Dict[str, str]) -> str:
    run_id = row.get("run_id", "")
    run_title = row.get("run_title", "")
    return os.path.join(runs_root, f"{run_id}__{run_title}")


def print_header(anchor: Dict[str, str], runs_root: str, scenario_rows: Sequence[Dict[str, str]]) -> None:
    print("Replay Run Report")
    print(f"run_id      : {anchor.get('run_id', '')}")
    print(f"run_title   : {anchor.get('run_title', '')}")
    print(f"timestamp   : {anchor.get('timestamp_utc', '')}")
    print(f"git_sha     : {anchor.get('git_sha', '')}")
    print(f"status      : {anchor.get('status', '')}")
    print(
        "scenario    : "
        f"csv={anchor.get('csv', '')}, "
        f"executors={anchor.get('executors', '')}, "
        f"speed={anchor.get('replay_speed', '')}, "
        f"run_seconds={anchor.get('run_seconds', '')}"
    )
    print(f"run_dir     : {row_run_dir(runs_root, anchor)}")
    print(f"scenario_n  : {len(scenario_rows)}")
    print()


def print_delta(anchor: Dict[str, str], best: Optional[Dict[str, str]]) -> None:
    if best is None:
        print("Best prior  : no successful runs in this scenario")
        print()
        return

    anchor_cv = as_float(anchor.get("smoothed_load_cv", ""))
    anchor_mpc = as_float(anchor.get("moves_per_cycle", ""))
    anchor_score = score(anchor)

    best_cv = as_float(best.get("smoothed_load_cv", ""))
    best_mpc = as_float(best.get("moves_per_cycle", ""))
    best_score = score(best)

    delta_cv = None if anchor_cv is None or best_cv is None else anchor_cv - best_cv
    delta_mpc = None if anchor_mpc is None or best_mpc is None else anchor_mpc - best_mpc
    delta_score = (
        None if math.isinf(anchor_score) or math.isinf(best_score) else anchor_score - best_score
    )

    print(
        "Best prior  : "
        f"run_id={best.get('run_id', '')} "
        f"(label={best.get('run_label', '')}, score={best_score:.4f})"
    )
    print(
        "Latest delta vs best prior : "
        f"smoothed_load_cv={delta_cv if delta_cv is not None else 'NA'} "
        f"moves_per_cycle={delta_mpc if delta_mpc is not None else 'NA'} "
        f"score={delta_score if delta_score is not None else 'NA'}"
    )
    print()


def print_table(rows_with_roles: Sequence[Tuple[str, Dict[str, str]]]) -> None:
    header = (
        f"{'role':<6} {'run_id':<15} {'label':<20} {'status':<7} "
        f"{'sm_cv':>8} {'sm_max':>8} {'mv/cyc':>8} {'score':>8} {'git':<10}"
    )
    print(header)
    print("-" * len(header))
    for role, row in rows_with_roles:
        sm_cv = as_float(row.get("smoothed_load_cv", ""))
        sm_max = as_float(row.get("smoothed_load_max_over_mean", ""))
        mv_cyc = as_float(row.get("moves_per_cycle", ""))
        score_value = score(row)
        score_text = fmt_num(None if math.isinf(score_value) else score_value, width=8)

        print(
            f"{role:<6} "
            f"{row.get('run_id', ''):<15} "
            f"{row.get('run_label', ''):<20.20} "
            f"{row.get('status', ''):<7} "
            f"{fmt_num(sm_cv)} "
            f"{fmt_num(sm_max)} "
            f"{fmt_num(mv_cyc)} "
            f"{score_text} "
            f"{row.get('git_sha', ''):<10.10}"
        )


def main() -> int:
    args = parse_args()
    runs_root = os.path.abspath(args.runs_root)
    index_file = os.path.join(runs_root, "index.csv")

    rows = load_rows(index_file)
    anchor = find_anchor(rows, args.run_id)

    scenario = scenario_key(anchor)
    scenario_rows = [row for row in rows if scenario_key(row) == scenario]
    # Keep historical order from index.
    anchor_idx = scenario_rows.index(anchor)
    prior_rows = scenario_rows[:anchor_idx]

    best_prior: Optional[Dict[str, str]] = None
    ok_prior = [row for row in prior_rows if row.get("status", "") == "ok"]
    if ok_prior:
        best_prior = min(ok_prior, key=score)

    recent_prior = list(reversed(prior_rows[-max(0, args.recent) :]))

    rows_with_roles: List[Tuple[str, Dict[str, str]]] = [("latest", anchor)]
    for idx, row in enumerate(recent_prior, start=1):
        rows_with_roles.append((f"prev{idx}", row))

    if best_prior is not None and best_prior not in [row for _, row in rows_with_roles]:
        rows_with_roles.append(("best", best_prior))

    print_header(anchor, runs_root, scenario_rows)
    print_delta(anchor, best_prior)
    print_table(rows_with_roles)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
