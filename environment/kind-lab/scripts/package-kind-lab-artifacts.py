#!/usr/bin/env python3
"""Create a compact, report-ready archive for one or more kind-lab runs.

Copies the small controlled-run artifacts and selected Prometheus CSV exports,
then writes a .tar.gz archive that can be downloaded into the May21 tree.
The large Prometheus raw/ directory is intentionally excluded.
"""

from __future__ import annotations

import argparse
import json
import shutil
import tarfile
from pathlib import Path


DEFAULT_RESULTS_DIR = Path("environment/kind-lab/results")

KEEP_PROMETHEUS_CSVS = [
    # Churn / shard movement.
    "sd_load_based_moves_total.csv",
    "sd_load_based_moves_rate_1m.csv",
    "sd_load_based_single_moves_total.csv",
    "sd_load_based_single_moves_rate_1m.csv",
    "sd_load_based_swap_moves_total.csv",
    "sd_load_based_swap_moves_rate_1m.csv",
    "sd_moved_shard_load.csv",
    "sd_moved_shard_load_total.csv",
    # Assignment balance.
    "sd_assignment_load_cv.csv",
    "sd_assignment_load_max_over_mean.csv",
    "sd_assignment_smoothed_load_cv.csv",
    "sd_assignment_smoothed_load_max_over_mean.csv",
    "sd_assignment_smoothed_load_missing_ratio.csv",
    "sd_assignment_smoothed_load_stale_ratio.csv",
    "sd_executor_owned_shards.csv",
    # Health / validity signals.
    "sd_executor_heartbeat_skipped_total.csv",
    "sd_oldest_executor_heartbeat_lag.csv",
    "sd_store_failures_total.csv",
    "sd_store_requests_total.csv",
    "sd_watch_events_received_total.csv",
    # Matching Prometheus signals that complement matching-lab logs/utilization CSVs.
    "matching_cpu_usage_cores.csv",
    "matching_cpu_throttled_cores.csv",
    "matching_addtask_request_rate_1m.csv",
    "matching_addtask_error_rate_1m.csv",
    "matching_addtask_p95_latency_ns_by_instance.csv",
    "matching_service_p95_latency_ns_by_operation.csv",
    "matching_task_backlog.csv",
    "matching_task_lag.csv",
]


def copy_if_exists(src: Path, dst: Path, copied: list[str], missing: list[str]) -> None:
    if not src.exists():
        missing.append(str(src))
        return
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)
    copied.append(str(dst))


def package_run(results_dir: Path, run: str, output_dir: Path, keep_all_prom_csv: bool, remove_stage: bool) -> Path:
    stage_root = output_dir / f"{run}-compact"
    if stage_root.exists():
        shutil.rmtree(stage_root)
    stage_root.mkdir(parents=True)

    copied: list[str] = []
    missing: list[str] = []

    for suffix in [".log", ".csv", "-metadata.json"]:
        copy_if_exists(results_dir / f"{run}{suffix}", stage_root / f"{run}{suffix}", copied, missing)

    prom_dir = results_dir / run
    if prom_dir.exists():
        for name in ["metadata.json", "summary.json", "summary.csv"]:
            copy_if_exists(prom_dir / name, stage_root / run / name, copied, missing)

        csv_dir = prom_dir / "csv"
        if csv_dir.exists():
            names = sorted(p.name for p in csv_dir.glob("*.csv")) if keep_all_prom_csv else KEEP_PROMETHEUS_CSVS
            for name in names:
                copy_if_exists(csv_dir / name, stage_root / run / "csv" / name, copied, missing)
        else:
            missing.append(str(csv_dir))
    else:
        missing.append(str(prom_dir))

    manifest = {
        "run": run,
        "results_dir": str(results_dir),
        "prometheus_raw_excluded": True,
        "keep_all_prometheus_csv": keep_all_prom_csv,
        "kept_prometheus_csv_names": "all" if keep_all_prom_csv else KEEP_PROMETHEUS_CSVS,
        "copied_count": len(copied),
        "copied": copied,
        "missing": missing,
    }
    (stage_root / "compact-manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

    archive = output_dir / f"{run}-compact.tar.gz"
    if archive.exists():
        archive.unlink()
    with tarfile.open(archive, "w:gz") as tar:
        tar.add(stage_root, arcname=stage_root.name)

    if remove_stage:
        shutil.rmtree(stage_root)

    return archive


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results-dir", type=Path, default=DEFAULT_RESULTS_DIR)
    parser.add_argument("--output-dir", type=Path, default=None, help="Default: RESULTS_DIR/compact")
    parser.add_argument("--run", action="append", required=True, help="Run stem, e.g. latency2-n3. Repeatable.")
    parser.add_argument("--all-prometheus-csv", action="store_true", help="Keep every Prometheus CSV instead of the curated report set.")
    parser.add_argument("--keep-stage", action="store_true", help="Keep the unpacked compact directory next to the archive.")
    args = parser.parse_args()

    output_dir = args.output_dir or (args.results_dir / "compact")
    output_dir.mkdir(parents=True, exist_ok=True)

    for run in args.run:
        archive = package_run(args.results_dir, run, output_dir, args.all_prometheus_csv, remove_stage=not args.keep_stage)
        print(f"wrote {archive}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
