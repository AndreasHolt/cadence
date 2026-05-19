#!/usr/bin/env python3
import argparse
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_RESULTS_DIR = Path("environment/kind-lab/results")
DEFAULT_PROMETHEUS_DIR = DEFAULT_RESULTS_DIR / "prometheus"

RUN_LABELS = {
    "off": "Greedy baseline",
    "greedy": "Greedy baseline",
    "baseline": "Greedy baseline",
    "latency": "Latency aware greedy",
    "cpu": "CPU utilization aware greedy",
    "cpu-seconds": "CPU utilization aware greedy",
    "cpu_seconds": "CPU utilization aware greedy",
    "cpuseconds": "CPU utilization aware greedy",
}


def safe_filename(value):
    return "".join(char if char.isalnum() or char in "._-" else "_" for char in value)


def normalize(value):
    return re.sub(r"[^a-z0-9]", "", value.lower())


def clean_label(value, fuzzy=True):
    key = value.strip().lower().replace("_", "-")
    if key in RUN_LABELS:
        return RUN_LABELS[key]
    if not fuzzy:
        return value.strip()
    compact = normalize(value)
    if "latency" in compact:
        return RUN_LABELS["latency"]
    if "cpusecond" in compact or compact.startswith("cpu"):
        return RUN_LABELS["cpu-seconds"]
    if "off" in compact or "baseline" in compact or "greedy" in compact:
        return RUN_LABELS["off"]
    cleaned = value.replace("_", " ").replace("-", " ").strip()
    return " ".join(word.capitalize() if word.islower() else word for word in cleaned.split())


def parse_run_arg(value):
    if "=" in value:
        label, path = value.split("=", 1)
        if not label.strip() or not path.strip():
            raise argparse.ArgumentTypeError("run must be LABEL=PATH or PATH")
        return clean_label(label, fuzzy=False), Path(path)
    path = Path(value)
    return clean_label(path.stem), path


def matching_stem_files(directory, stem, suffix):
    if not directory.exists():
        return []
    wanted = normalize(stem)
    return sorted(
        candidate
        for candidate in directory.glob(f"*{suffix}")
        if normalize(candidate.stem) == wanted
    )


def resolve_file(path, suffix, results_dir):
    candidates = []
    if path.suffix == suffix:
        candidates.append(path)
    elif path.suffix:
        candidates.append(path.with_suffix(suffix))
    else:
        candidates.append(path.with_suffix(suffix))

    if not path.is_absolute() and len(path.parts) == 1:
        candidates.append(results_dir / path.with_suffix(suffix))

    for candidate in candidates:
        if candidate.exists():
            return candidate

    search_dirs = []
    if path.parent != Path("."):
        search_dirs.append(path.parent)
    search_dirs.append(results_dir)

    stem = path.stem if path.suffix else path.name
    for directory in search_dirs:
        matches = matching_stem_files(directory, stem, suffix)
        if matches:
            return matches[0]

    tried = ", ".join(str(candidate) for candidate in candidates)
    raise FileNotFoundError(f"could not find {suffix} for {path}; tried {tried}")


def prometheus_candidates(label, csv_path, log_path):
    names = [
        label,
        safe_filename(label),
        csv_path.stem,
        log_path.stem,
    ]
    if "CPU utilization" in label or "CPU-time" in label:
        names.extend(["cpu-seconds", "cpu_seconds", "cpuseconds"])
    elif "Latency" in label:
        names.extend(["latency", "greedy-latency"])
    elif "Greedy baseline" in label:
        names.extend(["off", "greedy", "baseline"])

    seen = set()
    for name in names:
        if name in seen:
            continue
        seen.add(name)
        yield name


def resolve_churn_csv(label, csv_path, log_path, prometheus_dir):
    for name in prometheus_candidates(label, csv_path, log_path):
        candidate = prometheus_dir / name / "csv" / "sd_load_based_moves_total.csv"
        if candidate.exists():
            return candidate
    return None


def parse_run_id_timestamp(value):
    # matching-lab run ids use Go layout 20060102T150405.000000000 in UTC.
    try:
        date_part, frac_part = value.split(".", 1)
    except ValueError:
        date_part, frac_part = value, ""
    parsed = datetime.strptime(date_part, "%Y%m%dT%H%M%S").replace(tzinfo=timezone.utc)
    if frac_part:
        micros = int((frac_part[:6]).ljust(6, "0"))
        parsed = parsed.replace(microsecond=micros)
    return parsed


def infer_run_start_from_log(path):
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            match = re.search(r"^run id:\s+(\S+)", line.strip())
            if match:
                try:
                    return parse_run_id_timestamp(match.group(1))
                except ValueError:
                    return None
    return None


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Generate the paper-style kind-lab plots from matching-lab logs, "
            "sample-utilization CSVs, and optional Prometheus exports."
        )
    )
    parser.add_argument(
        "--run",
        action="append",
        type=parse_run_arg,
        required=True,
        help=(
            "Run as LABEL=PATH or PATH. PATH can be a stem, .csv, or .log. "
            "The matching .csv/.log is found by the same normalized stem, so "
            "cpuseconds-1hr.csv and cpu-seconds-1hr.log match."
        ),
    )
    parser.add_argument("--results-dir", type=Path, default=DEFAULT_RESULTS_DIR)
    parser.add_argument("--prometheus-dir", type=Path, default=DEFAULT_PROMETHEUS_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_RESULTS_DIR / "plots")
    parser.add_argument("--prefix", default="kind-lab-report")
    parser.add_argument("--x-min", type=float, default=None, help="Minimum x-axis value in seconds.")
    parser.add_argument("--x-max", type=float, default=None, help="Maximum x-axis value in seconds.")
    parser.add_argument("--cpu-y-max", type=float, default=5.0)
    parser.add_argument("--split-by-pod", action="store_true")
    parser.add_argument(
        "--split-utilization-by-run",
        action="store_true",
        help=(
            "Generate utilization figures as one panel per run. This is usually "
            "more readable than the combined nine-line CPU plot."
        ),
    )
    parser.add_argument(
        "--show-cpu-limits",
        action="store_true",
        help="Draw per-executor CPU-limit reference lines on utilization plots.",
    )
    parser.add_argument("--no-started-line", action="store_true")
    parser.add_argument(
        "--throughput-smooth-samples",
        type=int,
        default=1,
        help=(
            "Rolling mean window, in matching-lab summary samples, for throughput. "
            "With the default 10s summary interval, 6 means a 60s rolling mean."
        ),
    )
    parser.add_argument(
        "--latency-y-max-seconds",
        type=float,
        default=None,
        help="Maximum y-axis value for p95 latency plots in seconds. Use 0 to auto-scale.",
    )
    parser.add_argument("--mark-errors", action="store_true")
    parser.add_argument("--skip-utilization", action="store_true")
    parser.add_argument("--skip-matching", action="store_true")
    args = parser.parse_args()

    resolved = []
    for label, path in args.run:
        csv_path = resolve_file(path, ".csv", args.results_dir)
        log_path = resolve_file(path, ".log", args.results_dir)
        churn_path = resolve_churn_csv(label, csv_path, log_path, args.prometheus_dir)
        run_start = infer_run_start_from_log(log_path)
        resolved.append((label, csv_path, log_path, churn_path, run_start))

    if not args.skip_utilization:
        cmd = [
            sys.executable,
            str(SCRIPT_DIR / "plot-utilization.py"),
            "--output-dir",
            str(args.output_dir),
            "--prefix",
            args.prefix,
            "--cpu-y-max",
            str(args.cpu_y_max),
        ]
        if args.x_min is not None:
            cmd.extend(["--x-min", str(args.x_min)])
        elif any(run_start is not None for _, _, _, _, run_start in resolved):
            cmd.extend(["--x-min", "0"])
        if args.x_max is not None:
            cmd.extend(["--x-max", str(args.x_max)])
        if args.split_by_pod:
            cmd.append("--split-by-pod")
        if args.split_utilization_by_run:
            cmd.append("--split-by-run")
        if args.show_cpu_limits:
            cmd.append("--show-cpu-limits")
        for label, csv_path, _, _, run_start in resolved:
            cmd.extend(["--run", f"{label}={csv_path}"])
            if run_start is not None:
                cmd.extend(["--run-start", f"{label}={run_start.isoformat()}"])
        subprocess.run(cmd, check=True)

    if not args.skip_matching:
        cmd = [
            sys.executable,
            str(SCRIPT_DIR / "plot-matching-lab.py"),
            "--output-dir",
            str(args.output_dir),
            "--prefix",
            args.prefix,
            "--no-auto-churn",
        ]
        if args.x_min is not None:
            cmd.extend(["--x-min", str(args.x_min)])
        if args.x_max is not None:
            cmd.extend(["--x-max", str(args.x_max)])
        if args.no_started_line:
            cmd.append("--no-started-line")
        if args.throughput_smooth_samples > 1:
            cmd.extend(["--throughput-smooth-samples", str(args.throughput_smooth_samples)])
        if args.latency_y_max_seconds is not None:
            cmd.extend(["--latency-y-max-seconds", str(args.latency_y_max_seconds)])
        if args.mark_errors:
            cmd.append("--mark-errors")
        for label, _, log_path, churn_path, _ in resolved:
            cmd.extend(["--run", f"{label}={log_path}"])
            if churn_path is not None:
                cmd.extend(["--churn-run", f"{label}={churn_path}"])
            else:
                print(f"warning: no Prometheus shard-move export found for {label}", file=sys.stderr)
        subprocess.run(cmd, check=True)


if __name__ == "__main__":
    main()
