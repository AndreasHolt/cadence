#!/usr/bin/env python3
import argparse
import json
import os
import re


def parse_args():
    parser = argparse.ArgumentParser(
        description="Render a LaTeX table of shard distributor run configuration."
    )
    parser.add_argument("--config", default="config/development.yaml")
    parser.add_argument("--startenv", default="startenv.bash")
    parser.add_argument("--namespace", default="shard-distributor-replay")
    parser.add_argument("--out", default="plots/run_config.tex")
    parser.add_argument("--json-out", default="")
    parser.add_argument("--executors", default="")
    parser.add_argument("--replay-speed", default="")
    parser.add_argument("--replay-csv", default="")
    parser.add_argument("--start", default="")
    parser.add_argument("--end", default="")
    return parser.parse_args()


def parse_kv(line):
    if ":" not in line:
        return None, None
    key, val = line.split(":", 1)
    return key.strip(), val.strip().strip('"').strip("'")


def parse_development_yaml(path, target_namespace):
    result = {}
    in_shard_distribution = False
    in_process = False
    in_load_balance = False
    in_namespaces = False
    current_namespace = None

    if not os.path.exists(path):
        return result

    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.split("#", 1)[0].rstrip()
            if not line.strip():
                continue
            indent = len(line) - len(line.lstrip(" "))
            stripped = line.strip()

            if indent == 0:
                in_shard_distribution = stripped.startswith("shardDistribution:")
                in_process = False
                in_load_balance = False
                in_namespaces = False
                current_namespace = None
                continue

            if not in_shard_distribution:
                continue

            if indent == 2:
                in_process = stripped.startswith("process:")
                in_namespaces = stripped.startswith("namespaces:")
                if not in_process:
                    in_load_balance = False
                if not in_namespaces:
                    current_namespace = None
                continue

            if in_process:
                if indent == 4 and stripped.startswith("loadBalance:"):
                    in_load_balance = True
                    continue
                if indent == 4:
                    key, val = parse_kv(stripped)
                    if key in ("period", "heartbeatTTL"):
                        result[key] = val
                if indent <= 2:
                    in_process = False
                    in_load_balance = False

            if in_load_balance:
                if indent == 6:
                    key, val = parse_kv(stripped)
                    if key in (
                        "disableBenefitGating",
                        "severeImbalanceRatio",
                        "moveBudgetProportion",
                        "hysteresisUpperBand",
                        "hysteresisLowerBand",
                    ):
                        result[key] = val
                elif indent <= 4:
                    in_load_balance = False

            if in_namespaces:
                if indent == 4 and stripped.startswith("- name:"):
                    current_namespace = stripped.split(":", 1)[1].strip()
                    continue
                if indent == 6 and current_namespace == target_namespace:
                    key, val = parse_kv(stripped)
                    if key == "shardNum":
                        result["shardNum"] = val
                if indent <= 2:
                    in_namespaces = False
                    current_namespace = None

    return result


def parse_startenv(path):
    result = {}
    if not os.path.exists(path):
        return result

    patterns = {
        "CANARY_CSV": "replay_csv",
        "CANARY_NAMESPACE": "namespace",
        "CANARY_EXECUTORS": "executors",
    }

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            for var, key in patterns.items():
                if not line.startswith(var):
                    continue
                match = re.search(r":-([^}]+)\}", line)
                if not match:
                    continue
                result[key] = match.group(1).strip().strip('"').strip("'")
    return result


def format_tex_table(rows):
    lines = [
        "\\begin{tabular}{ll}",
        "\\textbf{Parameter} & \\textbf{Value} \\\\",
    ]
    for key, value in rows:
        lines.append(f"{key} & {value} \\\\")
    lines.append("\\end{tabular}")
    return "\n".join(lines) + "\n"


def main():
    args = parse_args()
    cfg = parse_development_yaml(args.config, args.namespace)
    env = parse_startenv(args.startenv)

    if args.executors:
        env["executors"] = args.executors
    if args.replay_speed:
        env["replay_speed"] = args.replay_speed
    if args.replay_csv:
        env["replay_csv"] = args.replay_csv
    if args.start:
        env["start"] = args.start
    if args.end:
        env["end"] = args.end

    rows = [
        ("namespace", args.namespace),
        ("period", cfg.get("period", "unknown")),
        ("heartbeatTTL", cfg.get("heartbeatTTL", "unknown")),
        ("disableBenefitGating", cfg.get("disableBenefitGating", "unknown")),
        ("severeImbalanceRatio", cfg.get("severeImbalanceRatio", "unknown")),
        ("moveBudgetProportion", cfg.get("moveBudgetProportion", "unknown")),
        ("hysteresisUpperBand", cfg.get("hysteresisUpperBand", "unknown")),
        ("hysteresisLowerBand", cfg.get("hysteresisLowerBand", "unknown")),
        ("shardNum", cfg.get("shardNum", "unknown")),
        ("executors", env.get("executors", "unknown")),
        ("replay_speed", env.get("replay_speed", "unknown")),
        ("replay_csv", env.get("replay_csv", "unknown")),
        ("start", env.get("start", "")),
        ("end", env.get("end", "")),
    ]

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        f.write(format_tex_table(rows))
    print(f"wrote {args.out}")

    if args.json_out:
        data = {key: value for key, value in rows if value}
        with open(args.json_out, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        print(f"wrote {args.json_out}")


if __name__ == "__main__":
    main()
