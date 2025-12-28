#!/usr/bin/env python3
import argparse
import csv
import datetime
import os
import sys


def parse_args():
    parser = argparse.ArgumentParser(
        description="Normalize CSV timestamps to YYYY-MM-DD HH:MM:SS."
    )
    parser.add_argument("--in", dest="input_path", required=True, help="Input CSV")
    parser.add_argument("--out", dest="output_path", required=True, help="Output CSV")
    parser.add_argument(
        "--delimiter",
        default=",",
        help="CSV delimiter (default: ,)",
    )
    return parser.parse_args()


def parse_ts(raw):
    raw = raw.lstrip("\ufeff").strip()
    if not raw:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%m/%d/%y %H:%M", "%d-%m-%Y %H:%M:%S"):
        try:
            return datetime.datetime.strptime(raw, fmt)
        except ValueError:
            continue
    return None


def main():
    args = parse_args()
    if not os.path.exists(args.input_path):
        print(f"missing input: {args.input_path}", file=sys.stderr)
        sys.exit(1)

    with open(args.input_path, newline="", encoding="utf-8") as f, open(
        args.output_path, "w", newline="", encoding="utf-8"
    ) as w:
        reader = csv.reader(f, delimiter=args.delimiter)
        writer = csv.writer(w)
        for i, row in enumerate(reader, 1):
            if not row or all(not c.strip() for c in row):
                continue
            dt = parse_ts(row[0])
            if dt is None:
                raise SystemExit(f"row {i}: unsupported or empty timestamp {row[0]!r}")
            row[0] = dt.strftime("%Y-%m-%d %H:%M:%S")
            writer.writerow(row)

    print(f"wrote {args.output_path}")


if __name__ == "__main__":
    main()
