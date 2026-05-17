#!/usr/bin/env bash
# fix-date-format.sh
#
# Reformat the first column of a CSV from DD-MM-YYYY HH:MM:SS
# to YYYY-MM-DD HH:MM:SS so cmd/balance-sim/main.go can parse it.
#
# Usage: ./fix-date-format.sh <input.csv> [output.csv]
# If output.csv is omitted, prints to stdout.

set -euo pipefail

if [ $# -lt 1 ] || [ $# -gt 2 ]; then
    echo "Usage: $0 <input.csv> [output.csv]" >&2
    exit 1
fi

input="$1"
output="${2:-}"

if [ ! -f "$input" ]; then
    echo "Error: file not found: $input" >&2
    exit 1
fi

convert() {
    awk 'BEGIN { FS=OFS="," }
    {
        split($1, dt, " ")
        split(dt[1], d, "-")
        $1 = d[3] "-" d[2] "-" d[1] " " dt[2]
        print
    }' "$1"
}

if [ -n "$output" ]; then
    convert "$input" > "$output"
    echo "Wrote fixed CSV to $output"
else
    convert "$input"
fi
