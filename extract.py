import csv
from statistics import mean
from pathlib import Path

run_dir = Path("./")  # <-- change
files = {
    "Smoothed max/mean": "smoothed_max_over_mean.csv",
    "Reported max/mean": "reported_max_over_mean.csv",
    "Smoothed CV": "smoothed_cv.csv",
    "Reported CV": "reported_cv.csv",
}
def read_vals(path):
    vals = []
    with open(path) as f:
        r = csv.reader(f)
        next(r, None)
        for row in r:
            if row:
                vals.append(float(row[1]))
    return vals

for label, fname in files.items():
    vals = read_vals(run_dir / fname)
    print(f"{label}: worst={max(vals):.3f} avg={mean(vals):.3f}")

moves = read_vals(run_dir / "moves_per_window.csv")
avg_moves = read_vals(run_dir / "avg_moves_per_cycle.csv")
print(f"Total moves (sum of moves_per_window) = {sum(moves):.1f}")
print(f"Moves per cycle (avg) = {mean(avg_moves):.4f}")
