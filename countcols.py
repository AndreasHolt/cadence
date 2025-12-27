import csv
import sys

def count_rows_and_columns(csv_path: str) -> tuple[int, int, int]:
    """
    Returns (rows, header_cols, max_cols).
    - rows: number of non-empty rows
    - header_cols: columns in the first non-empty row
    - max_cols: maximum columns seen in any row (handles ragged CSVs)
    """
    rows = 0
    header_cols = None
    max_cols = 0

    with open(csv_path, newline="", encoding="utf-8") as f:
        for row in csv.reader(f):
            if not row:
                continue
            rows += 1
            if header_cols is None:
                header_cols = len(row)
            max_cols = max(max_cols, len(row))

    return rows, (header_cols or 0), max_cols

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python count_csv.py <file.csv>")
        sys.exit(2)

    rows, header_cols, max_cols = count_rows_and_columns(sys.argv[1])
    print(f"rows={rows}")
    print(f"columns(first_row)={header_cols}")
    print(f"columns(max_row)={max_cols}")