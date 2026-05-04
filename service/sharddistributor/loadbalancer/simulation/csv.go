package simulation

import (
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

const timestampLayout = "2006-01-02 15:04:05"

// LoadHistoryRow is one timestamped snapshot of per-shard load values.
type LoadHistoryRow struct {
	Timestamp  time.Time
	ShardLoads map[string]float64
}

// LoadCSVHistory reads rows formatted as:
//
//	<timestamp>, <shard_0_load>, <shard_1_load>, ...
//
// Shard IDs are generated from the load column index: "0", "1", "2", ...
// If the first row is not parseable as a timestamp, it is treated as a header.
func LoadCSVHistory(r io.Reader, maxRows int) ([]LoadHistoryRow, []string, error) {
	reader := csv.NewReader(r)
	reader.TrimLeadingSpace = true
	reader.ReuseRecord = true

	var rows []LoadHistoryRow
	var shardIDs []string

	for rowIdx := 0; ; rowIdx++ {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, fmt.Errorf("read csv row %d: %w", rowIdx, err)
		}
		if len(record) < 2 {
			continue
		}

		ts, err := parseTimestamp(record[0])
		if err != nil {
			if len(rows) == 0 {
				continue
			}
			return nil, nil, fmt.Errorf("row %d: parse timestamp %q: %w", rowIdx, record[0], err)
		}

		if len(shardIDs) == 0 {
			shardIDs = make([]string, len(record)-1)
			for i := range shardIDs {
				shardIDs[i] = strconv.Itoa(i)
			}
		}
		if len(record)-1 != len(shardIDs) {
			return nil, nil, fmt.Errorf("row %d: expected %d load columns, got %d", rowIdx, len(shardIDs), len(record)-1)
		}

		loads := make(map[string]float64, len(shardIDs))
		for i, raw := range record[1:] {
			load, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
			if err != nil {
				load = 0
			}
			loads[shardIDs[i]] = load
		}

		rows = append(rows, LoadHistoryRow{
			Timestamp:  ts,
			ShardLoads: loads,
		})
		if maxRows > 0 && len(rows) >= maxRows {
			break
		}
	}

	if len(rows) == 0 {
		return nil, nil, fmt.Errorf("csv contains no data rows")
	}

	return rows, shardIDs, nil
}

func parseTimestamp(raw string) (time.Time, error) {
	s := strings.TrimPrefix(strings.TrimSpace(raw), "\ufeff")
	if ts, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return ts.UTC(), nil
	}
	if ts, err := time.ParseInLocation(timestampLayout, s, time.UTC); err == nil {
		return ts.UTC(), nil
	}
	return time.Time{}, fmt.Errorf("unsupported timestamp format")
}
