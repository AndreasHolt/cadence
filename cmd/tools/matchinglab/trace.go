package main

import (
	"encoding/csv"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"time"
)

const (
	defaultTraceInterval          = 10 * time.Second
	defaultTraceQPSScale          = 1.0
	defaultTraceTimeScale         = 1.0
	defaultTracePollerCapacityQPS = 50.0
	defaultTraceTaskListPrefix    = "trace-tl-"
)

type traceConfig struct {
	Path              string        `yaml:"path"`
	Interval          time.Duration `yaml:"interval"`
	QPSScale          float64       `yaml:"qps_scale"`
	TimeScale         float64       `yaml:"time_scale"`
	TopN              int           `yaml:"top_n"`
	StartRow          int           `yaml:"start_row"`
	Rows              int           `yaml:"rows"`
	PollerCapacityQPS float64       `yaml:"poller_capacity_qps"`
	ProcessTime       time.Duration `yaml:"process_time"`
	TaskListPrefix    string        `yaml:"tasklist_prefix"`
}

type traceEvent struct {
	at         time.Duration
	taskList   string
	workflowID string
}

type traceColumn struct {
	index  int
	maxQPS float64
}

func (c traceConfig) enabled() bool {
	return c.Path != ""
}

func (c *traceConfig) setDefaults() {
	if !c.enabled() {
		return
	}
	if c.Interval <= 0 {
		c.Interval = defaultTraceInterval
	}
	if c.QPSScale <= 0 {
		c.QPSScale = defaultTraceQPSScale
	}
	if c.TimeScale <= 0 {
		c.TimeScale = defaultTraceTimeScale
	}
	if c.PollerCapacityQPS <= 0 {
		c.PollerCapacityQPS = defaultTracePollerCapacityQPS
	}
	if c.TaskListPrefix == "" {
		c.TaskListPrefix = defaultTraceTaskListPrefix
	}
}

func buildTraceWorkload(cfg traceConfig, runID string) (*workload, error) {
	rows, err := loadTraceRows(cfg.Path)
	if err != nil {
		return nil, err
	}
	rows = selectTraceRows(rows, cfg.StartRow, cfg.Rows)
	if len(rows) == 0 {
		return nil, fmt.Errorf("trace has no selected rows")
	}

	columns := selectTraceColumns(rows, cfg.TopN)
	if len(columns) == 0 {
		return nil, fmt.Errorf("trace has no selected task-list columns")
	}

	taskLists := makeTraceTaskLists(columns, cfg)
	events := makeTraceEvents(rows, columns, cfg, runID)
	if len(events) == 0 {
		return nil, fmt.Errorf("trace generated no events")
	}

	sort.Slice(events, func(i, j int) bool {
		if events[i].at == events[j].at {
			if events[i].taskList == events[j].taskList {
				return events[i].workflowID < events[j].workflowID
			}
			return events[i].taskList < events[j].taskList
		}
		return events[i].at < events[j].at
	})

	duration := events[len(events)-1].at + scaledTraceInterval(cfg) + defaultTraceInterval
	return &workload{
		taskLists: taskLists,
		events:    events,
		duration:  duration,
	}, nil
}

func loadTraceRows(path string) ([][]float64, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	records, err := csv.NewReader(file).ReadAll()
	if err != nil {
		return nil, err
	}

	rows := make([][]float64, 0, len(records))
	expectedColumns := 0
	for rowNumber, record := range records {
		if len(record) < 2 {
			return nil, fmt.Errorf("trace row %d has no load columns", rowNumber)
		}
		if expectedColumns == 0 {
			expectedColumns = len(record) - 1
		} else if len(record)-1 != expectedColumns {
			return nil, fmt.Errorf("trace row %d has %d load columns, want %d", rowNumber, len(record)-1, expectedColumns)
		}

		loads := make([]float64, 0, len(record)-1)
		for columnNumber, raw := range record[1:] {
			load, err := strconv.ParseFloat(raw, 64)
			if err != nil {
				return nil, fmt.Errorf("parse trace row %d column %d: %w", rowNumber, columnNumber+1, err)
			}
			if load < 0 || math.IsNaN(load) || math.IsInf(load, 0) {
				return nil, fmt.Errorf("trace row %d column %d has invalid load %q", rowNumber, columnNumber+1, raw)
			}
			loads = append(loads, load)
		}
		rows = append(rows, loads)
	}

	return rows, nil
}

func selectTraceRows(rows [][]float64, startRow int, rowCount int) [][]float64 {
	if startRow >= len(rows) {
		return nil
	}
	rows = rows[startRow:]
	if rowCount > 0 && rowCount < len(rows) {
		rows = rows[:rowCount]
	}
	return rows
}

func selectTraceColumns(rows [][]float64, topN int) []traceColumn {
	columnCount := len(rows[0])
	columns := make([]traceColumn, columnCount)
	for column := range columnCount {
		columns[column].index = column
	}

	for _, row := range rows {
		for column, load := range row {
			if load > columns[column].maxQPS {
				columns[column].maxQPS = load
			}
		}
	}

	sort.Slice(columns, func(i, j int) bool {
		if columns[i].maxQPS == columns[j].maxQPS {
			return columns[i].index < columns[j].index
		}
		return columns[i].maxQPS > columns[j].maxQPS
	})

	selected := columns
	if topN > 0 && topN < len(selected) {
		selected = selected[:topN]
	}
	sort.Slice(selected, func(i, j int) bool {
		return selected[i].index < selected[j].index
	})
	return selected
}

func makeTraceTaskLists(columns []traceColumn, cfg traceConfig) []taskListConfig {
	taskLists := make([]taskListConfig, 0, len(columns))
	for _, column := range columns {
		pollers := int(math.Ceil(column.maxQPS * cfg.QPSScale / cfg.PollerCapacityQPS))
		if pollers < 1 {
			pollers = 1
		}
		taskLists = append(taskLists, taskListConfig{
			Name:        traceTaskListName(cfg.TaskListPrefix, column.index),
			Weight:      1,
			Pollers:     pollers,
			ProcessTime: cfg.ProcessTime,
		})
	}
	return taskLists
}

func makeTraceEvents(rows [][]float64, columns []traceColumn, cfg traceConfig, runID string) []traceEvent {
	rowInterval := scaledTraceInterval(cfg)
	events := make([]traceEvent, 0)
	for rowIndex, row := range rows {
		rowStart := time.Duration(rowIndex) * rowInterval
		for _, column := range columns {
			taskList := traceTaskListName(cfg.TaskListPrefix, column.index)
			eventCount := int(math.Round(row[column.index] * cfg.QPSScale * rowInterval.Seconds()))
			if eventCount <= 0 {
				continue
			}
			spacing := rowInterval / time.Duration(eventCount)
			for eventIndex := range eventCount {
				workflowID := fmt.Sprintf("trace-%s-row-%06d-tl-%04d-seq-%06d", runID, rowIndex, column.index, eventIndex)
				events = append(events, traceEvent{
					at:         rowStart + time.Duration(eventIndex)*spacing,
					taskList:   taskList,
					workflowID: workflowID,
				})
			}
		}
	}
	return events
}

func scaledTraceInterval(cfg traceConfig) time.Duration {
	return time.Duration(float64(cfg.Interval) / cfg.TimeScale)
}

func traceTaskListName(prefix string, column int) string {
	return fmt.Sprintf("%s%04d", prefix, column)
}
