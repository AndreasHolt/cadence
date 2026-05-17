// Copyright (c) 2017-2020 Uber Technologies Inc.

package handler

import (
	"math"
	"os"
	"runtime"
	"strconv"
)

const matchingLabAddTaskCPUBurnIterationsEnv = "MATCHING_LAB_ADD_TASK_CPU_BURN_ITERATIONS"

var matchingLabAddTaskCPUBurnIterations = readMatchingLabAddTaskCPUBurnIterations()

func readMatchingLabAddTaskCPUBurnIterations() int {
	raw := os.Getenv(matchingLabAddTaskCPUBurnIterationsEnv)
	if raw == "" {
		return 0
	}
	iterations, err := strconv.Atoi(raw)
	if err != nil || iterations < 0 {
		return 0
	}
	return iterations
}

func burnMatchingLabAddTaskCPU() {
	x := 1.000001
	for i := 0; i < matchingLabAddTaskCPUBurnIterations; i++ {
		x = math.Sqrt(x*x + 1.000001)
	}
	runtime.KeepAlive(x)
}
