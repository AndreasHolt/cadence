package handler

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestActiveWorkTrackerSmoothsTowardCurrentValue(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	tracker := activeWorkTracker{}

	require.Equal(t, 0.0, tracker.ObserveAt(now, 0))

	value := tracker.ObserveAt(now.Add(30*time.Second), 10)
	require.Greater(t, value, 0.0)
	require.Less(t, value, 10.0)

	value = tracker.ObserveAt(now.Add(90*time.Second), 0)
	require.Greater(t, value, 0.0)
	require.Less(t, value, 10.0)
}
