package raft

import (
	"math/rand"
	"time"
)

/*
 * Utility functions
 */

// Get a random positive duration with Millisecond granularity in a given range of durations
//   - when min > max, the values are inverted so the result will always be positive
func getRandomDuration(dMin, dMax time.Duration) time.Duration {

	var tMin int64 = 0
	var tMax int64 = 0

	tMin = dMin.Milliseconds()
	tMax = dMax.Milliseconds()

	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	n := rnd.Int63n(tMax-tMin+1) + tMin

	return time.Duration(n).Abs() * time.Microsecond
}
