package raft

import (
	"math/rand"
	"slices"
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

	if dMin <= dMax {
		tMin = dMin.Milliseconds()
		tMax = dMax.Milliseconds()
	} else {
		// when min > max, the values are inverted so the result will always be positive
		tMin = dMax.Milliseconds()
		tMax = dMin.Milliseconds()
	}

	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	n := rnd.Int63n(tMax-tMin+1) + tMin

	return time.Duration(n).Abs() * time.Microsecond
}

// Filters map keys in the filter array
func filterIn[K comparable, V any](m map[K]V, keys []K) (res map[K]V) {

	for k, v := range m {
		if slices.Contains(keys, k) {
			res[k] = v
		}
	}

	return res
}

// Filters slice values in the filter array
func sliceFilterIn[T comparable](slice []T, filter []T) (res []T) {

	for _, v := range slice {
		if slices.Contains(filter, v) {
			res = append(res, v)
		}
	}

	return res
}

func sliceFilterOut[T comparable](slice []T, filter []T) (res []T) {

	for _, v := range slice {
		if !slices.Contains(filter, v) {
			res = append(res, v)
		}
	}

	return res
}

// Filters map keys outside the filter key
func filterOut[K comparable, V any](m map[K]V, keys []K) (res map[K]V) {

	res = map[K]V{}
	for k, v := range m {
		if !slices.Contains(keys, k) {
			res[k] = v
		}
	}

	return res
}

// Tells if the map contains the given key
func contains[K comparable, V any](m map[K]V, key K) bool {
	_, ok := m[key]

	return ok
}

// Returns a slice without the given element (if it is there)
func sliceDelete[T comparable](slice []T, element T) []T {

	for i, v := range slice {
		if v == element {
			return append(slice[:i], slice[i+1:]...)
		}
	}

	return slice
}
