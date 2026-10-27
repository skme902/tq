// Ported from temporalio/temporal v1.32.1 af08a5e5, service/matching/counter/counter.go
// Copyright (c) 2025 Temporal Technologies Inc. MIT License; see UPSTREAM_LICENSE and NOTICE.

package counter

type Counter interface {
	// GetPass tracks a value per key, which may be lossy or approximate. It increments the
	// value by inc and returns the new value. The value returned must be >= base.
	GetPass(key string, base, inc int64) int64
	// EstimateDistinctKeys returns an estimate of the number of distinct keys in the counter.
	EstimateDistinctKeys() int
	// Get the counts for a small number of top keys (the maximum keys returned will be limited
	// by the Counter's configuration). Not guaranteed to return the exact top.
	TopK() []TopKEntry
}

// TopKEntry is a count for a value with high count.
type TopKEntry struct {
	Key   string
	Count int64
}
