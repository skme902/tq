// Ported from temporalio/temporal v1.32.1 af08a5e5, service/matching/fair_level.go
// Copyright (c) 2025 Temporal Technologies Inc. MIT License; see UPSTREAM_LICENSE and NOTICE.

package matching

import (
	"fmt"
	"math"

	"github.com/skme902/tq/store"
)

// fairLevel is a pair of "pass" and "id". Pass is a number that can be assigned to cause
// levels to be ordered to preserve fairness. Id is a unique task id. fairLevels are ordered
// lexicographically.
type fairLevel struct {
	pass int64
	id   int64
}

// maxFairLevel is higher than any real level. It's used as a sentinel "trim point" meaning
// "nothing is trimmed": no real task can reach it, so ranges up to it are effectively unbounded.
var maxFairLevel = fairLevel{pass: math.MaxInt64, id: math.MaxInt64}

func (a fairLevel) String() string {
	return fmt.Sprintf("<%d,%d>", a.pass, a.id)
}

// Returns true if a < b lexicographically.
func (a fairLevel) less(b fairLevel) bool {
	return a.pass < b.pass || a.pass == b.pass && a.id < b.id
}

// Returns the max of a and b.
func (a fairLevel) max(b fairLevel) fairLevel {
	if a.less(b) {
		return b
	}
	return a
}

// Returns the next highest fair level.
func (a fairLevel) inc() fairLevel {
	return fairLevel{pass: a.pass, id: a.id + 1}
}

func fairLevelFromAllocatedTask(t *store.TaskInfo) fairLevel {
	return fairLevel{pass: t.Pass, id: t.ID}
}

// Replaces fairLevelFromProto / toProto: the store uses store.Level (zero value = unset).
func fairLevelFromStore(l store.Level) fairLevel {
	return fairLevel{pass: l.Pass, id: l.ID}
}

func (a fairLevel) toStore() store.Level {
	return store.Level{Pass: a.pass, ID: a.id}
}
