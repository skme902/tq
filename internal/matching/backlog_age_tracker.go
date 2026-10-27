// Ported from temporalio/temporal v1.32.1 af08a5e5, service/matching/backlog_age_tracker.go
// (gods treemap replaced by tidwall/btree.Map).
// Copyright (c) 2025 Temporal Technologies Inc. MIT License; see UPSTREAM_LICENSE and NOTICE.

package matching

import (
	"time"

	"github.com/tidwall/btree"
)

// backlogAgeTracker is not safe for concurrent use
type backlogAgeTracker struct {
	tree *btree.Map[int64, int] // unix nano -> count
}

func newBacklogAgeTracker() backlogAgeTracker {
	return backlogAgeTracker{tree: new(btree.Map[int64, int])}
}

// record adds or removes a task from the tracker.
func (b backlogAgeTracker) record(ts time.Time, delta int) {
	if ts.IsZero() {
		return
	}
	createTime := ts.UnixNano()
	count := delta
	if prev, ok := b.tree.Get(createTime); ok {
		count += prev
	}
	if count = max(0, count); count == 0 {
		b.tree.Delete(createTime)
	} else {
		b.tree.Set(createTime, count)
	}
}

// oldestTime returns the time of the oldest task in this backlog, or
// the zero Time if empty.
func (b backlogAgeTracker) oldestTime() time.Time {
	k, _, ok := b.tree.Min()
	if !ok {
		return time.Time{}
	}
	return time.Unix(0, k)
}
