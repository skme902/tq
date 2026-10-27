// Ported from temporalio/temporal v1.32.1 af08a5e5, service/matching/liveness_test.go
// Copyright (c) 2025 Temporal Technologies Inc. MIT License; see UPSTREAM_LICENSE and NOTICE.

package matching

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/skme902/tq/clock"
	"github.com/stretchr/testify/assert"
)

func TestLiveness(t *testing.T) {
	t.Parallel()
	var idleCalled atomic.Int32
	ttl := func() time.Duration { return 2500 * time.Millisecond }
	timeSource := clock.NewEventTimeSource()
	liveness := newLiveness(timeSource, ttl, func() { idleCalled.Store(1) })
	liveness.Start()
	timeSource.Advance(1 * time.Second)
	assert.Equal(t, int32(0), idleCalled.Load())
	liveness.markAlive()
	timeSource.Advance(1 * time.Second)
	assert.Equal(t, int32(0), idleCalled.Load())
	liveness.markAlive()
	timeSource.Advance(1 * time.Second)
	assert.Equal(t, int32(0), idleCalled.Load())
	timeSource.Advance(1 * time.Second)
	assert.Equal(t, int32(0), idleCalled.Load())
	timeSource.Advance(1 * time.Second)
	assert.Equal(t, int32(1), idleCalled.Load())
	liveness.Stop()
}

func TestLivenessStop(t *testing.T) {
	t.Parallel()
	var idleCalled atomic.Int32
	ttl := func() time.Duration { return 1000 * time.Millisecond }
	timeSource := clock.NewEventTimeSource()
	liveness := newLiveness(timeSource, ttl, func() { idleCalled.Store(1) })
	liveness.Start()
	timeSource.Advance(500 * time.Millisecond)
	liveness.Stop()
	timeSource.Advance(1 * time.Second)
	assert.Equal(t, int32(0), idleCalled.Load())
	liveness.markAlive() // should not panic
}
