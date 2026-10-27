// Ported from temporalio/temporal v1.32.1 af08a5e5, service/matching/liveness.go
// Copyright (c) 2025 Temporal Technologies Inc. MIT License; see UPSTREAM_LICENSE and NOTICE.

package matching

import (
	"sync/atomic"
	"time"

	"github.com/skme902/tq/clock"
)

type (
	liveness struct {
		timeSource clock.TimeSource
		ttl        func() time.Duration
		onIdle     func()
		timer      atomic.Value
	}

	timerWrapper struct {
		clock.Timer
	}
)

func newLiveness(
	timeSource clock.TimeSource,
	ttl func() time.Duration,
	onIdle func(),
) *liveness {
	return &liveness{
		timeSource: timeSource,
		ttl:        ttl,
		onIdle:     onIdle,
	}
}

func (l *liveness) Start() {
	l.timer.Store(timerWrapper{l.timeSource.AfterFunc(l.ttl(), l.onIdle)})
}

func (l *liveness) Stop() {
	if t, ok := l.timer.Swap(timerWrapper{}).(timerWrapper); ok && t.Timer != nil {
		t.Stop()
	}
}

func (l *liveness) markAlive() {
	if t, ok := l.timer.Load().(timerWrapper); ok && t.Timer != nil {
		t.Reset(l.ttl())
	}
}
