// Ported from temporalio/temporal v1.32.1 af08a5e5, common/clock/time_source.go
// Copyright (c) 2025 Temporal Technologies Inc. MIT License; see UPSTREAM_LICENSE and NOTICE.

// Package clock provides the time abstraction used by the queue so tests can control time.
package clock

import "time"

type (
	// TimeSource is the clock used by the queue.
	TimeSource interface {
		Now() time.Time
		Since(t time.Time) time.Duration
		AfterFunc(d time.Duration, f func()) Timer
		NewTimer(d time.Duration) (<-chan time.Time, Timer)
	}

	// Timer is a stoppable/resettable timer returned by a TimeSource.
	Timer interface {
		Reset(d time.Duration) bool
		Stop() bool
	}

	// RealTimeSource is a TimeSource backed by the runtime clock.
	RealTimeSource struct{}
)

var _ TimeSource = RealTimeSource{}

// NewRealTimeSource returns the real clock.
func NewRealTimeSource() RealTimeSource { return RealTimeSource{} }

func (RealTimeSource) Now() time.Time                  { return time.Now().UTC() }
func (RealTimeSource) Since(t time.Time) time.Duration { return time.Since(t) }
func (RealTimeSource) AfterFunc(d time.Duration, f func()) Timer {
	return time.AfterFunc(d, f)
}
func (RealTimeSource) NewTimer(d time.Duration) (<-chan time.Time, Timer) {
	t := time.NewTimer(d)
	return t.C, t
}
