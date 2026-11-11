package matching

import (
	"time"

	"github.com/skme902/tq/clock"
)

// afterFunc is ts.AfterFunc, except that f always runs on its own goroutine, never inline on
// the clock. This is the one way engine code arms a timer (TestTimerCallbacksNeverInline
// enforces it).
//
// Why: clock.EventTimeSource runs due callbacks inside Advance/Update/AfterFunc with its own
// lock held, while engine code reads the clock and arms or stops timers while holding engine
// locks (reader, matcher, delivery). A callback that took an engine lock, or read the clock,
// inline would close the cycle clock lock -> engine lock -> clock lock and deadlock. With every
// callback asynchronous, the only lock order is engine lock -> clock lock, so timers can be
// armed, reset and stopped under any engine lock. The real clock already runs callbacks on
// their own goroutines, so it is used as is.
func afterFunc(ts clock.TimeSource, d time.Duration, f func()) clock.Timer {
	if _, real := ts.(clock.RealTimeSource); real {
		return ts.AfterFunc(d, f)
	}
	return ts.AfterFunc(d, func() { go f() })
}

// deadlineTimeSource is a clock that can arm a timer at an absolute time
// (clock.EventTimeSource).
type deadlineTimeSource interface {
	AfterFuncAt(deadline time.Time, f func()) clock.Timer
}

// afterFuncAt is afterFunc firing at deadline. On a clock with AfterFuncAt the deadline is
// exact even if the clock moves while arming; otherwise it is AfterFunc(deadline - Now), which
// for the real clock is late by at most the time between the two reads.
func afterFuncAt(ts clock.TimeSource, deadline time.Time, f func()) clock.Timer {
	if dts, ok := ts.(deadlineTimeSource); ok {
		return dts.AfterFuncAt(deadline, func() { go f() })
	}
	return afterFunc(ts, deadline.Sub(ts.Now()), f)
}
