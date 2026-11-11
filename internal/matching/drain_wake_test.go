package matching

import (
	"context"
	"sync/atomic"
	"time"
)

// A poller that queues once the engine is draining ends at once (the race with the Shutdown
// sweep: the flag is set before the sweep, so a poller the sweep missed sees it under lock).
func (s *MatcherDataSuite) TestDrainFlagEndsLatePoller() {
	var draining atomic.Bool
	draining.Store(true)
	s.md.draining = &draining
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res := make(chan *matchResult, 1)
	go func() { res <- s.pollContext(ctx, nil) }()
	select {
	case r := <-res:
		s.ErrorIs(r.ctxErr, errDrainWake)
		s.Equal(-1, r.ctxErrIdx)
	case <-time.After(2 * time.Second):
		s.Fail("poller queued while draining was not ended")
	}
	s.Zero(s.md.pollers.Len())
}

// wakePollersForDrain ends every waiting poller and leaves the list empty.
func (s *MatcherDataSuite) TestWakePollersForDrain() {
	var draining atomic.Bool
	s.md.draining = &draining
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	const n = 8
	res := make(chan *matchResult, n)
	for range n {
		go func() { res <- s.pollContext(ctx, nil) }()
	}
	s.Eventually(func() bool {
		s.md.lock.Lock()
		defer s.md.lock.Unlock()
		return s.md.pollers.Len() == n
	}, 2*time.Second, time.Millisecond)
	draining.Store(true)
	s.md.wakePollersForDrain()
	for range n {
		select {
		case r := <-res:
			s.ErrorIs(r.ctxErr, errDrainWake)
		case <-time.After(2 * time.Second):
			s.Fail("waiting poller was not woken by the drain sweep")
			return
		}
	}
	s.Zero(s.md.pollers.Len())
}
