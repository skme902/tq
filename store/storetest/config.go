package storetest

// Conformance cases for store.QueueConfigStore (new; no upstream counterpart).

import (
	"sync"
	"testing"

	"github.com/skme902/tq/store"
)

func (s *suite) qcs() store.QueueConfigStore {
	c, ok := s.store.(store.QueueConfigStore)
	if !ok {
		s.t.Skip("store does not implement store.QueueConfigStore")
	}
	return c
}

func (s *suite) putConfig(c store.QueueConfigStore, key store.QueueKey, expected int64, cfg string) (*store.QueueConfigRecord, error) {
	return c.PutQueueConfig(s.ctx, &store.PutQueueConfigRequest{
		Key: key, ExpectedVersion: expected, Config: []byte(cfg), UpdatedBy: "alice", Reason: "r-" + cfg, UpdateTime: s.now(),
	})
}

func testQueueConfigVersions(_ *testing.T, s *suite) {
	c := s.qcs()
	_, err := c.GetQueueConfig(s.ctx, s.key)
	var nf *store.NotFoundError
	s.ErrorAs(err, &nf)

	r1, err := s.putConfig(c, s.key, 0, `{"a":1}`)
	s.NoError(err)
	s.EqualValues(1, r1.Version)
	var cf *store.ConditionFailedError
	_, err = s.putConfig(c, s.key, 0, `{"a":2}`)
	s.ErrorAs(err, &cf, "stale version")
	r2, err := s.putConfig(c, s.key, 1, `{"a":2}`)
	s.NoError(err)
	s.EqualValues(2, r2.Version)
	s.Greater(r2.Seq, r1.Seq)

	// The returned record does not alias the request's buffer (review config r1 nit).
	buf := []byte(`{"a":3}`)
	rA, err := c.PutQueueConfig(s.ctx, &store.PutQueueConfigRequest{Key: s.key, ExpectedVersion: 2, Config: buf,
		UpdatedBy: "alice", Reason: "r-" + string(buf), UpdateTime: s.now()})
	s.NoError(err)
	buf[1] = 'X'
	s.Equal(`{"a":3}`, string(rA.Config))

	got, err := c.GetQueueConfig(s.ctx, s.key)
	s.NoError(err)
	s.Equal(*rA, *got)
	s.Equal("alice", got.UpdatedBy)
	s.Equal(`r-{"a":3}`, got.Reason)

	hist, err := c.QueueConfigHistory(s.ctx, s.key, 10)
	s.NoError(err)
	s.Equal([]store.QueueConfigRecord{*rA, *r2, *r1}, hist)
	hist, err = c.QueueConfigHistory(s.ctx, s.key, 1)
	s.NoError(err)
	s.Equal([]store.QueueConfigRecord{*rA}, hist)

	// Changes: every write after a seq, in seq order, across queues.
	other := s.key
	other.Partition++
	r3, err := s.putConfig(c, other, 0, `{"b":1}`)
	s.NoError(err)
	ch, err := c.ListQueueConfigChanges(s.ctx, r1.Seq-1, 1000)
	s.NoError(err)
	var mine []store.QueueConfigRecord
	for _, r := range ch {
		if r.Key == s.key || r.Key == other {
			mine = append(mine, r)
		}
	}
	s.Equal([]store.QueueConfigRecord{*r1, *r2, *rA, *r3}, mine)

	_, err = c.ListQueueConfigChanges(s.ctx, 0, 0)
	var ia *store.InvalidArgumentError
	s.ErrorAs(err, &ia)
}

// Concurrent writers with the same expected version: exactly one wins, and the change feed
// has no gaps or duplicates for them.
func testQueueConfigConcurrentPut(_ *testing.T, s *suite) {
	c := s.qcs()
	before, err := c.ListQueueConfigChanges(s.ctx, 0, 1<<30)
	s.NoError(err)
	var last int64
	if len(before) > 0 {
		last = before[len(before)-1].Seq
	}
	const n = 8
	var wg sync.WaitGroup
	wins := make(chan int64, n)
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			key := s.key
			key.Name += string(rune('a' + i%2)) // two keys, n/2 writers each
			for v := int64(0); ; v++ {
				r, err := s.putConfig(c, key, v, "{}")
				if err == nil {
					wins <- r.Seq
					return
				}
				if !store.IsConditionFailed(err) {
					s.t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(wins)
	seqs := map[int64]bool{}
	for q := range wins {
		seqs[q] = true
	}
	s.Len(seqs, n)
	ch, err := c.ListQueueConfigChanges(s.ctx, last, 1<<30)
	s.NoError(err)
	for i := 1; i < len(ch); i++ {
		s.Greater(ch[i].Seq, ch[i-1].Seq)
	}
	for q := range seqs {
		found := false
		for _, r := range ch {
			found = found || r.Seq == q
		}
		s.True(found, "seq %d missing from the change feed", q)
	}
}

// ListCurrentQueueConfigs returns one row per queue, at its current version and Seq.
func testQueueConfigCurrent(_ *testing.T, s *suite) {
	c := s.qcs()
	a, b := s.key, s.key
	b.Partition += 7
	_, err := s.putConfig(c, a, 0, `{"a":1}`)
	s.NoError(err)
	rb, err := s.putConfig(c, b, 0, `{"b":1}`)
	s.NoError(err)
	ra, err := s.putConfig(c, a, 1, `{"a":2}`)
	s.NoError(err)
	cur, err := c.ListCurrentQueueConfigs(s.ctx)
	s.NoError(err)
	var mine []store.QueueConfigRecord
	for _, r := range cur {
		if r.Key == a || r.Key == b {
			mine = append(mine, r)
		}
	}
	s.Equal([]store.QueueConfigRecord{*rb, *ra}, mine, "current versions, in Seq order")
}
