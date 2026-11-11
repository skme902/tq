package matching

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/skme902/tq/metrics"
	"github.com/skme902/tq/metrics/metricstest"
	"github.com/skme902/tq/store"
	"github.com/skme902/tq/store/memstore"
)

// loadedTasks changes only through addLoadedLocked, which keeps pausedLoaded exact (review
// pause r2 P1: pausedLoaded used to be a full scan on every read check).
func TestLoadedTasksChangedOnlyByHelper(t *testing.T) {
	files, err := filepath.Glob("*.go")
	require.NoError(t, err)
	re := regexp.MustCompile(`\.loadedTasks\s*(\+\+|--|\+=|-=|=[^=])`)
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		b, err := os.ReadFile(f)
		require.NoError(t, err)
		for i, line := range strings.Split(string(b), "\n") {
			if re.MatchString(line) && !strings.Contains(line, "tr.loadedTasks += delta") {
				t.Errorf("%s:%d changes loadedTasks directly; use addLoadedLocked: %s", f, i+1, strings.TrimSpace(line))
			}
		}
	}
}

// pausedLoaded stays equal to a recount through loads, leases, acks, nacks, delays and pause
// changes.
func TestPausedLoadedCounterExact(t *testing.T) {
	src := NewMemoryQueueConfigSource(0)
	require.NoError(t, src.SetFairnessKeyPaused(portQueue, "p", true))
	e := newR2Engine(t, memstore.New(), nil, nil, func(c *Config) {
		c.GetTasksBatchSize = 10
		c.GetTasksReloadAt = 3
	})
	e.configSource = src
	ctx := context.Background()
	for range 15 {
		for _, k := range []string{"p", "b"} {
			_, err := e.AddTask(ctx, portQueue, store.Task{Payload: []byte(k), Priority: store.Priority{FairnessKey: k}})
			require.NoError(t, err)
		}
	}
	tr := e.partitions[portQueue].defaultQueue.backlogMgr.subqueues[0]
	check := func(msg string) {
		t.Helper()
		tr.lock.Lock()
		defer tr.lock.Unlock()
		require.Equal(t, tr.pausedLoadedLocked(tr.countedKeys), tr.pausedLoaded, msg)
	}
	poll := func() *Delivery {
		t.Helper()
		pctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		d, err := e.PollTask(pctx, portQueue, PollOptions{})
		require.NoError(t, err)
		return d
	}
	for i := range 15 {
		d := poll()
		require.Equal(t, "b", string(d.Task.Payload))
		check("after lease")
		switch i % 3 {
		case 0:
			require.NoError(t, d.Ack())
		case 1:
			require.NoError(t, d.Nack(NackDelay(time.Millisecond)))
			require.NoError(t, poll().Ack())
		default:
			require.NoError(t, d.Ack())
		}
		check("after completion")
	}
	require.NoError(t, src.SetFairnessKeyPaused(portQueue, "p", false))
	require.Eventually(t, func() bool {
		tr.lock.Lock()
		defer tr.lock.Unlock()
		return tr.countedKeys == nil
	}, 3*time.Second, time.Millisecond)
	check("after resume")
	for range 15 {
		require.NoError(t, poll().Ack())
		check("after paused-key delivery")
	}
}

// Like upstream v1.32.1, any backlog task in the matcher older than BacklogNegligibleAge
// turns off sync match for the whole queue, including tasks of a paused key (upstream counts
// every DB-backlog task in the matcher, dispatchable or not, e.g. a rate-limited key's). The
// spooled task is still delivered through the backlog (review pause r2 P1: kept as upstream).
func TestPausedKeyBacklogAgeGatesSyncMatchLikeUpstream(t *testing.T) {
	src := NewMemoryQueueConfigSource(0)
	require.NoError(t, src.SetFairnessKeyPaused(portQueue, "p", true))
	e := newR2Engine(t, memstore.New(), nil, nil, func(c *Config) { c.BacklogNegligibleAge = 50 * time.Millisecond })
	e.configSource = src
	ctx := context.Background()
	_, err := e.AddTask(ctx, portQueue, store.Task{Payload: []byte("p"), Priority: store.Priority{FairnessKey: "p"}})
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond) // the paused task is now older than BacklogNegligibleAge
	got := make(chan *Delivery, 1)
	go func() {
		pctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		if d, err := e.PollTask(pctx, portQueue, PollOptions{}); err == nil {
			got <- d
		}
	}()
	require.Eventually(t, func() bool {
		desc, err := e.DescribeQueue(ctx, portQueue)
		return err == nil && desc.WaitingPollers > 0
	}, 3*time.Second, time.Millisecond)
	synced, err := e.AddTask(ctx, portQueue, store.Task{Payload: []byte("b"), Priority: store.Priority{FairnessKey: "b"}})
	require.NoError(t, err)
	require.False(t, synced, "not sync-matched: the paused key's backlog is not negligible")
	select {
	case d := <-got:
		require.Equal(t, "b", string(d.Task.Payload), "delivered through the backlog")
		require.NoError(t, d.Ack())
	case <-time.After(3 * time.Second):
		t.Fatal("not delivered")
	}
}

// A config callback that arrives after Stop began does nothing: queue_paused stays 0 (review
// pause r2 P2).
func TestConfigCallbackAfterStopIgnored(t *testing.T) {
	rec := metricstest.New()
	src := NewMemoryQueueConfigSource(0)
	src.SetPaused(portQueue, true)
	e := newR2Engine(t, memstore.New(), nil, rec, nil)
	e.configSource = src
	_, err := e.AddTask(context.Background(), portQueue, store.Task{Payload: []byte("a")})
	require.NoError(t, err)
	pm := e.partitions[portQueue]
	pm.Stop(unloadCauseShuttingDown)
	pm.userDataChanged() // the callback that was already on its way
	v, _ := rec.GaugeValue(metrics.QueuePaused, portMetricTags()...)
	require.Zero(t, v)
}

// SetPaused with the current value does not notify (review pause r2 P2), and QueueConfig
// returns a copy of PausedFairnessKeys.
func TestSetPausedNoopAndKeysCopied(t *testing.T) {
	src := NewMemoryQueueConfigSource(0)
	n := 0
	cancel := src.WatchQueueConfig(portQueue, func() { n++ })
	defer cancel()
	src.SetPaused(portQueue, true)
	src.SetPaused(portQueue, true)
	require.Equal(t, 1, n)
	require.NoError(t, src.SetFairnessKeyPaused(portQueue, "a", true))
	src.QueueConfig(portQueue).PausedFairnessKeys[0] = "z"
	require.Equal(t, []string{"a"}, src.QueueConfig(portQueue).PausedFairnessKeys)
}
