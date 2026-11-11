package matching

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/skme902/tq/store"
	"github.com/skme902/tq/store/memstore"
)

// PurgeQueue counts the rows its own delete removes. adminPurge sets numToGC so GC can retry a
// failed purge delete; a GC that starts before that delete must not take the purged rows
// first, or PurgeQueue reports fewer tasks than it purged (TestPurgeQueue/sqlite flake).
func TestPurgeCountNotStolenByGC(t *testing.T) {
	st := memstore.New()
	e := newR2Engine(t, st, nil, nil, nil)
	for i := range 5 {
		_, err := e.AddTask(context.Background(), portQueue, store.Task{Payload: []byte(fmt.Sprint(i))})
		require.NoError(t, err)
	}

	testHookPurgeBeforeDelete = func(readers []*fairTaskReader) {
		for _, tr := range readers {
			tr.lock.Lock()
			tr.lastGCTime = time.Time{} // TaskDeleteInterval has passed: GC is due
			tr.lock.Unlock()
			tr.maybeGC()
			require.Eventually(t, func() bool {
				tr.lock.Lock()
				defer tr.lock.Unlock()
				return !tr.inGC
			}, 3*time.Second, time.Millisecond)
		}
	}
	t.Cleanup(func() { testHookPurgeBeforeDelete = nil })

	n, err := e.PurgeQueue(context.Background(), portQueue)
	require.NoError(t, err)
	require.Equal(t, 5, n, "purge reports every task it removed")
	testHookPurgeBeforeDelete = nil

	page, _, err := e.PeekTasks(context.Background(), portQueue, PeekOptions{})
	require.NoError(t, err)
	require.Empty(t, page)
}
