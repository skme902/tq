package sqlite_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/skme902/tq/store/sqlite"
)

// Concurrent Open+Migrate of a fresh file: several connections race to switch it to WAL.
func TestConcurrentOpenFreshFile(t *testing.T) {
	ctx := context.Background()
	fails := map[string]int{}
	var mu sync.Mutex
	for i := range 50 {
		path := fmt.Sprintf("%s/m%d.db", t.TempDir(), i)
		var wg sync.WaitGroup
		for range 4 {
			wg.Go(func() {
				st, err := sqlite.Open(ctx, path, sqlite.Options{})
				if err != nil {
					mu.Lock()
					fails["open: "+err.Error()]++
					mu.Unlock()
					return
				}
				defer st.Close()
				if err := st.Migrate(ctx); err != nil {
					mu.Lock()
					fails["migrate: "+err.Error()]++
					mu.Unlock()
				}
			})
		}
		wg.Wait()
	}
	if len(fails) > 0 {
		t.Fatalf("failures: %v", fails)
	}
}
