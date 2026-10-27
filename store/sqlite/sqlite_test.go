package sqlite_test

import (
	"testing"

	"github.com/skme902/tq/internal/testdb"
	"github.com/skme902/tq/store"
	"github.com/skme902/tq/store/sqlite"
	"github.com/skme902/tq/store/storetest"
)

func TestConformance(t *testing.T) {
	for _, mode := range []string{sqlite.SyncFull, sqlite.SyncNormal} {
		t.Run(mode, func(t *testing.T) {
			open := testdb.SQLiteWith(t, sqlite.Options{Synchronous: mode})
			storetest.Run(t, func(t *testing.T) store.Store { return open(t) })
		})
	}
}
