package postgres_test

import (
	"testing"

	"github.com/skme902/tq/internal/testdb"
	"github.com/skme902/tq/store"
	"github.com/skme902/tq/store/storetest"
)

// Requires TQ_TEST_POSTGRES_DSN; skips otherwise.
func TestConformance(t *testing.T) {
	open := testdb.Postgres(t)
	storetest.Run(t, func(t *testing.T) store.Store { return open(t) })
}
