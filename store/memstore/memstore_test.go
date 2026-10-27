package memstore_test

import (
	"testing"

	"github.com/skme902/tq/store"
	"github.com/skme902/tq/store/memstore"
	"github.com/skme902/tq/store/storetest"
)

func TestConformance(t *testing.T) {
	storetest.Run(t, func(*testing.T) store.Store { return memstore.New() })
}
