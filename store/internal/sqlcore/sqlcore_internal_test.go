package sqlcore

import (
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/skme902/tq/store"
)

func TestSplitStatements(t *testing.T) {
	got := splitStatements("-- header\nCREATE TABLE a (\n  x INT -- trailing\n);\n\n-- c\nCREATE INDEX i ON a (x);\nSELECT 1")
	require.Equal(t, []string{"CREATE TABLE a (\n  x INT \n);", "CREATE INDEX i ON a (x);", "SELECT 1"}, got)
}

func TestQueueIDIsUnambiguous(t *testing.T) {
	seen := map[string]store.QueueKey{}
	for _, k := range []store.QueueKey{
		{Namespace: "a", Name: "bc"}, {Namespace: "ab", Name: "c"}, {Namespace: "", Name: "abc"},
		{Namespace: "a", Name: "bc", Partition: 1}, {Namespace: "a", Name: "bc\x01"},
	} {
		id, _ := queueIDAndHash(k)
		prev, dup := seen[string(id)]
		require.False(t, dup, "%v and %v share an id", prev, k)
		seen[string(id)] = k
	}
	id1, h1 := queueIDAndHash(store.QueueKey{Namespace: "ns", Name: "q"})
	id2, h2 := queueIDAndHash(store.QueueKey{Namespace: "ns", Name: "q"})
	require.Equal(t, id1, id2)
	require.Equal(t, h1, h2, "hash is deterministic (farm.Fingerprint32)")
}

func TestRebind(t *testing.T) {
	require.Equal(t, "a=$1 AND b IN ($2, $3)", rebind("a=? AND b IN (?, ?)", 0))
}

func TestNumberedQueriesHaveNoQuestionMarks(t *testing.T) {
	q := buildQueries(Dialect{NumberedPlaceholders: true, RowRef: "ctid", LockSuffix: " FOR UPDATE"})
	v := reflect.ValueOf(q)
	for i := range v.NumField() {
		require.NotContains(t, v.Field(i).String(), "?", "query %s is not rebound", v.Type().Field(i).Name)
	}
	require.True(t, strings.Contains(q.deadExists, "$1"))
	cq := buildConfigQueries(Dialect{NumberedPlaceholders: true, LockSuffix: " FOR UPDATE"})
	cv := reflect.ValueOf(cq)
	for i := range cv.NumField() {
		require.NotContains(t, cv.Field(i).String(), "?", "config query %s is not rebound", cv.Type().Field(i).Name)
	}
}
