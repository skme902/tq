// Package testdb opens throwaway databases for tests and benchmarks of the three stores.
//
// Postgres tests need TQ_TEST_POSTGRES_DSN (e.g.
// "postgres://tq@127.0.0.1:55432/tqtest?sslmode=disable"); without it they skip. Each
// call to Postgres creates a fresh schema and drops it on cleanup, so runs never see each
// other's rows. SQLite uses a file in t.TempDir().
package testdb

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/skme902/tq/store"
	"github.com/skme902/tq/store/memstore"
	"github.com/skme902/tq/store/postgres"
	"github.com/skme902/tq/store/sqlite"
)

// PostgresDSNEnv names the environment variable holding the test Postgres DSN.
const PostgresDSNEnv = "TQ_TEST_POSTGRES_DSN"

// Opener returns a new, independent handle (own connection pools) to one database. Calling
// it twice models two processes, or one process restarting, against the same data.
type Opener func(tb testing.TB) store.Store

// Kind is one store implementation under test.
type Kind struct {
	Name string
	// NewDB creates a fresh, migrated database and returns an opener for it.
	NewDB func(tb testing.TB) Opener
	// Durable is false for memstore (its "reopen" returns the same in-process instance).
	Durable bool
}

// Kinds returns memstore, sqlite and postgres (the latter skips when the DSN is unset).
func Kinds() []Kind {
	return []Kind{
		{Name: "memstore", NewDB: Memory},
		{Name: "sqlite", NewDB: SQLite, Durable: true},
		{Name: "postgres", NewDB: Postgres, Durable: true},
	}
}

// Memory returns an opener that always yields the same memstore.
func Memory(testing.TB) Opener {
	st := memstore.New()
	return func(testing.TB) store.Store { return st }
}

// SQLite creates a migrated database file under tb.TempDir().
func SQLite(tb testing.TB) Opener {
	return SQLiteWith(tb, sqlite.Options{})
}

// SQLiteWith is SQLite with explicit options.
func SQLiteWith(tb testing.TB, opts sqlite.Options) Opener {
	tb.Helper()
	path := filepath.Join(tb.TempDir(), "queue.db")
	open := func(tb testing.TB) store.Store {
		tb.Helper()
		st, err := sqlite.Open(context.Background(), path, opts)
		if err != nil {
			tb.Fatalf("sqlite.Open: %v", err)
		}
		tb.Cleanup(func() { _ = st.Close() })
		return st
	}
	st := open(tb).(*sqlite.Store)
	if err := st.Migrate(context.Background()); err != nil {
		tb.Fatalf("sqlite Migrate: %v", err)
	}
	return open
}

// Postgres creates a fresh schema in the TQ_TEST_POSTGRES_DSN database, migrates it, and
// drops it at cleanup. It skips the test when the variable is unset.
func Postgres(tb testing.TB) Opener {
	return PostgresWith(tb, postgres.Options{})
}

// PostgresWith is Postgres with explicit options.
func PostgresWith(tb testing.TB, opts postgres.Options) Opener {
	tb.Helper()
	ctx := context.Background()
	sdsn := PostgresSchemaDSN(tb)
	open := func(tb testing.TB) store.Store {
		tb.Helper()
		st, err := postgres.Open(context.Background(), sdsn, opts)
		if err != nil {
			tb.Fatalf("postgres.Open: %v", err)
		}
		tb.Cleanup(func() { _ = st.Close() })
		return st
	}
	st := open(tb).(*postgres.Store)
	if err := st.Migrate(ctx); err != nil {
		tb.Fatalf("postgres Migrate: %v", err)
	}
	return open
}

// PostgresSchemaDSN creates an empty schema (dropped at cleanup) in the TQ_TEST_POSTGRES_DSN
// database and returns a DSN whose search_path points at it. Not migrated. Skips the test
// when the variable is unset.
func PostgresSchemaDSN(tb testing.TB) string {
	tb.Helper()
	dsn := os.Getenv(PostgresDSNEnv)
	if dsn == "" {
		tb.Skipf("%s not set; skipping Postgres test", PostgresDSNEnv)
	}
	admin, err := sql.Open("pgx", dsn)
	if err != nil {
		tb.Fatalf("open admin: %v", err)
	}
	var rb [6]byte
	_, _ = rand.Read(rb[:])
	schema := "tq_test_" + hex.EncodeToString(rb[:])
	if _, err := admin.ExecContext(context.Background(), "CREATE SCHEMA "+schema); err != nil {
		_ = admin.Close()
		tb.Fatalf("create schema: %v", err)
	}
	tb.Cleanup(func() {
		_, _ = admin.ExecContext(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		_ = admin.Close()
	})
	sdsn, err := withSearchPath(dsn, schema)
	if err != nil {
		tb.Fatalf("dsn: %v", err)
	}
	return sdsn
}

// withSearchPath adds search_path (a pgx runtime parameter) to a URL or key=value DSN.
func withSearchPath(dsn, schema string) (string, error) {
	if u, err := url.Parse(dsn); err == nil && (u.Scheme == "postgres" || u.Scheme == "postgresql") {
		q := u.Query()
		q.Set("search_path", schema)
		u.RawQuery = q.Encode()
		return u.String(), nil
	}
	return dsn + " search_path=" + schema, nil
}
