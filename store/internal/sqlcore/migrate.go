// Schema migrations. Upstream ships versioned schema directories applied by
// its temporal-sql-tool (tools/sql, schema/*/versioned/*/manifest.json); this library embeds
// its migrations and applies them with an explicit Migrate call instead.

package sqlcore

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Migration is one forward-only schema step, loaded from "NNNN_name.sql".
type Migration struct {
	Version int
	Name    string
	SQL     string
}

// LoadMigrations reads every *.sql file in dir of fsys, ordered by numeric prefix.
func LoadMigrations(fsys fs.FS, dir string) ([]Migration, error) {
	entries, err := fs.ReadDir(fsys, dir)
	if err != nil {
		return nil, err
	}
	var ms []Migration
	seen := map[int]string{}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		num, name, ok := strings.Cut(strings.TrimSuffix(e.Name(), ".sql"), "_")
		v, err := strconv.Atoi(num)
		if !ok || err != nil || v <= 0 {
			return nil, fmt.Errorf("migration %q: want NNNN_name.sql", e.Name())
		}
		if prev, dup := seen[v]; dup {
			return nil, fmt.Errorf("migrations %q and %q share version %d", prev, e.Name(), v)
		}
		seen[v] = e.Name()
		b, err := fs.ReadFile(fsys, path.Join(dir, e.Name()))
		if err != nil {
			return nil, err
		}
		ms = append(ms, Migration{Version: v, Name: name, SQL: string(b)})
	}
	sort.Slice(ms, func(i, j int) bool { return ms[i].Version < ms[j].Version })
	return ms, nil
}

const versionTable = `CREATE TABLE IF NOT EXISTS tq_schema_version (
  version INTEGER NOT NULL PRIMARY KEY,
  name TEXT NOT NULL,
  applied_at TEXT NOT NULL
)`

// Migrate applies every migration not yet recorded in tq_schema_version, all in one
// transaction (DDL is transactional on both Postgres and SQLite), so a failed run leaves the
// schema unchanged. It is idempotent and safe to call from several processes at once.
// It returns the versions it applied.
func Migrate(ctx context.Context, db *sql.DB, d Dialect, ms []Migration) ([]int, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	if d.MigrationLockSQL != "" {
		if _, err := tx.ExecContext(ctx, d.MigrationLockSQL); err != nil {
			return nil, fmt.Errorf("migration lock: %w", err)
		}
	}
	if _, err := tx.ExecContext(ctx, versionTable); err != nil {
		return nil, fmt.Errorf("create tq_schema_version: %w", err)
	}
	applied := map[int]bool{}
	rows, err := tx.QueryContext(ctx, `SELECT version FROM tq_schema_version`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			rows.Close()
			return nil, err
		}
		applied[v] = true
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	ins := `INSERT INTO tq_schema_version (version, name, applied_at) VALUES (?, ?, ?)`
	if d.NumberedPlaceholders {
		ins = rebind(ins, 0)
	}
	var did []int
	for _, m := range ms {
		if applied[m.Version] {
			continue
		}
		for _, stmt := range splitStatements(m.SQL) {
			if _, err := tx.ExecContext(ctx, stmt); err != nil {
				return nil, fmt.Errorf("migration %04d_%s: %w", m.Version, m.Name, err)
			}
		}
		if _, err := tx.ExecContext(ctx, ins, m.Version, m.Name, time.Now().UTC().Format(time.RFC3339)); err != nil {
			return nil, err
		}
		did = append(did, m.Version)
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return did, nil
}

// splitStatements splits a migration on ";" at line ends and drops "--" comment lines. The
// shipped migrations contain no semicolons inside literals or bodies.
func splitStatements(src string) []string {
	var out []string
	var cur strings.Builder
	for _, line := range strings.Split(src, "\n") {
		t := strings.TrimSpace(line)
		if t == "" || strings.HasPrefix(t, "--") {
			continue
		}
		if i := strings.Index(line, "--"); i >= 0 {
			line = line[:i]
		}
		cur.WriteString(line)
		cur.WriteByte('\n')
		if strings.HasSuffix(strings.TrimSpace(line), ";") {
			if s := strings.TrimSpace(cur.String()); s != ";" {
				out = append(out, s)
			}
			cur.Reset()
		}
	}
	if s := strings.TrimSpace(cur.String()); s != "" {
		out = append(out, s)
	}
	return out
}
