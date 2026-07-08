package orm

import (
	"database/sql"
	"fmt"
	"net/url"

	_ "modernc.org/sqlite" // CGO-freier SQLite-Treiber
)

type sqliteDriver struct{ path string }

// SQLite öffnet eine eingebettete SQLite-Datenbank (Datei oder ":memory:").
// WAL-Modus, aktivierte Foreign Keys und busy_timeout sind fest eingestellt.
func SQLite(path string) Driver { return sqliteDriver{path: path} }

func (d sqliteDriver) connect() (*sql.DB, dialect, error) {
	dsn := fmt.Sprintf(
		"file:%s?_txlock=immediate&_pragma=foreign_keys(1)&_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)",
		url.PathEscape(d.path),
	)
	sdb, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, nil, fmt.Errorf("orm: SQLite öffnen: %w", err)
	}
	// Eine Verbindung: SQLite hat genau einen Schreiber; ein getrennter
	// Lese-Pool ist eine spätere Optimierung (TASK.md).
	sdb.SetMaxOpenConns(1)
	return sdb, sqliteDialect{}, nil
}

type sqliteDialect struct{}

func (sqliteDialect) name() string { return "sqlite" }

func (sqliteDialect) tableColumns(q queryer, table string) ([]string, error) {
	rows, err := q.QueryContext(bgCtx(), fmt.Sprintf("PRAGMA table_info(%q)", table))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var cols []string
	for rows.Next() {
		var (
			cid     int
			name    string
			ctype   string
			notnull int
			dflt    any
			pk      int
		)
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			return nil, err
		}
		cols = append(cols, name)
	}
	return cols, rows.Err()
}
