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
	// Lese-Pool ist eine spätere Optimierung (doc/TASK.md).
	sdb.SetMaxOpenConns(1)
	return sdb, sqliteDialect{}, nil
}

type sqliteDialect struct{}

func (sqliteDialect) name() string { return "sqlite" }

func (sqliteDialect) rebind(query string) string { return query }

func (sqliteDialect) columnType(k colKind) string {
	switch k {
	case kInt, kBool:
		return "INTEGER"
	case kFloat:
		return "REAL"
	case kBlob:
		return "BLOB"
	default: // kText, kJSON
		return "TEXT"
	}
}

func (sqliteDialect) zeroLiteral(k colKind) string {
	switch k {
	case kInt, kBool, kFloat:
		return "0"
	case kBlob:
		return "x''"
	default:
		return "''"
	}
}

func (sqliteDialect) autoPK() string { return "INTEGER PRIMARY KEY" }

func (sqliteDialect) limitAll() string { return "-1" }

// forUpdate: SQLite emuliert über die serialisierte Schreib-Connection
// (txlock=immediate) — verhaltensgleich, kein Suffix nötig.
func (sqliteDialect) forUpdate() string { return "" }

// SQLite kollabiert alle Regionen physisch: keine Partitionierung,
// die Geo-Spalte trägt weiter das deklarierte Daten-Geo.
func (sqliteDialect) partitionClause() string { return "" }

func (sqliteDialect) partitionSQL(table string, regions []string) []string { return nil }

func (sqliteDialect) dualWriteTriggerSQL(table, pk string) []string {
	now := `strftime('%Y-%m-%dT%H:%M:%fZ','now')`
	return []string{
		fmt.Sprintf(`CREATE TRIGGER IF NOT EXISTS %q AFTER INSERT ON %q BEGIN
			INSERT INTO ormpp_dualwrite_queue (tbl, pk, op, changed_at) VALUES ('%s', NEW.%q, 'upsert', %s);
		END`, "ormpp_dw_"+table+"_ins", table, table, pk, now),
		fmt.Sprintf(`CREATE TRIGGER IF NOT EXISTS %q AFTER UPDATE ON %q BEGIN
			INSERT INTO ormpp_dualwrite_queue (tbl, pk, op, changed_at) VALUES ('%s', NEW.%q, 'upsert', %s);
		END`, "ormpp_dw_"+table+"_upd", table, table, pk, now),
		fmt.Sprintf(`CREATE TRIGGER IF NOT EXISTS %q AFTER DELETE ON %q BEGIN
			INSERT INTO ormpp_dualwrite_queue (tbl, pk, op, changed_at) VALUES ('%s', OLD.%q, 'delete', %s);
		END`, "ormpp_dw_"+table+"_del", table, table, pk, now),
	}
}

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
