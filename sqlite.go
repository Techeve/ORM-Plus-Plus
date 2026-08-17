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
	// Eine Verbindung: SQLite hat genau einen Schreiber. Lesende Pfade
	// laufen über den getrennten Lese-Pool (connectRead).
	sdb.SetMaxOpenConns(1)
	return sdb, sqliteDialect{}, nil
}

// connectRead öffnet den Lese-Pool: WAL erlaubt beliebig viele Leser
// parallel zum Schreiber; query_only macht die Verbindungen hart lesend.
func (d sqliteDriver) connectRead() (*sql.DB, error) {
	dsn := fmt.Sprintf(
		"file:%s?_pragma=query_only(1)&_pragma=busy_timeout(5000)",
		url.PathEscape(d.path),
	)
	rdb, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("orm: SQLite-Lese-Pool öffnen: %w", err)
	}
	rdb.SetMaxOpenConns(4)
	return rdb, nil
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

// blobColumnSQL: SQLite typisiert dynamisch — BLOB-Werte lassen sich ohne
// DDL in jede Spalte schreiben, die Typaffinität ist nur ein Hinweis.
func (sqliteDialect) blobColumnSQL(queryer, string, string) ([]string, error) { return nil, nil }

func (sqliteDialect) zeroLiteral(k colKind) string {
	switch k {
	case kInt, kBool, kFloat:
		return "0"
	case kBlob:
		return "x''"
	case kJSON:
		return "'null'" // gültiges JSON — '' wäre beim Lesen ein Parse-Fehler
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

func (sqliteDialect) partitionSQL(string, []regionPlacement) []string { return nil }

func (sqliteDialect) adoptRegionSQL(string, []string, regionPlacement) []string { return nil }

// Ohne Partitionen gibt es nichts zu platzieren — die Deklaration bleibt
// gültig, nur ohne physische Wirkung (Verhaltensgleichheit).
func (sqliteDialect) placementExists(queryer, string) (bool, error) { return true, nil }

func (sqliteDialect) geoPartitions(queryer, string) (map[string]string, error) { return nil, nil }

func (d sqliteDialect) tableKind(q queryer, table string) (byte, error) {
	cols, err := d.tableColumns(q, table)
	if err != nil || len(cols) == 0 {
		return 0, err
	}
	return 'r', nil
}

func (sqliteDialect) incomingFKs(queryer, string) ([][2]string, error) { return nil, nil }

func (sqliteDialect) tableIndexes(queryer, string) ([]string, error) { return nil, nil }

// SQLite schreibt mit genau einer Verbindung (MaxOpenConns=1) — die Vergabe
// ist bereits serialisiert.
func (sqliteDialect) lockGeoSeq(ctxType, queryer, string, string) error { return nil }

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
