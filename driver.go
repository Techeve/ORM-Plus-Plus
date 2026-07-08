package orm

import (
	"database/sql"
	"fmt"
)

// Driver beschreibt ein Datenbank-Backend. Die App wählt den Treiber bei
// Open — danach ist das Backend für sie unsichtbar (Verhaltensgleichheit).
type Driver interface {
	connect() (*sql.DB, dialect, error)
}

// dialect kapselt die dialektspezifischen Unterschiede. Bewusst klein —
// alles Verhaltens-Relevante lebt in der Engine.
type dialect interface {
	name() string
	// tableColumns liefert die existierenden Spalten einer Tabelle
	// (leer, wenn die Tabelle nicht existiert).
	tableColumns(q queryer, table string) ([]string, error)
}

type queryer interface {
	ExecContext(ctx ctxType, query string, args ...any) (sql.Result, error)
	QueryContext(ctx ctxType, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx ctxType, query string, args ...any) *sql.Row
}

// --- Platzhalter für Phase 4 ---

type unimplementedDriver struct{ which string }

func (u unimplementedDriver) connect() (*sql.DB, dialect, error) {
	return nil, nil, fmt.Errorf("orm: %s-Treiber ist noch nicht implementiert (Phase 4, siehe doc/ROADMAP.md)", u.which)
}

// Postgres verbindet mit PostgreSQL (Phase 4).
func Postgres(dsn string) Driver { return unimplementedDriver{which: "PostgreSQL"} }

// Yugabyte verbindet mit YugabyteDB (Phase 4).
func Yugabyte(dsn string) Driver { return unimplementedDriver{which: "YugabyteDB"} }
