package orm

import (
	"database/sql"
)

// Driver beschreibt ein Datenbank-Backend. Die App wählt den Treiber bei
// Open — danach ist das Backend für sie unsichtbar (Verhaltensgleichheit).
type Driver interface {
	connect() (*sql.DB, dialect, error)
}

// colKind klassifiziert Spaltentypen backend-neutral; der Dialekt mappt
// auf den physischen Typ.
type colKind int

const (
	kText colKind = iota
	kInt
	kFloat
	kBool
	kBlob
	kJSON
)

// dialect kapselt die dialektspezifischen Unterschiede. Bewusst klein —
// alles Verhaltens-Relevante lebt in der Engine.
type dialect interface {
	name() string
	// tableColumns liefert die existierenden Spalten einer Tabelle
	// (leer, wenn die Tabelle nicht existiert).
	tableColumns(q queryer, table string) ([]string, error)
	// rebind übersetzt ?-Platzhalter in die native Form ($n auf Postgres).
	rebind(query string) string
	// columnType liefert den physischen Spaltentyp.
	columnType(k colKind) string
	// zeroLiteral liefert das SQL-Literal des Zero-Values (ALTER ADD NOT NULL).
	zeroLiteral(k colKind) string
	// autoPK ist die Spaltendefinition eines auto-vergebenen Integer-PK.
	autoPK() string
	// limitAll ist der LIMIT-Wert für "alle Zeilen" (OFFSET ohne LIMIT).
	limitAll() string
	// forUpdate ist der Suffix für pessimistisches Sperren ("" = emuliert).
	forUpdate() string
	// dualWriteTriggerSQL erzeugt die Nachlauf-Trigger auf einer Alt-Tabelle.
	dualWriteTriggerSQL(table, pk string) []string
}

type queryer interface {
	ExecContext(ctx ctxType, query string, args ...any) (sql.Result, error)
	QueryContext(ctx ctxType, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx ctxType, query string, args ...any) *sql.Row
}

// dialq routet alle Statements durch das Platzhalter-Rebinding des Dialekts.
type dialq struct {
	q    queryer
	dial dialect
}

func (r dialq) ExecContext(ctx ctxType, query string, args ...any) (sql.Result, error) {
	return r.q.ExecContext(ctx, r.dial.rebind(query), args...)
}

func (r dialq) QueryContext(ctx ctxType, query string, args ...any) (*sql.Rows, error) {
	return r.q.QueryContext(ctx, r.dial.rebind(query), args...)
}

func (r dialq) QueryRowContext(ctx ctxType, query string, args ...any) *sql.Row {
	return r.q.QueryRowContext(ctx, r.dial.rebind(query), args...)
}

// rebindDollar übersetzt ?-Platzhalter in $1, $2, … — String-Literale in
// einfachen Anführungszeichen bleiben unangetastet.
func rebindDollar(query string) string {
	var b []byte
	n := 0
	inQuote := false
	for i := 0; i < len(query); i++ {
		c := query[i]
		switch {
		case c == '\'':
			inQuote = !inQuote
			b = append(b, c)
		case c == '?' && !inQuote:
			n++
			b = append(b, '$')
			b = appendInt(b, n)
		default:
			b = append(b, c)
		}
	}
	return string(b)
}

func appendInt(b []byte, n int) []byte {
	if n >= 10 {
		b = appendInt(b, n/10)
	}
	return append(b, byte('0'+n%10))
}
