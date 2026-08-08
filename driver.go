package orm

import (
	"database/sql"
)

// Driver beschreibt ein Datenbank-Backend. Die App wählt den Treiber bei
// Open — danach ist das Backend für sie unsichtbar (Verhaltensgleichheit).
type Driver interface {
	connect() (*sql.DB, dialect, error)
}

// readPooler liefert optional einen getrennten Lese-Pool. SQLite nutzt das:
// WAL erlaubt viele Leser neben dem EINEN Schreiber — Worker-Reads, Loads
// und Streams konkurrieren dann nicht mehr mit Schreibzugriffen um die
// serialisierte Schreib-Connection. PG/YB brauchen das nicht (Pool).
type readPooler interface {
	connectRead() (*sql.DB, error)
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
	// blobColumnSQL liefert die DDL, die eine Bestands-Spalte auf den
	// Binärtyp der encrypted-Spalten bringt (EncryptFields) — nil, wenn
	// nichts zu tun ist (Spalte schon binär; SQLite typisiert dynamisch).
	blobColumnSQL(q queryer, table, col string) ([]string, error)
	// partitionClause ist die PARTITION-BY-Klausel der Event-Tabellen
	// ("" = Backend partitioniert nicht; SQLite kollabiert).
	partitionClause() string
	// partitionSQL erzeugt die Partitionen einer Event-Tabelle: eine je
	// Region plus eine DEFAULT-Partition (idempotent).
	partitionSQL(table string, regions []regionPlacement) []string
	// adoptRegionSQL löst eine Region aus der DEFAULT-Partition heraus:
	// Partition anlegen, vorhandene Zeilen umhängen, anschließen. Nötig,
	// weil PG/YB keine Partition anlegen können, solange die
	// DEFAULT-Partition passende Zeilen hält.
	adoptRegionSQL(table string, key []string, r regionPlacement) []string
	// placementExists prüft, ob ein deklariertes Placement (Tablespace)
	// im Backend existiert. Backends ohne Placements melden true.
	placementExists(q queryer, name string) (bool, error)
	// geoPartitions liefert die vorhandenen Geo-Partitionen einer Tabelle:
	// Region → Placement ("" = Default-Tablespace). Die DEFAULT-Partition
	// erscheint unter geoDefaultRegion.
	geoPartitions(q queryer, table string) (map[string]string, error)
	// tableKind klassifiziert eine Tabelle: 'r' normal, 'p' partitioniert,
	// 0 = existiert nicht. Backends ohne Partitionierung melden nie 'p'.
	tableKind(q queryer, table string) (byte, error)
	// incomingFKs liefert Fremdschlüssel ANDERER Tabellen, die auf diese
	// Tabelle zeigen — als DROP-CONSTRAINT-Paare (Tabelle, Constraint).
	incomingFKs(q queryer, table string) ([][2]string, error)
	// tableIndexes liefert die Indexnamen einer Tabelle (inkl. PK-Index).
	tableIndexes(q queryer, table string) ([]string, error)
}

// regionPlacement bindet eine Region an ihren physischen Standort — auf
// PG/YB ein Tablespace, der auf YB per replica_placement die Zeilen
// tatsächlich in der Region hält. Leeres placement = Backend entscheidet.
type regionPlacement struct {
	name      string
	placement string
}

// geoDefaultRegion ist der Schlüssel der DEFAULT-Partition in
// geoPartitions — ein gültiger Regionsname kann so nicht heißen.
const geoDefaultRegion = "\x00default"

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
