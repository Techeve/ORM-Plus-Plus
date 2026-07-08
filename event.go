package orm

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Event ist das dekodierte Ereignis, das an Apply übergeben wird.
type Event struct {
	Type    string // Kurzname laut Deklaration, z. B. "zone.created"
	Version int    // Payload-Schema-Version (nach Upcasting die deklarierte)
	Time    time.Time
	Seq     int64 // Aggregat-Sequenz dieses Events
	Payload any
}

// applier ist die einzige Pflichtfunktion eines ES-Models.
type applier interface{ Apply(Event) error }

// CloudEvent ist der CloudEvents-1.0-Envelope eines gespeicherten Events.
// Er wird beim Lesen aus den Spalten rekonstruiert — CloudEvents ist
// Austauschformat, nicht Speicherformat.
type CloudEvent struct {
	SpecVersion     string
	ID              string
	Source          string
	Type            string // voller Typ inkl. Version, z. B. "….zone.created.v1"
	Subject         string // Aggregat-ID
	Time            time.Time
	DataContentType string
	Data            []byte // Payload-JSON in der gespeicherten Version

	// Extension-Attribute:
	Tenant       ID
	Geo          string
	Sequence     int64 // je Geo monotone Log-Sequenz
	AggregateSeq int64
}

// Position ist das Konsistenz-Token eines Appends: ein Cursor-Vektor mit
// einer Sequenz je Geo. Es gibt keine Totalordnung über Regionen.
type Position struct {
	seqs map[string]int64
}

// StreamOption konfiguriert Stream und Watch.
type StreamOption func(*streamOpts)

type streamOpts struct{ from Position }

// From startet einen Strom nach der angegebenen Position.
func From(pos Position) StreamOption {
	return func(o *streamOpts) { o.from = pos }
}

// --- Event-Deklaration ---

// EventDecl deklariert ein Event eines ES-Models (orm.E).
type EventDecl struct {
	name    string
	version int
	payload reflect.Type
}

// EventOption konfiguriert eine Event-Deklaration.
type EventOption func(*EventDecl)

// V setzt die Schema-Version eines Events (Default 1).
func V(n int) EventOption {
	return func(e *EventDecl) { e.version = n }
}

// E deklariert ein Event mit Payload-Typ P und Kurznamen wie "zone.created".
func E[P any](name string, opts ...EventOption) EventDecl {
	d := EventDecl{name: name, version: 1, payload: reflect.TypeFor[P]()}
	for _, o := range opts {
		o(&d)
	}
	return d
}

// Events deklariert die Events eines ES-Models bei der Registrierung.
func Events(decls ...EventDecl) ModelOption {
	return func(m *modelOptions) { m.events = append(m.events, decls...) }
}

// eventDecl ist die kompilierte Event-Deklaration.
type eventDecl struct {
	name    string
	version int
	goType  reflect.Type
}

// esInfo ist der kompilierte ES-Plan eines Models.
type esInfo struct {
	aggIdx []int // reflect-Indexpfad des eingebetteten Aggregate
	decls  []*eventDecl
	byType map[reflect.Type]*eventDecl
	byName map[string]*eventDecl
	prefix string // CloudEvents-Typ-Präfix, gesetzt bei Migrate
}

func (es *esInfo) fullType(name string, v int) string {
	return es.prefix + "." + name + ".v" + strconv.Itoa(v)
}

func (es *esInfo) parseType(full string) (name string, v int, ok bool) {
	rest, found := strings.CutPrefix(full, es.prefix+".")
	if !found {
		return "", 0, false
	}
	i := strings.LastIndex(rest, ".v")
	if i < 0 {
		return "", 0, false
	}
	n, err := strconv.Atoi(rest[i+2:])
	if err != nil || n < 1 {
		return "", 0, false
	}
	return rest[:i], n, true
}

func esEventsTable(m *model) string { return m.table + "_events" }
func esSnapsTable(m *model) string  { return m.table + "_snapshots" }

// --- Typ-Wörterbuch (ormpp_event_types) ---

// typeDict hält das Mapping type_id ↔ voller CloudEvents-Typ im Speicher.
type typeDict struct {
	mu     sync.RWMutex
	byID   map[int64]string
	byName map[string]int64
}

func (td *typeDict) idOf(full string) (int64, bool) {
	td.mu.RLock()
	defer td.mu.RUnlock()
	id, ok := td.byName[full]
	return id, ok
}

func (td *typeDict) typeOf(id int64) (string, bool) {
	td.mu.RLock()
	defer td.mu.RUnlock()
	s, ok := td.byID[id]
	return s, ok
}

// bootstrapEventTypes lädt das Typ-Wörterbuch und registriert die
// deklarierten Event-Typen aller ES-Modelle.
func (d *DB) bootstrapEventTypes(ctx context.Context) error {
	dict := &typeDict{byID: map[int64]string{}, byName: map[string]int64{}}
	rows, err := d.q().QueryContext(ctx, `SELECT type_id, type FROM ormpp_event_types`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var id int64
		var full string
		if err := rows.Scan(&id, &full); err != nil {
			rows.Close()
			return err
		}
		dict.byID[id] = full
		dict.byName[full] = id
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	var next int64 = 1
	for id := range dict.byID {
		if id >= next {
			next = id + 1
		}
	}
	for _, m := range d.reg.ordered {
		if m.kind != kindEventSourced {
			continue
		}
		prefix := d.opts.eventTypePrefix
		if prefix == "" {
			prefix = strings.ReplaceAll(m.goType.PkgPath(), "/", ".")
		}
		m.es.prefix = prefix
		for _, decl := range m.es.decls {
			full := m.es.fullType(decl.name, decl.version)
			if _, ok := dict.byName[full]; ok {
				continue
			}
			if _, err := d.q().ExecContext(ctx,
				`INSERT INTO ormpp_event_types (type_id, type) VALUES (?, ?)`, next, full); err != nil {
				return fmt.Errorf("orm: Event-Typ %q registrieren: %w", full, err)
			}
			dict.byID[next] = full
			dict.byName[full] = next
			next++
		}
	}
	d.esTypes = dict
	return nil
}

// --- Event-Zeilen lesen ---

type eventRow struct {
	aggregateID string
	aggSeq      int64
	geo         string
	tenant      ID
	seq         int64
	eventID     string
	occurred    time.Time
	typeID      int64
	data        []byte
}

func esEventSelect(m *model) string {
	cols := `"aggregate_id", "aggregate_seq", "geo", "seq", "event_id", "occurred_at", "type_id", "data"`
	if m.tenanted() {
		cols += `, "tenant_id"`
	}
	return cols
}

func scanEventRow(m *model, sc rowScanner) (eventRow, error) {
	var r eventRow
	var occurred string
	dest := []any{&r.aggregateID, &r.aggSeq, &r.geo, &r.seq, &r.eventID, &occurred, &r.typeID, &r.data}
	if m.tenanted() {
		dest = append(dest, &r.tenant)
	}
	if err := sc.Scan(dest...); err != nil {
		return r, err
	}
	t, err := time.Parse(time.RFC3339Nano, occurred)
	if err != nil {
		return r, fmt.Errorf("orm: occurred_at %q: %w", occurred, err)
	}
	r.occurred = t
	return r, nil
}

// cloudEvent rekonstruiert den CloudEvents-Envelope aus einer Event-Zeile.
func (d *DB) cloudEvent(m *model, r eventRow) CloudEvent {
	full, _ := d.esTypes.typeOf(r.typeID)
	return CloudEvent{
		SpecVersion:     "1.0",
		ID:              r.eventID,
		Source:          "/orm/" + m.table,
		Type:            full,
		Subject:         r.aggregateID,
		Time:            r.occurred,
		DataContentType: "application/json",
		Data:            r.data,
		Tenant:          r.tenant,
		Geo:             r.geo,
		Sequence:        r.seq,
		AggregateSeq:    r.aggSeq,
	}
}
