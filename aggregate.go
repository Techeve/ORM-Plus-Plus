package orm

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"reflect"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// Aggregate wird in ES-Modelle eingebettet und bringt Identität, Version
// und alle Lade-/Historien-Funktionen mit. Instanzen entstehen über
// orm.New oder orm.Load — nie über ein nacktes Struct-Literal.
type Aggregate struct {
	id        ID
	version   int64 // Aggregat-Sequenz des zuletzt angewandten Events
	updatedAt time.Time
	rt        *esRuntime
	self      any // *T des umgebenden Structs
}

type esRuntime struct {
	h Handle
	m *model
}

// ID liefert die Aggregat-ID (UUIDv7).
func (a *Aggregate) ID() ID { return a.id }

// Version liefert die Aggregat-Sequenz des zuletzt angewandten Events.
func (a *Aggregate) Version() int64 { return a.version }

// UpdatedAt liefert den Zeitpunkt des zuletzt angewandten Events.
func (a *Aggregate) UpdatedAt() time.Time { return a.updatedAt }

func aggOf(m *model, e any) *Aggregate {
	return reflect.ValueOf(e).Elem().FieldByIndex(m.es.aggIdx).Addr().Interface().(*Aggregate)
}

func wireAggregate(m *model, e any, h Handle, id ID) *Aggregate {
	agg := aggOf(m, e)
	agg.id = id
	agg.rt = &esRuntime{h: h, m: m}
	agg.self = e
	return agg
}

// New erzeugt ein neues, noch nicht persistiertes Aggregat mit frischer ID.
// Das erste Append schreibt es in den Event-Log.
func New[T any](h Handle) *T {
	e := new(T)
	if m := h.db().reg.models[reflect.TypeFor[T]()]; m != nil && m.kind == kindEventSourced {
		wireAggregate(m, e, h, NewID())
	}
	return e
}

// --- Laden ---

type loadOpts struct {
	wait    bool
	pos     Position
	timeout time.Duration
}

// LoadOption konfiguriert Load und Refresh.
type LoadOption func(*loadOpts)

// WaitFor wartet, bis die eingebaute Projektion die angegebene Position
// erreicht hat (Read-your-writes) — sonst ErrWaitTimeout.
func WaitFor(pos Position, timeout time.Duration) LoadOption {
	return func(o *loadOpts) { o.wait, o.pos, o.timeout = true, pos, timeout }
}

func esTenant(ctx context.Context, m *model) (ID, error) {
	if !m.tenanted() {
		return ID{}, nil
	}
	t, ok := tenantFrom(ctx)
	if !ok {
		return ID{}, ErrNoTenant
	}
	return t, nil
}

// Load lädt den aktuellen Zustand eines Aggregats: letzter Snapshot plus
// Restevents, gefaltet durch Apply — transparent für den Aufrufer.
func Load[T any](ctx context.Context, h Handle, id ID, opts ...LoadOption) (*T, error) {
	d := h.db()
	m := d.reg.models[reflect.TypeFor[T]()]
	if m == nil || m.kind != kindEventSourced {
		return nil, fmt.Errorf("orm: Load[%T]: kein registriertes EventSourced-Model", *new(T))
	}
	var lo loadOpts
	for _, o := range opts {
		o(&lo)
	}
	tenant, err := esTenant(ctx, m)
	if err != nil {
		return nil, err
	}
	if lo.wait {
		if err := d.waitForProjection(ctx, m, lo.pos, lo.timeout); err != nil {
			return nil, err
		}
	}
	e := new(T)
	agg := wireAggregate(m, e, h, id)
	if err := d.foldInto(ctx, readQ(h), m, agg, tenant, 0, nil); err != nil {
		return nil, err
	}
	if agg.version == 0 {
		return nil, ErrNotFound
	}
	return e, nil
}

// foldInto lädt Snapshot + Restevents und faltet sie durch Apply in agg.self.
// upToSeq > 0 begrenzt auf eine Aggregat-Sequenz, upToTime auf einen Zeitpunkt.
func (d *DB) foldInto(ctx context.Context, q queryer, m *model, agg *Aggregate, tenant ID, upToSeq int64, upToTime *time.Time) error {
	if d.esTypes == nil {
		return fmt.Errorf("orm: Migrate muss vor Event-Operationen laufen")
	}

	// Zeitgrenzen brauchen die Events selbst — dann ohne Snapshot falten.
	if upToTime == nil && !m.opts.snapshotDisabled {
		query := fmt.Sprintf(`SELECT aggregate_seq, state FROM %q WHERE aggregate_id = ?`, esSnapsTable(m))
		args := []any{agg.id.String()}
		if m.tenanted() {
			query += ` AND tenant_id = ?`
			args = append(args, tenant.String())
		}
		if upToSeq > 0 {
			query += ` AND aggregate_seq <= ?`
			args = append(args, upToSeq)
		}
		query += ` ORDER BY aggregate_seq DESC LIMIT 1`
		var seq int64
		var state []byte
		switch err := q.QueryRowContext(ctx, query, args...).Scan(&seq, &state); err {
		case nil:
			if err := restoreSnapshot(agg.self, state); err != nil {
				return fmt.Errorf("orm: Snapshot von %s laden: %w", m.name, err)
			}
			agg.version = seq
		case sql.ErrNoRows:
		default:
			return err
		}
	}

	// Normalfall (Snapshot + Rest) liest nur den Hot-Log; Zeitreisen unter
	// die Archiv-Grenze lesen transparent Hot + Archiv.
	query := fmt.Sprintf(`SELECT aggregate_seq, occurred_at, type_id, data FROM %s WHERE aggregate_id = ? AND aggregate_seq > ?`,
		esEventsFrom(m, upToSeq > 0 || upToTime != nil))
	args := []any{agg.id.String(), agg.version}
	if m.tenanted() {
		query += ` AND tenant_id = ?`
		args = append(args, tenant.String())
	}
	if upToSeq > 0 {
		query += ` AND aggregate_seq <= ?`
		args = append(args, upToSeq)
	}
	query += ` ORDER BY aggregate_seq`
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	ap := agg.self.(applier)
	for rows.Next() {
		var seq, typeID int64
		var occurred string
		var data []byte
		if err := rows.Scan(&seq, &occurred, &typeID, &data); err != nil {
			return err
		}
		t, err := time.Parse(time.RFC3339Nano, occurred)
		if err != nil {
			return err
		}
		if upToTime != nil && t.After(*upToTime) {
			break
		}
		full, ok := d.esTypes.typeOf(typeID)
		if !ok {
			return fmt.Errorf("orm: unbekannte type_id %d in %s", typeID, esEventsTable(m))
		}
		payload, decl, err := d.decodePayload(m, full, data)
		if err != nil {
			return err
		}
		if err := ap.Apply(Event{Type: decl.name, Version: decl.version, Time: t, Seq: seq, Payload: payload}); err != nil {
			return fmt.Errorf("orm: %s.Apply: %w", m.name, err)
		}
		agg.version, agg.updatedAt = seq, t
	}
	return rows.Err()
}

// restoreSnapshot stellt den Zustand her — SnapshotUnmarshal, wenn das
// Model es implementiert, sonst JSON.
func restoreSnapshot(self any, state []byte) error {
	if su, ok := self.(interface{ SnapshotUnmarshal([]byte) error }); ok {
		return su.SnapshotUnmarshal(state)
	}
	return json.Unmarshal(state, self)
}

func marshalSnapshot(self any) ([]byte, error) {
	if sm, ok := self.(interface{ SnapshotMarshal() ([]byte, error) }); ok {
		return sm.SnapshotMarshal()
	}
	return json.Marshal(self)
}

// --- Schreiben ---

// Append hängt ein oder mehrere Events atomar an und erwartet implizit die
// geladene Aggregat-Version — ist inzwischen jemand dazwischengekommen:
// ErrVersionConflict. Apply-Fehler rollen die Events zurück; der In-Memory-
// Zustand ist dann stale und muss per Refresh neu geladen werden.
func (a *Aggregate) Append(ctx context.Context, payloads ...any) (Position, error) {
	if a.rt == nil {
		return Position{}, fmt.Errorf("orm: Aggregat nicht initialisiert — mit orm.New oder orm.Load erzeugen")
	}
	if len(payloads) == 0 {
		return Position{}, fmt.Errorf("orm: Append ohne Events")
	}
	d := a.rt.h.db()
	m := a.rt.m
	if d.esTypes == nil {
		return Position{}, fmt.Errorf("orm: Migrate muss vor Event-Operationen laufen")
	}
	tenant, err := esTenant(ctx, m)
	if err != nil {
		return Position{}, err
	}
	if m.tenanted() {
		if err := d.tenants.verify(tenant); err != nil {
			return Position{}, err
		}
	}
	geo, err := d.dataGeo(ctx)
	if err != nil {
		return Position{}, err
	}

	type staged struct {
		decl    *eventDecl
		payload any
		data    []byte
		eventID string
	}
	sts := make([]staged, len(payloads))
	for i, p := range payloads {
		v := reflect.ValueOf(p)
		if v.Kind() == reflect.Pointer {
			v = v.Elem()
			p = v.Interface()
		}
		decl := m.es.byType[v.Type()]
		if decl == nil {
			return Position{}, fmt.Errorf("orm: %s: Event-Typ %T ist nicht deklariert", m.name, p)
		}
		data, err := json.Marshal(p)
		if err != nil {
			return Position{}, fmt.Errorf("orm: Event %s serialisieren: %w", decl.name, err)
		}
		sts[i] = staged{decl: decl, payload: p, data: data, eventID: NewID().String()}
	}

	ev := esEventsTable(m)
	now := nowUTC()
	occurred := now.Format(time.RFC3339Nano)
	var lastSeq int64
	var applied bool

	write := func(q queryer) error {
		// Fastpath: Log-Spitze (Version + Heimat-Geo) UND Geo-Sequenz in
		// EINEM Statement — der Normalfall (bestehendes Aggregat) macht vor
		// den Inserts nur noch eine Abfrage. Optimistische Prüfung: liegt
		// die Spitze über der geladenen Version, war jemand schneller
		// (kleiner darf sie sein — Events vor dem Snapshot archiviert;
		// Duplikate verhindert der PK). Geo-Pinning: das Daten-Geo klebt ab
		// dem ersten Event am Aggregat, unabhängig vom Context-Geo.
		var cur, geoSeq int64
		var homeGeo string
		row := q.QueryRowContext(ctx, m.es.sqlTopAndSeq, a.id.String())
		switch err := row.Scan(&cur, &homeGeo, &geoSeq); err {
		case nil:
			geo = homeGeo
		case sql.ErrNoRows:
			cur = 0 // neues Aggregat, Geo bleibt das aus dem Context
		default:
			return err
		}
		if cur > a.version {
			return ErrVersionConflict
		}
		// Vergabe der Geo-Sequenz serialisieren und danach NEU lesen: Der
		// Wert oben stammt von vor der Sperre, in der Zwischenzeit kann ein
		// anderer Append committet haben. Erst hier steht das Geo fest —
		// bei einem bestehenden Aggregat gewinnt sein Heimat-Geo.
		if err := d.dial.lockGeoSeq(ctx, q, ev, geo); err != nil {
			return err
		}
		if err := q.QueryRowContext(ctx, m.es.sqlGeoSeq, geo).Scan(&geoSeq); err != nil {
			return err
		}
		for i, st := range sts {
			full := m.es.fullType(st.decl.name, st.decl.version)
			typeID, ok := d.esTypes.idOf(full)
			if !ok {
				return fmt.Errorf("orm: Event-Typ %q fehlt im Wörterbuch", full)
			}
			args := []any{a.id.String(), a.version + int64(i) + 1, geo, geoSeq + int64(i) + 1,
				st.eventID, occurred, typeID, string(st.data)}
			if m.tenanted() {
				args = append(args, tenant.String())
			}
			if _, err := q.ExecContext(ctx, m.es.sqlInsert, args...); err != nil {
				return err
			}
		}
		lastSeq = geoSeq + int64(len(sts))
		// Apply innerhalb der Transaktion: ein Apply-Fehler rollt die Events zurück.
		applied = true
		ap := a.self.(applier)
		for i, st := range sts {
			e := Event{Type: st.decl.name, Version: st.decl.version, Time: now, Seq: a.version + int64(i) + 1, Payload: st.payload}
			if err := ap.Apply(e); err != nil {
				return fmt.Errorf("orm: %s.Apply: %w", m.name, err)
			}
		}
		// Read-Model in DERSELBEN Transaktion: der Zustand liegt nach Apply
		// im Speicher, die Zeile kostet einen Upsert. Damit ist das
		// Read-Model ab dem Commit auf jedem Knoten konsistent zum Log —
		// ohne auf einen Worker zu warten, der die Lease womöglich auf einem
		// anderen Knoten hält und Sekunden braucht. Der Worker bleibt das
		// Netz darunter (Reaktoren, Snapshots, Nachprojektion).
		return d.upsertReadModel(ctx, q, m, a.id.String(), tenant, geo,
			reflect.ValueOf(a.self).Elem(), a.version+int64(len(sts)))
	}

	// Unter echter Nebenläufigkeit (PG/YB) kann die MAX-Prüfung zweier
	// paralleler Appends gleichzeitig bestehen — dann entscheidet der PK
	// (aggregate_id, aggregate_seq) ⇒ ErrVersionConflict. Kollisionen auf
	// der Geo-Sequenz (anderes Aggregat, gleiche seq) werden wiederholt —
	// ebenso ein Serialisierungsabbruch (SQLSTATE 40001), mit dem
	// YugabyteDB unter Snapshot-Isolation denselben Wettlauf meldet.
	// Beides passiert vor Apply, der In-Memory-Zustand bleibt unberührt.
	//
	// Wiederholt wird nur, solange Apply noch nicht lief: danach traegt der
	// In-Memory-Zustand die Events bereits, ein zweiter Durchlauf wendete
	// sie doppelt an. Scheitert es erst dahinter (Read-Model, Commit), ist
	// der Zustand stale und der Aufrufer laedt per Refresh neu.
	if a.rt.h.inTx() {
		err = classifyAppendErr(write(a.rt.h.q()), ev)
	} else {
		for attempt := 0; ; attempt++ {
			applied = false
			err = d.Tx(ctx, func(tx Tx) error { return write(tx.q()) })
			if err != nil && !applied && attempt < 8 && (isSeqCollision(err, ev) || isSerializationFailure(err)) {
				time.Sleep(time.Duration(attempt+1) * 10 * time.Millisecond)
				continue
			}
			err = classifyAppendErr(err, ev)
			break
		}
	}
	if err != nil {
		return Position{}, err
	}

	firstAggSeq := a.version + 1
	a.version += int64(len(sts))
	a.updatedAt = now

	// Trigger-Kette: Watcher benachrichtigen (nur wenn jemand zuhört —
	// der Envelope-Bau lohnt sonst nicht), Worker wecken.
	base := lastSeq - int64(len(sts))
	for i, st := range sts {
		if !d.hasWatchers(m) {
			break
		}
		d.publish(m, CloudEvent{
			SpecVersion:     "1.0",
			ID:              st.eventID,
			Source:          "/orm/" + m.table,
			Type:            m.es.fullType(st.decl.name, st.decl.version),
			Subject:         a.id.String(),
			Time:            now,
			DataContentType: "application/json",
			Data:            st.data,
			Tenant:          tenant,
			Geo:             geo,
			Sequence:        base + int64(i) + 1,
			AggregateSeq:    firstAggSeq + int64(i),
		})
	}
	d.wakeWorker()
	return Position{seqs: map[string]int64{geo: lastSeq}, projected: true}, nil
}

// --- Nachladen & Zeitreisen ---

// Refresh lädt das Aggregat neu (z. B. nach ErrVersionConflict).
func (a *Aggregate) Refresh(ctx context.Context, opts ...LoadOption) error {
	if a.rt == nil {
		return fmt.Errorf("orm: Aggregat nicht initialisiert — mit orm.New oder orm.Load erzeugen")
	}
	d := a.rt.h.db()
	m := a.rt.m
	var lo loadOpts
	for _, o := range opts {
		o(&lo)
	}
	tenant, err := esTenant(ctx, m)
	if err != nil {
		return err
	}
	if lo.wait {
		if err := d.waitForProjection(ctx, m, lo.pos, lo.timeout); err != nil {
			return err
		}
	}
	fresh := reflect.New(m.goType)
	freshAgg := wireAggregate(m, fresh.Interface(), a.rt.h, a.id)
	if err := d.foldInto(ctx, readQ(a.rt.h), m, freshAgg, tenant, 0, nil); err != nil {
		return err
	}
	if freshAgg.version == 0 {
		return ErrNotFound
	}
	// Zustand in das bestehende Objekt kopieren, Verdrahtung erhalten.
	self := a.self
	rt := a.rt
	reflect.ValueOf(self).Elem().Set(fresh.Elem())
	na := aggOf(m, self)
	na.self = self
	na.rt = rt
	return nil
}

// AtVersion liefert den Zustand nach Event version als neues Objekt (*T des Models).
func (a *Aggregate) AtVersion(ctx context.Context, version int64) (any, error) {
	return a.rebuild(ctx, version, nil)
}

// AtTime liefert den Zustand zu einem Zeitpunkt als neues Objekt (*T des Models).
func (a *Aggregate) AtTime(ctx context.Context, at time.Time) (any, error) {
	return a.rebuild(ctx, 0, &at)
}

func (a *Aggregate) rebuild(ctx context.Context, upToSeq int64, upToTime *time.Time) (any, error) {
	if a.rt == nil {
		return nil, fmt.Errorf("orm: Aggregat nicht initialisiert — mit orm.New oder orm.Load erzeugen")
	}
	d := a.rt.h.db()
	m := a.rt.m
	tenant, err := esTenant(ctx, m)
	if err != nil {
		return nil, err
	}
	fresh := reflect.New(m.goType).Interface()
	freshAgg := wireAggregate(m, fresh, a.rt.h, a.id)
	if err := d.foldInto(ctx, readQ(a.rt.h), m, freshAgg, tenant, upToSeq, upToTime); err != nil {
		return nil, err
	}
	if freshAgg.version == 0 {
		return nil, ErrNotFound
	}
	return fresh, nil
}

// History liefert den Event-Strom dieses Aggregats als CloudEvents,
// aufsteigend nach Aggregat-Sequenz.
func (a *Aggregate) History(ctx context.Context) iter.Seq2[CloudEvent, error] {
	return func(yield func(CloudEvent, error) bool) {
		if a.rt == nil {
			yield(CloudEvent{}, fmt.Errorf("orm: Aggregat nicht initialisiert — mit orm.New oder orm.Load erzeugen"))
			return
		}
		d := a.rt.h.db()
		m := a.rt.m
		tenant, err := esTenant(ctx, m)
		if err != nil {
			yield(CloudEvent{}, err)
			return
		}
		// Chunkweise materialisieren: kein offener Cursor während der
		// Konsument arbeitet (SQLite hat eine Verbindung). Historie liest
		// transparent Hot + Archiv.
		var after int64
		for {
			query := fmt.Sprintf(`SELECT %s FROM %s WHERE aggregate_id = ? AND aggregate_seq > ?`,
				esEventSelect(m), esEventsFrom(m, true))
			args := []any{a.id.String(), after}
			if m.tenanted() {
				query += ` AND tenant_id = ?`
				args = append(args, tenant.String())
			}
			query += ` ORDER BY aggregate_seq LIMIT 500`
			batch, err := fetchEventRows(ctx, readQ(a.rt.h), m, query, args)
			if err != nil {
				yield(CloudEvent{}, err)
				return
			}
			if len(batch) == 0 {
				return
			}
			for _, r := range batch {
				if !yield(d.cloudEvent(m, r), nil) {
					return
				}
				after = r.aggSeq
			}
		}
	}
}

// violatesIndex erkennt eine Constraint-Verletzung des Event-Logs am Namen.
//
// Bei Geo-Partitionierung meldet PG/YB den Index der PARTITION
// (conversation_events_geo_default_pkey), nicht den der Elterntabelle
// (conversation_events_pkey) — beide tragen aber Tabellennamen und Marker.
func violatesIndex(msg, eventsTable, marker string) bool {
	return strings.Contains(msg, eventsTable) && strings.Contains(msg, marker)
}

// classifyAppendErr mappt eine PK-Verletzung des Event-Logs (paralleler
// Append auf dasselbe Aggregat) auf ErrVersionConflict.
func classifyAppendErr(err error, eventsTable string) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	if violatesIndex(msg, eventsTable, "_pkey") || strings.Contains(msg, eventsTable+".aggregate_id") {
		return ErrVersionConflict
	}
	return err
}

// isSeqCollision erkennt eine Kollision auf dem Unique-Index (geo, seq) —
// zwei parallele Appends verschiedener Aggregate; wiederholbar.
func isSeqCollision(err error, eventsTable string) bool {
	msg := err.Error()
	return violatesIndex(msg, eventsTable, "geo_seq") || strings.Contains(msg, eventsTable+".geo")
}

// isSerializationFailure erkennt den Abbruch einer Transaktion durch einen
// Wettlauf (SQLSTATE 40001). YugabyteDB meldet so unter Snapshot-Isolation,
// was PostgreSQL als Unique-Verletzung meldet — dieselbe Ursache, anderer
// Text. Wiederholbar, solange ORM++ die Transaktion selbst führt.
func isSerializationFailure(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "40001"
}

// fetchEventRows liest eine Ergebnismenge vollständig ein und schließt den Cursor.
func fetchEventRows(ctx context.Context, q queryer, m *model, query string, args []any) ([]eventRow, error) {
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []eventRow
	for rows.Next() {
		r, err := scanEventRow(m, rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
