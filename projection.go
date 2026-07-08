package orm

import (
	"context"
	"database/sql"
	"fmt"
	"path"
	"reflect"
	"strings"
	"time"
)

// --- Checkpoints (ormpp_checkpoints) ---

func getCheckpoint(ctx context.Context, q queryer, consumer, geo string) (int64, error) {
	var seq int64
	err := q.QueryRowContext(ctx,
		`SELECT seq FROM ormpp_checkpoints WHERE consumer = ? AND geo = ?`, consumer, geo).Scan(&seq)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return seq, err
}

func setCheckpoint(ctx context.Context, q queryer, consumer, geo string, seq int64) error {
	_, err := q.ExecContext(ctx, `INSERT INTO ormpp_checkpoints (consumer, geo, seq) VALUES (?, ?, ?)
		ON CONFLICT (consumer, geo) DO UPDATE SET seq = excluded.seq`, consumer, geo, seq)
	return err
}

// eventGeos liefert alle Geos, die im Event-Log eines Models vorkommen.
func (d *DB) eventGeos(ctx context.Context, m *model) ([]string, error) {
	rows, err := d.sql.QueryContext(ctx, fmt.Sprintf(`SELECT DISTINCT geo FROM %q`, esEventsTable(m)))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var g string
		if err := rows.Scan(&g); err != nil {
			return nil, err
		}
		out = append(out, g)
	}
	return out, rows.Err()
}

// --- Worker ---

func (d *DB) wakeWorker() {
	select {
	case d.wake <- struct{}{}:
	default:
	}
}

func (d *DB) workerLoop(ctx context.Context) {
	defer d.workerWG.Done()
	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()
	for {
		d.processOnce(ctx)
		select {
		case <-ctx.Done():
			return
		case <-d.wake:
		case <-tick.C:
		}
	}
}

// processOnce fährt einen Verarbeitungsdurchlauf: Heartbeat, Projektionen,
// Reaktoren, Snapshots, Dual-Write-Nachlauf. Fehler werden geschluckt und im
// nächsten Durchlauf erneut versucht (Checkpoints bleiben stehen — at-least-once).
func (d *DB) processOnce(ctx context.Context) {
	if !d.migrated {
		return
	}
	if time.Since(d.lastBeat) >= heartbeatEvery {
		if d.heartbeat(ctx) == nil {
			d.lastBeat = time.Now()
		}
	}
	for _, m := range d.reg.ordered {
		if m.kind != kindEventSourced {
			continue
		}
		_ = d.processProjection(ctx, m)
		_ = d.maybeSnapshot(ctx, m)
	}
	d.reactorMu.Lock()
	reactors := append([]*reactor(nil), d.reactors...)
	d.reactorMu.Unlock()
	for _, r := range reactors {
		_ = d.processReactor(ctx, r)
	}
	_ = d.drainDualWrite(ctx)
}

// --- Eingebaute Projektion (Read-Model) ---

func (d *DB) processProjection(ctx context.Context, m *model) error {
	consumer := "projection:" + m.table
	geos, err := d.eventGeos(ctx, m)
	if err != nil {
		return err
	}
	for _, geo := range geos {
		cp, err := getCheckpoint(ctx, d.sql, consumer, geo)
		if err != nil {
			return err
		}
		type touched struct {
			id     string
			tenant ID
		}
		var order []touched
		seen := map[string]bool{}
		var last int64
		{
			sel := `"aggregate_id", "seq"`
			if m.tenanted() {
				sel = `"aggregate_id", "tenant_id", "seq"`
			}
			query := fmt.Sprintf(`SELECT %s FROM %q WHERE geo = ? AND seq > ? ORDER BY seq`, sel, esEventsTable(m))
			rows, err := d.sql.QueryContext(ctx, query, geo, cp)
			if err != nil {
				return err
			}
			for rows.Next() {
				var t touched
				var seq int64
				var err error
				if m.tenanted() {
					err = rows.Scan(&t.id, &t.tenant, &seq)
				} else {
					err = rows.Scan(&t.id, &seq)
				}
				if err != nil {
					rows.Close()
					return err
				}
				if !seen[t.id] {
					seen[t.id] = true
					order = append(order, t)
				}
				last = seq
			}
			rows.Close()
			if err := rows.Err(); err != nil {
				return err
			}
		}
		if len(order) == 0 {
			continue
		}
		err = d.Tx(ctx, func(tx Tx) error {
			for _, t := range order {
				if err := d.projectAggregate(ctx, tx.q(), m, t.id, t.tenant, geo); err != nil {
					return err
				}
			}
			return setCheckpoint(ctx, tx.q(), consumer, geo, last)
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// projectAggregate faltet ein Aggregat und schreibt es ins Read-Model.
func (d *DB) projectAggregate(ctx context.Context, q queryer, m *model, aggID string, tenant ID, geo string) error {
	id, err := ParseID(aggID)
	if err != nil {
		return err
	}
	inst := reflect.New(m.goType)
	agg := wireAggregate(m, inst.Interface(), d, id)
	if err := d.foldInto(ctx, q, m, agg, tenant, 0, nil); err != nil {
		return err
	}
	if agg.version == 0 {
		return nil
	}
	rv := inst.Elem()
	cols := []string{"id"}
	vals := []any{aggID}
	for _, f := range m.fields {
		v, err := encodeField(f, rv.FieldByIndex(f.index))
		if err != nil {
			return err
		}
		cols = append(cols, f.column)
		vals = append(vals, v)
	}
	if m.tenanted() {
		cols = append(cols, "tenant_id")
		vals = append(vals, tenant.String())
	}
	cols = append(cols, "geo", "aggregate_seq")
	vals = append(vals, geo, agg.version)

	var updates []string
	for _, c := range cols[1:] {
		updates = append(updates, fmt.Sprintf("%q = excluded.%q", c, c))
	}
	query := insertSQL(m.table, cols) + fmt.Sprintf(` ON CONFLICT ("id") DO UPDATE SET %s`, strings.Join(updates, ", "))
	_, err = q.ExecContext(ctx, query, vals...)
	return err
}

// --- Reaktoren (OnEvent) ---

type reactor struct {
	typ     reflect.Type
	name    string
	pattern string
	fn      func(context.Context, CloudEvent, Tx) error
}

// ConsumerOption konfiguriert einen OnEvent-Konsumenten.
type ConsumerOption func(*reactor)

// Named gibt dem Konsumenten einen stabilen Namen (für RebuildView und
// den Checkpoint). Ohne Named: Model-Name + Pattern.
func Named(name string) ConsumerOption {
	return func(r *reactor) { r.name = name }
}

// OnEvent registriert einen persistenten, checkpointed Reaktor: at-least-once,
// Handler müssen idempotent sein. pattern matcht den Event-Kurznamen
// (Glob, z. B. "zone.*"). Registrierung vor StartWorkers.
func OnEvent[T any](d *DB, pattern string, fn func(ctx context.Context, ce CloudEvent, tx Tx) error, opts ...ConsumerOption) {
	r := &reactor{typ: reflect.TypeFor[T](), pattern: pattern, fn: fn}
	for _, o := range opts {
		o(r)
	}
	if r.name == "" {
		r.name = r.typ.Name() + ":" + pattern
	}
	d.reactorMu.Lock()
	d.reactors = append(d.reactors, r)
	d.reactorMu.Unlock()
}

func (d *DB) processReactor(ctx context.Context, r *reactor) error {
	m := d.reg.models[r.typ]
	if m == nil || m.kind != kindEventSourced || m.es.prefix == "" {
		return nil
	}
	consumer := "view:" + r.name
	geos, err := d.eventGeos(ctx, m)
	if err != nil {
		return err
	}
	for _, geo := range geos {
		for {
			cp, err := getCheckpoint(ctx, d.sql, consumer, geo)
			if err != nil {
				return err
			}
			query := fmt.Sprintf(`SELECT %s FROM %q WHERE geo = ? AND seq > ? ORDER BY seq LIMIT 500`,
				esEventSelect(m), esEventsTable(m))
			batch, err := fetchEventRows(ctx, d.sql, m, query, []any{geo, cp})
			if err != nil {
				return err
			}
			if len(batch) == 0 {
				break
			}
			for _, row := range batch {
				ce := d.cloudEvent(m, row)
				name, _, ok := m.es.parseType(ce.Type)
				matched := false
				if ok {
					matched, _ = path.Match(r.pattern, name)
				}
				err := d.Tx(ctx, func(tx Tx) error {
					if matched {
						if err := r.fn(ctx, ce, tx); err != nil {
							return err
						}
					}
					return setCheckpoint(ctx, tx.q(), consumer, geo, row.seq)
				})
				if err != nil {
					return err // Checkpoint bleibt stehen — Retry im nächsten Durchlauf
				}
			}
		}
	}
	return nil
}

// --- Snapshots ---

// maybeSnapshot erzeugt Snapshots nach der Model-Politik: alle n Events pro
// Aggregat oder spätestens nach SnapshotMaxAge; Aufbewahrung KeepLast.
// Läuft asynchron im Worker — nie im Schreibpfad.
func (d *DB) maybeSnapshot(ctx context.Context, m *model) error {
	if m.opts.snapshotDisabled {
		return nil
	}
	every := m.opts.snapshotEvery
	if every <= 0 {
		every = d.opts.defaultSnapshotEach
	}
	maxAge := m.opts.snapshotMaxAge
	if every <= 0 && maxAge <= 0 {
		return nil
	}
	ev, sn := esEventsTable(m), esSnapsTable(m)

	type candidate struct {
		id     string
		tenant ID
		cur    int64
		oldest time.Time // ältestes Event (Basis für MaxAge ohne Snapshot)
	}
	var cands []candidate
	{
		sel := `"aggregate_id", MAX("aggregate_seq"), MIN("occurred_at")`
		group := `"aggregate_id"`
		if m.tenanted() {
			sel = `"aggregate_id", "tenant_id", MAX("aggregate_seq"), MIN("occurred_at")`
			group = `"aggregate_id", "tenant_id"`
		}
		rows, err := d.sql.QueryContext(ctx, fmt.Sprintf(`SELECT %s FROM %q GROUP BY %s`, sel, ev, group))
		if err != nil {
			return err
		}
		for rows.Next() {
			var c candidate
			var oldest string
			var err error
			if m.tenanted() {
				err = rows.Scan(&c.id, &c.tenant, &c.cur, &oldest)
			} else {
				err = rows.Scan(&c.id, &c.cur, &oldest)
			}
			if err != nil {
				rows.Close()
				return err
			}
			if t, err := time.Parse(time.RFC3339Nano, oldest); err == nil {
				c.oldest = t
			}
			cands = append(cands, c)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
	}

	type snapState struct {
		seq   int64
		taken time.Time
	}
	snaps := map[string]snapState{}
	{
		rows, err := d.sql.QueryContext(ctx, fmt.Sprintf(
			`SELECT "aggregate_id", MAX("aggregate_seq"), MAX("taken_at") FROM %q GROUP BY "aggregate_id"`, sn))
		if err != nil {
			return err
		}
		for rows.Next() {
			var id, taken string
			var seq int64
			if err := rows.Scan(&id, &seq, &taken); err != nil {
				rows.Close()
				return err
			}
			st := snapState{seq: seq}
			if t, err := time.Parse(time.RFC3339Nano, taken); err == nil {
				st.taken = t
			}
			snaps[id] = st
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
	}

	now := nowUTC()
	for _, c := range cands {
		st := snaps[c.id]
		if c.cur <= st.seq {
			continue
		}
		need := every > 0 && c.cur-st.seq >= int64(every)
		if !need && maxAge > 0 {
			ref := st.taken
			if ref.IsZero() {
				ref = c.oldest
			}
			need = !ref.IsZero() && now.Sub(ref) >= maxAge
		}
		if !need {
			continue
		}
		if err := d.snapshotAggregate(ctx, m, c.id, c.tenant); err != nil {
			return err
		}
	}
	return nil
}

func (d *DB) snapshotAggregate(ctx context.Context, m *model, aggID string, tenant ID) error {
	id, err := ParseID(aggID)
	if err != nil {
		return err
	}
	inst := reflect.New(m.goType)
	agg := wireAggregate(m, inst.Interface(), d, id)
	if err := d.foldInto(ctx, d.sql, m, agg, tenant, 0, nil); err != nil {
		return err
	}
	if agg.version == 0 {
		return nil
	}
	state, err := marshalSnapshot(agg.self)
	if err != nil {
		return fmt.Errorf("orm: Snapshot von %s serialisieren: %w", m.name, err)
	}
	sn := esSnapsTable(m)
	cols := []string{"aggregate_id", "aggregate_seq", "taken_at", "state"}
	vals := []any{aggID, agg.version, nowUTC().Format(time.RFC3339Nano), state}
	if m.tenanted() {
		cols = append(cols, "tenant_id")
		vals = append(vals, tenant.String())
	}
	return d.Tx(ctx, func(tx Tx) error {
		query := insertSQL(sn, cols) + ` ON CONFLICT ("aggregate_id", "aggregate_seq")
			DO UPDATE SET taken_at = excluded.taken_at, state = excluded.state`
		if _, err := tx.q().ExecContext(ctx, query, vals...); err != nil {
			return err
		}
		keep := m.opts.snapshotKeepLast
		if keep < 1 {
			keep = 1
		}
		prune := fmt.Sprintf(`DELETE FROM %q WHERE aggregate_id = ? AND aggregate_seq NOT IN
			(SELECT aggregate_seq FROM %q WHERE aggregate_id = ? ORDER BY aggregate_seq DESC LIMIT %d)`,
			sn, sn, keep)
		_, err := tx.q().ExecContext(ctx, prune, aggID, aggID)
		return err
	})
}

// --- Read-your-writes & Rebuild ---

// waitForProjection blockiert, bis die eingebaute Projektion die Position
// erreicht hat — sonst ErrWaitTimeout.
func (d *DB) waitForProjection(ctx context.Context, m *model, pos Position, timeout time.Duration) error {
	if len(pos.seqs) == 0 {
		return nil
	}
	consumer := "projection:" + m.table
	deadline := time.Now().Add(timeout)
	for {
		done := true
		for geo, want := range pos.seqs {
			cp, err := getCheckpoint(ctx, d.sql, consumer, geo)
			if err != nil {
				return err
			}
			if cp < want {
				done = false
				break
			}
		}
		if done {
			return nil
		}
		if time.Now().After(deadline) {
			return ErrWaitTimeout
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// RebuildProjection baut das Read-Model eines ES-Models komplett neu aus
// dem Event-Strom auf (synchron).
func RebuildProjection[T any](ctx context.Context, h Handle) error {
	d := h.db()
	m := d.reg.models[reflect.TypeFor[T]()]
	if m == nil || m.kind != kindEventSourced {
		return fmt.Errorf("orm: RebuildProjection[%T]: kein registriertes EventSourced-Model", *new(T))
	}
	consumer := "projection:" + m.table
	err := d.Tx(ctx, func(tx Tx) error {
		if _, err := tx.q().ExecContext(ctx, fmt.Sprintf(`DELETE FROM %q`, m.table)); err != nil {
			return err
		}
		_, err := tx.q().ExecContext(ctx, `DELETE FROM ormpp_checkpoints WHERE consumer = ?`, consumer)
		return err
	})
	if err != nil {
		return err
	}
	return d.processProjection(ctx, m)
}

// RebuildView spielt den Event-Strom erneut durch einen benannten
// OnEvent-Konsumenten (synchron). Handler müssen idempotent sein.
func RebuildView(ctx context.Context, d *DB, name string) error {
	d.reactorMu.Lock()
	var target *reactor
	for _, r := range d.reactors {
		if r.name == name {
			target = r
			break
		}
	}
	d.reactorMu.Unlock()
	if target == nil {
		return fmt.Errorf("orm: unbekannte View %q", name)
	}
	if _, err := d.sql.ExecContext(ctx, `DELETE FROM ormpp_checkpoints WHERE consumer = ?`, "view:"+name); err != nil {
		return err
	}
	return d.processReactor(ctx, target)
}
