package orm

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"
)

// Zustandsmaschine der Online-Migration (Expand/Contract):
// idle → expanding → backfill → dual-write → finalizing → idle
const (
	phaseIdle       = "idle"
	phaseExpanding  = "expanding"
	phaseBackfill   = "backfill"
	phaseDualWrite  = "dual-write"
	phaseFinalizing = "finalizing"
)

// schemaState ist die eine Zeile in ormpp_schema_state.
type schemaState struct {
	current  int
	target   int // 0 = keine Migration unterwegs
	phase    string
	checksum string
}

func (d *DB) readSchemaState(ctx context.Context) (schemaState, error) {
	var st schemaState
	row := d.q().QueryRowContext(ctx,
		`SELECT schema_version, target_version, phase, models_checksum FROM ormpp_schema_state WHERE id = 1`)
	switch err := row.Scan(&st.current, &st.target, &st.phase, &st.checksum); err {
	case nil:
		return st, nil
	case sql.ErrNoRows:
		return schemaState{phase: phaseIdle}, nil
	default:
		return schemaState{}, err
	}
}

func (d *DB) writeSchemaState(ctx context.Context, q queryer, st schemaState) error {
	_, err := q.ExecContext(ctx, `
		INSERT INTO ormpp_schema_state (id, schema_version, target_version, phase, models_checksum, updated_at)
		VALUES (1, ?, ?, ?, ?, ?)
		ON CONFLICT (id) DO UPDATE SET schema_version = excluded.schema_version,
			target_version = excluded.target_version, phase = excluded.phase,
			models_checksum = excluded.models_checksum, updated_at = excluded.updated_at`,
		st.current, st.target, st.phase, st.checksum, nowUTC().Format(time.RFC3339Nano))
	return err
}

// setPhase schreibt den Phasenwechsel in den Zustand und ins Audit-Log.
func (d *DB) setPhase(ctx context.Context, st *schemaState, phase string) error {
	from := st.phase
	if from == "" {
		from = phaseIdle
	}
	st.phase = phase
	if err := d.writeSchemaState(ctx, d.q(), *st); err != nil {
		return err
	}
	version := st.target
	if version == 0 {
		version = st.current
	}
	_, err := d.q().ExecContext(ctx, `
		INSERT INTO ormpp_schema_history (version, phase_from, phase_to, applied_at, applied_by)
		VALUES (?, ?, ?, ?, ?)`,
		version, from, phase, nowUTC().Format(time.RFC3339Nano), d.instanceID.String())
	return err
}

// --- Upgrade-Lauf (expanding → backfill → dual-write) ---

// versionSteps sind die kompilierten Schritte einer Zielversion.
type versionSteps struct {
	version  int
	replaces []*compiledReplace
	scripts  []*batchScript
}

// compileSteps kompiliert die Schritte für die Versionen from..to (aufsteigend).
func (d *DB) compileSteps(from, to int) ([]versionSteps, error) {
	var out []versionSteps
	for v := from; v <= to; v++ {
		vs := versionSteps{version: v}
		for _, s := range d.migrations[v] {
			switch step := s.(type) {
			case *replaceStep:
				cr, err := d.compileReplace(step)
				if err != nil {
					return nil, err
				}
				vs.replaces = append(vs.replaces, cr)
			case *batchScript:
				vs.scripts = append(vs.scripts, step)
			default:
				return nil, fmt.Errorf("orm: unbekannter Migrationsschritt %T", s)
			}
		}
		out = append(out, vs)
	}
	return out, nil
}

// setActiveReplaces aktiviert die Replace-Schritte für den Dual-Write-Drain
// (Worker verarbeitet die Trigger-Queue nur mit bekannter Transformation).
func (d *DB) setActiveReplaces(pending []versionSteps) {
	active := map[string]*compiledReplace{}
	for _, vs := range pending {
		for _, cr := range vs.replaces {
			active[cr.oldM.table] = cr
		}
	}
	d.dwMu.Lock()
	d.activeReplace = active
	d.dwMu.Unlock()
}

// runUpgrade fährt die Migration von st.current auf d.schemaVersion bis in
// die Dual-Write-Phase. Wiederaufnehmbar: Phase und Backfill-Checkpoints
// liegen in der DB.
func (d *DB) runUpgrade(ctx context.Context, st schemaState, plan MigrationPlan) error {
	target := d.schemaVersion

	pending, err := d.compileSteps(st.current+1, target)
	if err != nil {
		return err
	}
	d.setActiveReplaces(pending)

	// Leader-Lease: genau eine Instanz führt aus, die anderen warten.
	for {
		ok, err := d.acquireLease(ctx, "migration", 30*time.Second)
		if err != nil {
			return err
		}
		if ok {
			break
		}
		cur, err := d.readSchemaState(ctx)
		if err != nil {
			return err
		}
		if cur.current >= target || (cur.target == target && cur.phase == phaseDualWrite) {
			return nil // fremder Leader ist fertig
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
	defer d.releaseLease(bgCtx(), "migration")

	// Zustand ggf. neu laden (Lease-Wartezeit) und Ziel setzen.
	st, err = d.readSchemaState(ctx)
	if err != nil {
		return err
	}
	if st.current >= target {
		return nil
	}
	if st.phase == phaseIdle || st.phase == "" || st.target != target {
		st.target = target
		if err := d.setPhase(ctx, &st, phaseExpanding); err != nil {
			return err
		}
	}

	// expanding — idempotent, läuft auch bei Wiederaufnahme:
	// additive DDL, Deprecated-Markierung, Dual-Write-Trigger auf Alt-Tabellen.
	if err := d.applySchema(ctx); err != nil {
		return err
	}
	if err := d.markDeprecated(ctx, target); err != nil {
		return err
	}
	for _, vs := range pending {
		for _, cr := range vs.replaces {
			if err := d.createDualWriteTriggers(ctx, cr); err != nil {
				return err
			}
		}
	}
	if st.phase == phaseExpanding {
		if err := d.setPhase(ctx, &st, phaseBackfill); err != nil {
			return err
		}
	}

	// backfill — checkpointed und drosselbar; erledigte Schritte werden übersprungen.
	if st.phase == phaseBackfill {
		for _, vs := range pending {
			for _, cr := range vs.replaces {
				if err := d.backfillReplace(ctx, vs.version, cr, plan); err != nil {
					return err
				}
			}
			for _, bs := range vs.scripts {
				if err := d.runBatchScript(ctx, vs.version, bs); err != nil {
					return err
				}
			}
		}
		if err := d.setPhase(ctx, &st, phaseDualWrite); err != nil {
			return err
		}
	}
	return nil
}

// markDeprecated registriert alle deprecated-Felder im Register (nie löschen —
// entfernt wird erst bei FinalizeMigration, wenn das Feld aus dem Struct ist).
func (d *DB) markDeprecated(ctx context.Context, version int) error {
	for _, m := range d.reg.ordered {
		for _, f := range m.fields {
			if !f.deprecated {
				continue
			}
			if _, err := d.q().ExecContext(ctx, `
				INSERT INTO ormpp_deprecated (model, column_name, deprecated_in) VALUES (?, ?, ?)
				ON CONFLICT (model, column_name) DO NOTHING`, m.name, f.column, version); err != nil {
				return err
			}
		}
	}
	return nil
}

// createDualWriteTriggers legt Insert/Update/Delete-Trigger auf die alte
// Tabelle: jede Änderung einer Alt-Instanz landet in der Nachlauf-Queue.
// Die Trigger-DDL liefert der Dialekt (SQLite: drei Trigger; PG/YB:
// plpgsql-Funktion + Zeilen-Trigger).
func (d *DB) createDualWriteTriggers(ctx context.Context, cr *compiledReplace) error {
	t := cr.oldM.table
	for _, stmt := range d.dial.dualWriteTriggerSQL(t, cr.oldM.pk.column) {
		if _, err := d.q().ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("orm: Dual-Write-Trigger auf %s: %w", t, err)
		}
	}
	return nil
}

// --- Backfill ---

// oldRow ist eine gelesene Zeile der Alt-Tabelle samt Scope-Spalten.
type oldRow struct {
	ptr    any // *Old
	pk     string
	tenant string
	geo    string
}

// queryOldRows liest Zeilen der Alt-Tabelle (materialisiert, PK-sortiert).
func (d *DB) queryOldRows(ctx context.Context, q queryer, m *model, cond string, args []any, limit int) ([]oldRow, error) {
	sel := make([]string, 0, len(m.fields)+2)
	for _, f := range m.fields {
		sel = append(sel, f.column)
	}
	if m.tenanted() {
		sel = append(sel, "tenant_id")
	}
	sel = append(sel, "geo")
	query := fmt.Sprintf("SELECT %s FROM %q %s ORDER BY %q LIMIT %d",
		quoteAll(sel...), m.table, cond, m.pk.column, limit)
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []oldRow
	for rows.Next() {
		raws := make([]any, len(sel))
		ptrs := make([]any, len(sel))
		for i := range raws {
			ptrs[i] = &raws[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		inst := reflect.New(m.goType)
		rv := inst.Elem()
		for i, f := range m.fields {
			if err := decodeField(f, rv.FieldByIndex(f.index), raws[i]); err != nil {
				return nil, err
			}
		}
		r := oldRow{ptr: inst.Interface(), pk: rv.FieldByIndex(m.pk.index).Interface().(ID).String()}
		i := len(m.fields)
		if m.tenanted() {
			r.tenant, _ = rawString(raws[i])
			i++
		}
		r.geo, _ = rawString(raws[i])
		out = append(out, r)
	}
	return out, rows.Err()
}

// transformAndUpsert wendet die alt→neu-Transformation an und schreibt die
// Zeile in die neue Tabelle — Tenant, Geo und (Default) die ID bleiben erhalten.
func (d *DB) transformAndUpsert(ctx context.Context, q queryer, cr *compiledReplace, r oldRow) error {
	newPtr, err := cr.step.fwd(ctx, r.ptr)
	if err != nil {
		return fmt.Errorf("orm: Transformation %s (%s): %w", cr.name, r.pk, err)
	}
	m := cr.newM
	rv := reflect.ValueOf(newPtr).Elem()
	pkv := rv.FieldByIndex(m.pk.index)
	if pkv.Interface().(ID).IsZero() {
		id, err := ParseID(r.pk)
		if err != nil {
			return err
		}
		pkv.Set(reflect.ValueOf(id))
	}

	cols := make([]string, 0, len(m.fields)+2)
	vals := make([]any, 0, len(m.fields)+2)
	for _, f := range m.fields {
		fv := rv.FieldByIndex(f.index)
		if f.required && fv.IsZero() {
			return fmt.Errorf("%w: %s.%s (Backfill %s)", ErrRequiredField, m.name, f.name, cr.name)
		}
		if err := checkEnum(m, f, fv); err != nil {
			return err
		}
		v, err := encodeField(f, fv)
		if err != nil {
			return err
		}
		cols = append(cols, f.column)
		vals = append(vals, v)
	}
	if m.tenanted() {
		cols = append(cols, "tenant_id")
		vals = append(vals, r.tenant)
	}
	cols = append(cols, "geo")
	vals = append(vals, r.geo)

	var updates []string
	for _, c := range cols {
		if c == m.pk.column {
			continue
		}
		updates = append(updates, fmt.Sprintf("%q = excluded.%q", c, c))
	}
	query := insertSQL(m.table, cols) + fmt.Sprintf(
		" ON CONFLICT (%q) DO UPDATE SET %s", m.pk.column, strings.Join(updates, ", "))
	_, err = q.ExecContext(ctx, query, vals...)
	return err
}

// backfillReplace kopiert/transformiert die Alt-Tabelle in Batches —
// checkpointed (wiederaufnehmbar) und drosselbar.
func (d *DB) backfillReplace(ctx context.Context, version int, cr *compiledReplace, plan MigrationPlan) error {
	p, err := d.readProgress(ctx, version, cr.name)
	if err != nil {
		return err
	}
	if p.state == "done" {
		return nil
	}
	last, done := p.lastKey, p.rowsDone
	for {
		cond, args := "", []any{}
		if last != "" {
			cond = fmt.Sprintf("WHERE %q > ?", cr.oldM.pk.column)
			args = append(args, last)
		}
		batch, err := d.queryOldRows(ctx, d.q(), cr.oldM, cond, args, plan.BatchSize)
		if err != nil {
			return err
		}
		if len(batch) == 0 {
			break
		}
		started := time.Now()
		err = d.Tx(ctx, func(tx Tx) error {
			for _, r := range batch {
				if err := d.transformAndUpsert(ctx, tx.q(), cr, r); err != nil {
					return err
				}
			}
			done += int64(len(batch))
			return d.writeProgress(ctx, tx.q(), version, cr.name, batch[len(batch)-1].pk, done, "running")
		})
		if err != nil {
			return fmt.Errorf("orm: Backfill %s: %w", cr.name, err)
		}
		last = batch[len(batch)-1].pk
		throttleBatch(plan.Throttle, len(batch), started)
	}
	return d.writeProgress(ctx, d.q(), version, cr.name, last, done, "done")
}

// runBatchScript führt ein Skript aus; den Checkpoint verwaltet das Skript
// selbst über Batch. Erfolg markiert den Schritt als erledigt.
func (d *DB) runBatchScript(ctx context.Context, version int, bs *batchScript) error {
	step := "script:" + bs.name
	p, err := d.readProgress(ctx, version, step)
	if err != nil {
		return err
	}
	if p.state == "done" {
		return nil
	}
	if err := bs.fn(ctx, Batch{d: d, version: version, step: step}); err != nil {
		return fmt.Errorf("orm: BatchScript %q: %w", bs.name, err)
	}
	p, err = d.readProgress(ctx, version, step)
	if err != nil {
		return err
	}
	return d.writeProgress(ctx, d.q(), version, step, p.lastKey, p.rowsDone, "done")
}

func throttleBatch(rate Rate, n int, started time.Time) {
	if rate <= 0 {
		return
	}
	minTime := time.Duration(float64(n) / float64(rate) * float64(time.Second))
	if d := minTime - time.Since(started); d > 0 {
		time.Sleep(d)
	}
}

// --- Dual-Write-Nachlauf ---

// drainDualWrite verarbeitet die Trigger-Queue: Schreibvorgänge von
// Alt-Instanzen werden in die neuen Tabellen nachgezogen. At-least-once —
// Zeilen werden erst nach erfolgreicher Verarbeitung gelöscht.
func (d *DB) drainDualWrite(ctx context.Context) error {
	d.dwMu.Lock()
	active := d.activeReplace
	d.dwMu.Unlock()
	if len(active) == 0 {
		return nil
	}
	for {
		type qrow struct {
			id          int64
			tbl, pk, op string
		}
		var batch []qrow
		{
			rows, err := d.q().QueryContext(ctx,
				`SELECT id, tbl, pk, op FROM ormpp_dualwrite_queue ORDER BY id LIMIT 200`)
			if err != nil {
				return err
			}
			for rows.Next() {
				var r qrow
				if err := rows.Scan(&r.id, &r.tbl, &r.pk, &r.op); err != nil {
					rows.Close()
					return err
				}
				batch = append(batch, r)
			}
			rows.Close()
			if err := rows.Err(); err != nil {
				return err
			}
		}
		if len(batch) == 0 {
			return nil
		}
		progressed := false
		for _, r := range batch {
			cr := active[r.tbl]
			if cr == nil {
				continue // Queue-Eintrag einer fremden Generation
			}
			if err := d.applyQueued(ctx, cr, r.pk, r.op); err != nil {
				return err
			}
			if _, err := d.q().ExecContext(ctx, `DELETE FROM ormpp_dualwrite_queue WHERE id = ?`, r.id); err != nil {
				return err
			}
			progressed = true
		}
		if !progressed {
			return nil
		}
	}
}

func (d *DB) applyQueued(ctx context.Context, cr *compiledReplace, pk, op string) error {
	if op == "delete" {
		_, err := d.q().ExecContext(ctx,
			fmt.Sprintf("DELETE FROM %q WHERE %q = ?", cr.newM.table, cr.newM.pk.column), pk)
		return err
	}
	cond := fmt.Sprintf("WHERE %q = ?", cr.oldM.pk.column)
	rows, err := d.queryOldRows(ctx, d.q(), cr.oldM, cond, []any{pk}, 1)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		// Zeile inzwischen gelöscht — Delete nachziehen.
		_, err := d.q().ExecContext(ctx,
			fmt.Sprintf("DELETE FROM %q WHERE %q = ?", cr.newM.table, cr.newM.pk.column), pk)
		return err
	}
	return d.transformAndUpsert(ctx, d.q(), cr, rows[0])
}

// --- Finalisierung ---

// FinalizeMigration schließt eine Migration explizit ab: Vorbedingung ist,
// dass keine lebende Instanz eine ältere Schema-Version meldet. Dann wird
// die Nachlauf-Queue geleert, Alt-Tabellen und als deprecated markierte,
// aus den Structs entfernte Spalten werden gedroppt, der Zustand geht auf idle.
func (d *DB) FinalizeMigration(ctx context.Context, version int) error {
	if !d.migrated {
		return fmt.Errorf("orm: Migrate muss vor FinalizeMigration laufen")
	}
	st, err := d.readSchemaState(ctx)
	if err != nil {
		return err
	}
	if st.current == version && st.target == 0 && st.phase == phaseIdle {
		return nil // bereits finalisiert — idempotent
	}
	if st.target != version || st.phase != phaseDualWrite {
		return fmt.Errorf("orm: Version %d ist nicht im Dual-Write (Phase %q, Ziel v%d): %w",
			version, st.phase, st.target, ErrMigrationPending)
	}
	if d.schemaVersion != version {
		return fmt.Errorf("orm: FinalizeMigration(%d) erfordert eine Instanz mit SchemaVersion %d (diese: %d): %w",
			version, version, d.schemaVersion, ErrMigrationPending)
	}
	if id, v, err := d.livingOlderInstance(ctx, version); err != nil {
		return err
	} else if id != "" {
		return fmt.Errorf("orm: Instanz %s meldet noch Schema v%d: %w", id, v, ErrMigrationPending)
	}
	ok, err := d.acquireLease(ctx, "migration", 30*time.Second)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("orm: Migrations-Lease ist belegt: %w", ErrMigrationPending)
	}
	defer d.releaseLease(bgCtx(), "migration")

	if err := d.setPhase(ctx, &st, phaseFinalizing); err != nil {
		return err
	}
	// Nachzügler vollständig einarbeiten, dann Dual-Write beenden.
	if err := d.drainDualWrite(ctx); err != nil {
		return err
	}
	d.dwMu.Lock()
	active := d.activeReplace
	d.activeReplace = nil
	d.dwMu.Unlock()
	for tbl := range active {
		// Trigger fallen mit der Tabelle.
		if _, err := d.q().ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %q", tbl)); err != nil {
			return fmt.Errorf("orm: Alt-Tabelle %s entfernen: %w", tbl, err)
		}
		if _, err := d.q().ExecContext(ctx, `DELETE FROM ormpp_dualwrite_queue WHERE tbl = ?`, tbl); err != nil {
			return err
		}
	}
	if err := d.dropRemovedDeprecated(ctx); err != nil {
		return err
	}

	st.current, st.target = version, 0
	st.checksum = d.reg.checksum()
	return d.setPhase(ctx, &st, phaseIdle)
}

// dropRemovedDeprecated entfernt Spalten, die als deprecated markiert wurden
// UND nicht mehr im Struct stehen. Felder mit deprecated-Tag im Struct
// bleiben — sie fallen erst, wenn der Entwickler sie aus dem Code nimmt.
func (d *DB) dropRemovedDeprecated(ctx context.Context) error {
	type entry struct{ model, column string }
	var entries []entry
	{
		rows, err := d.q().QueryContext(ctx, `SELECT model, column_name FROM ormpp_deprecated`)
		if err != nil {
			return err
		}
		for rows.Next() {
			var e entry
			if err := rows.Scan(&e.model, &e.column); err != nil {
				rows.Close()
				return err
			}
			entries = append(entries, e)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
	}
	for _, e := range entries {
		m := d.reg.byName[e.model]
		if m == nil {
			// Model existiert nicht mehr (z. B. ersetzt) — Eintrag erledigt.
			if _, err := d.q().ExecContext(ctx,
				`DELETE FROM ormpp_deprecated WHERE model = ? AND column_name = ?`, e.model, e.column); err != nil {
				return err
			}
			continue
		}
		if m.fieldByColumn(e.column) != nil {
			continue // Feld noch im Struct — Spalte bleibt bis zur nächsten Contract-Runde
		}
		cols, err := d.dial.tableColumns(d.q(), m.table)
		if err != nil {
			return err
		}
		for _, c := range cols {
			if c == e.column {
				stmt := fmt.Sprintf("ALTER TABLE %q DROP COLUMN %q", m.table, e.column)
				if _, err := d.q().ExecContext(ctx, stmt); err != nil {
					return fmt.Errorf("orm: deprecated-Spalte %s.%s entfernen: %w", m.table, e.column, err)
				}
				break
			}
		}
		if _, err := d.q().ExecContext(ctx,
			`DELETE FROM ormpp_deprecated WHERE model = ? AND column_name = ?`, e.model, e.column); err != nil {
			return err
		}
	}
	return nil
}
