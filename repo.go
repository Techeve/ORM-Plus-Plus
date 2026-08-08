package orm

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
)

// Repository ist der typisierte Zugriff auf ein CRUD-Model.
type Repository[T any] interface {
	Insert(ctx context.Context, entity *T) error
	InsertMany(ctx context.Context, entities []*T, opts ...BatchOption) error
	Get(ctx context.Context, id ID) (*T, error)
	GetForUpdate(ctx context.Context, id ID) (*T, error)
	Update(ctx context.Context, entity *T) error
	Upsert(ctx context.Context, entity *T) error
	Delete(ctx context.Context, id ID) error
	SetGeo(ctx context.Context, id ID, home string, opts ...GeoOption) error
	Query(ctx context.Context) QueryBuilder[T]
}

// Repo liefert das Repository für T auf einer DB oder in einer Transaktion.
func Repo[T any](h Handle) Repository[T] {
	m := h.db().reg.models[reflect.TypeFor[T]()]
	return &repo[T]{h: h, m: m}
}

type repo[T any] struct {
	h Handle
	m *model
}

// scope löst Tenant und Geo aus dem Context auf (fail-closed).
func (r *repo[T]) scope(ctx context.Context) (tenant ID, geo string, err error) {
	tenant, err = r.tenantScope(ctx)
	if err != nil {
		return ID{}, "", err
	}
	geo, err = r.h.db().dataGeo(ctx)
	if err != nil {
		return ID{}, "", err
	}
	return tenant, geo, nil
}

// tenantScope ist der schlanke Scope für Pfade ohne Geo-Bedarf
// (Get/Update/Delete/Queries — Reads filtern nie auf Geo).
func (r *repo[T]) tenantScope(ctx context.Context) (ID, error) {
	if r.m == nil {
		return ID{}, fmt.Errorf("orm: Model %T ist nicht registriert", *new(T))
	}
	if r.m.kind == kindEventSourced {
		return ID{}, fmt.Errorf("orm: %s ist event-sourced — orm.New/orm.Load/Append statt CRUD-Repository verwenden", r.m.name)
	}
	if !r.m.tenanted() {
		return ID{}, nil
	}
	t, ok := tenantFrom(ctx)
	if !ok {
		return ID{}, ErrNoTenant
	}
	return t, nil
}

// prepareWrite validiert Constraints und erzeugt die Insert-Werte —
// in exakt der Spaltenreihenfolge des gecachten Statements (buildSQL).
func (r *repo[T]) prepareWrite(ctx context.Context, e *T, tenant ID, geo string) ([]any, error) {
	d := r.h.db()
	rv := reflect.ValueOf(e).Elem()
	now := nowValue()

	vals := make([]any, 0, len(r.m.fields)+3)
	for _, f := range r.m.fields {
		fv := rv.FieldByIndex(f.index)

		if f.pk && fv.Interface().(ID).IsZero() {
			fv.Set(reflect.ValueOf(NewID()))
		}
		if f.autoCreate && fv.Interface().(timeValue).IsZero() {
			fv.Set(now)
		}
		if f.autoUpdate {
			fv.Set(now)
		}
		if f.hasDefault && fv.IsZero() && !f.nullable {
			fv.SetString(f.defaultVal)
		}
		if f.required && fv.IsZero() {
			return nil, fmt.Errorf("%w: %s.%s", ErrRequiredField, r.m.name, f.name)
		}
		if err := checkEnum(r.m, f, fv); err != nil {
			return nil, err
		}
		if err := r.checkRef(ctx, f, fv, tenant); err != nil {
			return nil, err
		}

		v, err := encodeField(d, f, fv)
		if err != nil {
			return nil, err
		}
		vals = append(vals, v)
	}
	for _, f := range r.m.lookups {
		v, err := encodeLookup(d, f, rv.FieldByIndex(f.index))
		if err != nil {
			return nil, err
		}
		vals = append(vals, v)
	}
	if r.m.tenanted() {
		if err := d.tenants.verify(tenant); err != nil {
			return nil, err
		}
		vals = append(vals, tenant.String())
	}
	vals = append(vals, geo)
	if r.m.opts.geoMode == geoFlexible {
		g, _ := geoFrom(ctx)
		reps, err := d.replicasJSON(g)
		if err != nil {
			return nil, err
		}
		vals = append(vals, reps)
	}
	return vals, nil
}

// checkRef verifiziert eine Referenz engine-seitig (zusätzlich zum FK):
// Ziel muss existieren und zum selben Tenant gehören.
func (r *repo[T]) checkRef(ctx context.Context, f *field, fv reflect.Value, tenant ID) error {
	if f.ref == nil {
		return nil
	}
	v := fv
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return nil
		}
		v = v.Elem()
	}
	target := v.Interface().(ID)
	if target.IsZero() {
		if f.required {
			return fmt.Errorf("%w: %s.%s", ErrRequiredField, r.m.name, f.name)
		}
		return nil
	}
	args := []any{target.String()}
	if f.ref.tenanted() {
		args = append(args, tenant.String())
	}
	var one int
	if err := r.h.q().QueryRowContext(ctx, f.refSQL, args...).Scan(&one); err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("%w: %s.%s → %s(%s)", ErrInvalidReference, r.m.name, f.name, f.refModel, target)
		}
		return err
	}
	return nil
}

func (r *repo[T]) Insert(ctx context.Context, e *T) error {
	tenant, geo, err := r.scope(ctx)
	if err != nil {
		return err
	}
	vals, err := r.prepareWrite(ctx, e, tenant, geo)
	if err != nil {
		return err
	}
	if err := r.checkUniques(ctx, e, tenant); err != nil {
		return err
	}
	_, err = r.h.q().ExecContext(ctx, r.m.sqlc.insert, vals...)
	return err
}

// checkUniques prüft Unique-Constraints engine-seitig über ALLE Regionen.
// Nötig auf Geo-Modellen: der physische Unique-Index partitionierter
// Tabellen enthält den Partitionsschlüssel und gilt damit nur pro Geo.
// Läuft topologie-, nicht backend-abhängig — SQLite verhält sich identisch
// (dort fängt zusätzlich der globale Index das Wettlauf-Restrisiko).
func (r *repo[T]) checkUniques(ctx context.Context, e *T, tenant ID) error {
	d := r.h.db()
	if !d.geoEngine(r.m) {
		return nil
	}
	rv := reflect.ValueOf(e).Elem()
	self := rv.FieldByIndex(r.m.pk.index).Interface().(ID)

	var sets [][]*field
	for _, f := range r.m.fields {
		if f.unique {
			sets = append(sets, []*field{f})
		}
	}
	for _, names := range r.m.opts.uniques {
		set := make([]*field, len(names))
		for i, n := range names {
			set[i] = r.m.fieldByName(n)
		}
		sets = append(sets, set)
	}

check:
	for _, set := range sets {
		conds := make([]string, 0, len(set)+2)
		args := make([]any, 0, len(set)+2)
		for _, f := range set {
			// Lookup-Felder vergleichen über die deterministische
			// Index-Spalte — der Ciphertext selbst ist nie gleich.
			col, v := f.column, any(nil)
			var err error
			if f.lookup {
				col = f.lookupColumn()
				v, err = encodeLookup(d, f, rv.FieldByIndex(f.index))
			} else {
				v, err = encodeField(d, f, rv.FieldByIndex(f.index))
			}
			if err != nil {
				return err
			}
			if v == nil {
				continue check // NULL kollidiert per SQL-Semantik nie
			}
			conds = append(conds, fmt.Sprintf("%q = ?", col))
			args = append(args, v)
		}
		if r.m.tenanted() {
			conds = append(conds, `"tenant_id" = ?`)
			args = append(args, tenant.String())
		}
		conds = append(conds, fmt.Sprintf("%q <> ?", r.m.pk.column))
		args = append(args, self.String())

		var one int
		err := r.h.q().QueryRowContext(ctx, fmt.Sprintf(
			"SELECT 1 FROM %q WHERE %s", r.m.table, strings.Join(conds, " AND ")), args...).Scan(&one)
		switch err {
		case sql.ErrNoRows:
		case nil:
			names := make([]string, len(set))
			for i, f := range set {
				names[i] = f.name
			}
			return fmt.Errorf("%w: %s(%s)", ErrUniqueConflict, r.m.name, strings.Join(names, ", "))
		default:
			return err
		}
	}
	return nil
}

func (r *repo[T]) InsertMany(ctx context.Context, entities []*T, opts ...BatchOption) error {
	bo := batchOptions{}
	for _, o := range opts {
		o(&bo)
	}
	if bo.chunk <= 0 {
		bo.chunk = len(entities)
	}
	tenant, geo, err := r.scope(ctx)
	if err != nil {
		return err
	}
	d := r.h.db()
	for start := 0; start < len(entities); start += bo.chunk {
		end := min(start+bo.chunk, len(entities))
		chunk := entities[start:end]
		err := d.Tx(ctx, func(tx Tx) error {
			// Das Insert-Statement EINMAL pro Chunk-Transaktion präparieren
			// und wiederverwenden — pgx cached ohnehin, SQLite spart das
			// Parse/Prepare pro Zeile. Alle Integritätsprüfungen (required,
			// enum, ref, Tenant) laufen unverändert pro Entität.
			th := tx.(*txHandle)
			stmt, err := th.tx.PrepareContext(ctx, d.dial.rebind(r.m.sqlc.insert))
			if err != nil {
				return err
			}
			defer func() { _ = stmt.Close() }()
			cr := &repo[T]{h: tx, m: r.m}
			for _, e := range chunk {
				vals, err := cr.prepareWrite(ctx, e, tenant, geo)
				if err != nil {
					return err
				}
				if err := cr.checkUniques(ctx, e, tenant); err != nil {
					return err
				}
				if _, err := stmt.ExecContext(ctx, vals...); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("orm: InsertMany nach %d Zeilen: %w", start, err)
		}
	}
	return nil
}

func (r *repo[T]) get(ctx context.Context, id ID, lock bool) (*T, error) {
	tenant, err := r.tenantScope(ctx)
	if err != nil {
		return nil, err
	}
	query := r.m.sqlc.getByPK
	args := []any{id.String()}
	if r.m.tenanted() {
		args = append(args, tenant.String())
	}
	if lock {
		query += r.h.db().dial.forUpdate()
	}
	rows, err := r.h.q().QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	list, err := scanModelRows[T](r.h, r.m, rows)
	if err != nil {
		return nil, err
	}
	if len(list) == 0 {
		return nil, ErrNotFound
	}
	return list[0], nil
}

func (r *repo[T]) Get(ctx context.Context, id ID) (*T, error) { return r.get(ctx, id, false) }

func (r *repo[T]) GetForUpdate(ctx context.Context, id ID) (*T, error) {
	if !r.h.inTx() {
		return nil, ErrRequiresTx
	}
	// PG/YB: SELECT … FOR UPDATE; SQLite emuliert über die serialisierte
	// Schreib-Connection (txlock=immediate) — verhaltensgleich.
	return r.get(ctx, id, true)
}

func (r *repo[T]) Update(ctx context.Context, e *T) error {
	tenant, err := r.tenantScope(ctx)
	if err != nil {
		return err
	}
	rv := reflect.ValueOf(e).Elem()
	pk := rv.FieldByIndex(r.m.pk.index).Interface().(ID)
	if pk.IsZero() {
		return ErrNotFound
	}

	vals := make([]any, 0, len(r.m.updateFields)+3)
	var oldVersion int64
	for _, f := range r.m.updateFields {
		fv := rv.FieldByIndex(f.index)
		if f.autoUpdate {
			fv.Set(nowValue())
		}
		if f.version {
			oldVersion = fv.Int()
			fv.SetInt(oldVersion + 1)
		}
		if err := checkEnum(r.m, f, fv); err != nil {
			return err
		}
		if err := r.checkRef(ctx, f, fv, tenant); err != nil {
			return err
		}
		v, err := encodeField(r.h.db(), f, fv)
		if err != nil {
			return err
		}
		vals = append(vals, v)
	}
	for _, f := range r.m.updateLookups {
		v, err := encodeLookup(r.h.db(), f, rv.FieldByIndex(f.index))
		if err != nil {
			return err
		}
		vals = append(vals, v)
	}

	vals = append(vals, pk.String())
	if r.m.tenanted() {
		vals = append(vals, tenant.String())
	}
	if r.m.version != nil {
		vals = append(vals, oldVersion)
	}

	if err := r.checkUniques(ctx, e, tenant); err != nil {
		return err
	}
	res, err := r.h.q().ExecContext(ctx, r.m.sqlc.update, vals...)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		// Existenz entscheidet: Versionskonflikt oder nicht gefunden.
		if r.m.version != nil {
			if _, gerr := r.get(ctx, pk, false); gerr == nil {
				rv.FieldByIndex(r.m.version.index).SetInt(oldVersion) // zurückrollen
				return ErrVersionConflict
			}
		}
		return ErrNotFound
	}
	return nil
}

func (r *repo[T]) Upsert(ctx context.Context, e *T) error {
	tenant, geo, err := r.scope(ctx)
	if err != nil {
		return err
	}
	vals, err := r.prepareWrite(ctx, e, tenant, geo)
	if err != nil {
		return err
	}
	if err := r.checkUniques(ctx, e, tenant); err != nil {
		return err
	}
	d := r.h.db()
	if !d.geoEngine(r.m) || r.m.sqlc.upsertUpdate == "" {
		_, err = r.h.q().ExecContext(ctx, r.m.sqlc.upsert, vals...)
		return err
	}
	// Geo-Modelle: erst UPDATE nach pk (findet den Datensatz in JEDER
	// Region — ein ON CONFLICT träfe nur das Context-Geo und legte einen
	// umgezogenen Datensatz doppelt an), bei 0 Zeilen INSERT.
	upsert := func(h Handle) error {
		rv := reflect.ValueOf(e).Elem()
		uvals := make([]any, 0, len(r.m.updateFields)+2)
		for _, f := range r.m.updateFields {
			v, err := encodeField(d, f, rv.FieldByIndex(f.index))
			if err != nil {
				return err
			}
			uvals = append(uvals, v)
		}
		for _, f := range r.m.updateLookups {
			v, err := encodeLookup(d, f, rv.FieldByIndex(f.index))
			if err != nil {
				return err
			}
			uvals = append(uvals, v)
		}
		uvals = append(uvals, rv.FieldByIndex(r.m.pk.index).Interface().(ID).String())
		if r.m.tenanted() {
			uvals = append(uvals, tenant.String())
		}
		res, err := h.q().ExecContext(ctx, r.m.sqlc.upsertUpdate, uvals...)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n > 0 {
			return nil
		}
		_, err = h.q().ExecContext(ctx, r.m.sqlc.insert, vals...)
		return err
	}
	if r.h.inTx() {
		return upsert(r.h)
	}
	return d.Tx(ctx, func(tx Tx) error { return upsert(tx) })
}

func (r *repo[T]) Delete(ctx context.Context, id ID) error {
	tenant, err := r.tenantScope(ctx)
	if err != nil {
		return err
	}
	// restrict-Vorprüfung für sprechende Fehler (der FK würde es auch verhindern).
	for _, by := range r.m.referencedBy {
		for _, f := range by.fields {
			if f.ref == r.m && f.refOn == odRestrict {
				var one int
				err := r.h.q().QueryRowContext(ctx, f.restrictSQL, id.String()).Scan(&one)
				if err == nil {
					return fmt.Errorf("%w: %s.%s", ErrReferenceInUse, by.name, f.name)
				}
				if err != sql.ErrNoRows {
					return err
				}
			}
		}
	}
	args := []any{id.String()}
	if r.m.tenanted() {
		args = append(args, tenant.String())
	}
	d := r.h.db()
	// Auf Geo-Modellen ist kein FK auf diese Tabelle darstellbar
	// (partitioniertes Ziel) — setnull/cascade übernimmt die Engine.
	// Wo ein nativer FK existiert (SQLite, kollabierte Backends), hat er
	// die Kinder schon behandelt und die Emulation findet nichts mehr vor.
	emulate := false
	if d.geoEngine(r.m) {
		for _, by := range r.m.referencedBy {
			for _, f := range by.fields {
				if f.ref == r.m && f.refOn != odRestrict {
					emulate = true
				}
			}
		}
	}
	del := func(h Handle) error {
		res, err := h.q().ExecContext(ctx, r.m.sqlc.deleteByPK, args...)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n == 0 {
			return ErrNotFound
		}
		if emulate {
			return d.emulateOnDelete(ctx, h, r.m, []string{id.String()})
		}
		return nil
	}
	if emulate && !r.h.inTx() {
		return d.Tx(ctx, func(tx Tx) error { return del(tx) })
	}
	return del(r.h)
}

// emulateOnDelete zieht die ondelete-Semantik der Referenzen nach, die auf
// partitionierten Zieltabellen kein natives Pendant haben: setnull leert
// die Referenzspalte, cascade löscht rekursiv — mit restrict-Schutz auf
// jeder Ebene, wie es der native FK täte.
func (d *DB) emulateOnDelete(ctx context.Context, h Handle, m *model, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	ph := strings.TrimSuffix(strings.Repeat("?, ", len(ids)), ", ")
	idArgs := make([]any, len(ids))
	for i, id := range ids {
		idArgs[i] = id
	}
	for _, by := range m.referencedBy {
		for _, f := range by.fields {
			if f.ref != m {
				continue
			}
			switch f.refOn {
			case odSetNull:
				if _, err := h.q().ExecContext(ctx, fmt.Sprintf(
					"UPDATE %q SET %q = NULL WHERE %q IN (%s)", by.table, f.column, f.column, ph), idArgs...); err != nil {
					return err
				}
			case odCascade:
				rows, err := h.q().QueryContext(ctx, fmt.Sprintf(
					"SELECT %q FROM %q WHERE %q IN (%s)", by.pkColumn(), by.table, f.column, ph), idArgs...)
				if err != nil {
					return err
				}
				var children []string
				for rows.Next() {
					var c string
					if err := rows.Scan(&c); err != nil {
						rows.Close()
						return err
					}
					children = append(children, c)
				}
				rows.Close()
				if err := rows.Err(); err != nil {
					return err
				}
				if len(children) == 0 {
					continue
				}
				// restrict eine Ebene tiefer blockiert die Kaskade — wie nativ.
				for _, gby := range by.referencedBy {
					for _, gf := range gby.fields {
						if gf.ref == by && gf.refOn == odRestrict {
							for _, c := range children {
								var one int
								err := h.q().QueryRowContext(ctx, gf.restrictSQL, c).Scan(&one)
								if err == nil {
									return fmt.Errorf("%w: %s.%s", ErrReferenceInUse, gby.name, gf.name)
								}
								if err != sql.ErrNoRows {
									return err
								}
							}
						}
					}
				}
				if err := d.emulateOnDelete(ctx, h, by, children); err != nil {
					return err
				}
				if _, err := h.q().ExecContext(ctx, fmt.Sprintf(
					"DELETE FROM %q WHERE %q IN (%s)", by.table, f.column, ph), idArgs...); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// SetGeo verlegt einen einzelnen Datensatz in eine andere Region
// (engine-geführt, tenant-gescoped):
//
//   - GeoScoped: die Heimatregion, ohne Replikate — Replikat-Optionen
//     lehnt der Aufruf ab, dafür ist GeoFlexible da.
//   - GeoFlexible: Heimatregion und Replikat-Liste.
//   - EventSourced: das ganze Aggregat — Event-Log, Archiv und
//     Read-Model ziehen gemeinsam um, damit das Geo-Pinning stimmig
//     bleibt.
//
// Auf partitionierten Backends wandern die Zeilen dabei physisch in die
// Partition der Zielregion; auf kollabierten Backends bleibt es beim
// Spaltenwert. GeoGlobal-Modelle sind per Definition überall — für sie
// gibt es nichts zu verlegen.
func (r *repo[T]) SetGeo(ctx context.Context, id ID, home string, opts ...GeoOption) error {
	d := r.h.db()
	if r.m == nil {
		return fmt.Errorf("orm: Model %T ist nicht registriert", *new(T))
	}
	if r.m.opts.geoMode == geoGlobal {
		return fmt.Errorf("orm: %s ist GeoGlobal — in allen Regionen vorhanden, SetGeo greift nicht", r.m.name)
	}
	if r.m.opts.geoMode != geoFlexible && len(opts) > 0 {
		return fmt.Errorf("orm: %s ist nicht GeoFlexible — Replikate brauchen orm.GeoFlexible()", r.m.name)
	}
	var tenant ID
	if r.m.tenanted() {
		t, ok := tenantFrom(ctx)
		if !ok {
			return ErrNoTenant
		}
		tenant = t
	}
	if !d.validGeo(home) {
		return fmt.Errorf("%w: %q", ErrRegionNotActive, home)
	}

	sets := []string{`"geo" = ?`}
	args := []any{home}
	if r.m.opts.geoMode == geoFlexible {
		g := geoScope{home: home}
		for _, o := range opts {
			o(&g)
		}
		reps, err := d.replicasJSON(g)
		if err != nil {
			return err
		}
		sets = append(sets, `"geo_replicas" = ?`)
		args = append(args, reps)
	}

	f := geoFilter{cond: fmt.Sprintf("%q = ?", r.m.pkColumn()), args: []any{id.String()}}
	if r.m.tenanted() {
		f.cond += ` AND "tenant_id" = ?`
		f.args = append(f.args, tenant.String())
	}
	// Beim Aggregat zuerst der Event-Log: er trägt die Heimatregion, an der
	// das Geo-Pinning hängt. Der Filter greift dort auf aggregate_id.
	// Ob das Aggregat existiert, entscheidet der Log — nicht das
	// Read-Model, das der Projektion hinterherhinken darf.
	if r.m.kind == kindEventSourced {
		ef := geoFilter{cond: `"aggregate_id" = ?`, args: []any{id.String()}}
		if r.m.tenanted() {
			ef.cond += ` AND "tenant_id" = ?`
			ef.args = append(ef.args, tenant.String())
		}
		var events int64
		if err := r.h.q().QueryRowContext(ctx, fmt.Sprintf(
			`SELECT (SELECT COUNT(*) FROM %q WHERE %s) + (SELECT COUNT(*) FROM %q WHERE %s)`,
			esEventsTable(r.m), ef.cond, esArchiveTable(r.m), ef.cond),
			append(append([]any{}, ef.args...), ef.args...)...).Scan(&events); err != nil {
			return err
		}
		if events == 0 {
			return ErrNotFound
		}
		if err := d.moveEvents(ctx, r.h, esEventsTable(r.m), ef, home); err != nil {
			return err
		}
		for _, t := range []string{esArchiveTable(r.m), esSnapsTable(r.m)} {
			if err := moveGeoColumn(ctx, r.h, t, ef, home); err != nil {
				return err
			}
		}
	}

	res, err := r.h.q().ExecContext(ctx, fmt.Sprintf("UPDATE %q SET %s WHERE %s",
		r.m.table, strings.Join(sets, ", "), f.cond), append(args, f.args...)...)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 && r.m.kind != kindEventSourced {
		return ErrNotFound
	}
	return nil
}

// replicasJSON validiert und serialisiert die Replikat-Liste eines
// GeoFlexible-Datensatzes ("*" = ReplicateAll).
func (d *DB) replicasJSON(g geoScope) (string, error) {
	if g.replicateAll {
		return `["*"]`, nil
	}
	for _, r := range g.replicateTo {
		if !d.validGeo(r) {
			return "", fmt.Errorf("%w: %q", ErrRegionNotActive, r)
		}
	}
	b, err := json.Marshal(append([]string{}, g.replicateTo...))
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func (r *repo[T]) Query(ctx context.Context) QueryBuilder[T] {
	return &queryBuilder[T]{r: r, ctx: ctx}
}

// --- Hilfen ---

type timeValue interface{ IsZero() bool }

func nowValue() reflect.Value {
	return reflect.ValueOf(nowUTC())
}

func checkEnum(m *model, f *field, fv reflect.Value) error {
	if len(f.enum) == 0 {
		return nil
	}
	v := fv
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return nil
		}
		v = v.Elem()
	}
	s := v.String()
	if s == "" && !f.required {
		return nil
	}
	for _, allowed := range f.enum {
		if s == allowed {
			return nil
		}
	}
	return fmt.Errorf("%w: %s.%s = %q (erlaubt: %s)", ErrInvalidValue, m.name, f.name, s, strings.Join(f.enum, "|"))
}

func insertSQL(table string, cols []string) string {
	ph := make([]string, len(cols))
	for i := range ph {
		ph[i] = "?"
	}
	return fmt.Sprintf("INSERT INTO %q (%s) VALUES (%s)", table, quoteAll(cols...), strings.Join(ph, ", "))
}

func selectList(m *model) string {
	if m.sqlc.selectList != "" {
		return m.sqlc.selectList
	}
	// Fallback vor resolve (interne Pfade, Scratch-Pläne).
	cols := make([]string, len(m.fields))
	for i, f := range m.fields {
		cols[i] = f.column
	}
	if m.kind == kindEventSourced {
		cols = append(cols, "id", "aggregate_seq")
	}
	return quoteAll(cols...)
}
