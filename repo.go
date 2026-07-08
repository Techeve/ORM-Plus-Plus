package orm

import (
	"context"
	"database/sql"
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
	d := r.h.db()
	if r.m == nil {
		return ID{}, "", fmt.Errorf("orm: Model %T ist nicht registriert", *new(T))
	}
	if r.m.kind == kindEventSourced {
		return ID{}, "", fmt.Errorf("orm: %s ist event-sourced — orm.New/orm.Load/Append statt CRUD-Repository verwenden", r.m.name)
	}
	if r.m.tenanted() {
		t, ok := tenantFrom(ctx)
		if !ok {
			return ID{}, "", ErrNoTenant
		}
		tenant = t
	}
	geo, err = d.dataGeo(ctx)
	if err != nil {
		return ID{}, "", err
	}
	return tenant, geo, nil
}

// prepareWrite validiert Constraints und erzeugt die Spalten/Werte eines Inserts.
func (r *repo[T]) prepareWrite(ctx context.Context, e *T, tenant ID, geo string) ([]string, []any, error) {
	d := r.h.db()
	rv := reflect.ValueOf(e).Elem()
	now := nowValue()

	var cols []string
	var vals []any
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
			return nil, nil, fmt.Errorf("%w: %s.%s", ErrRequiredField, r.m.name, f.name)
		}
		if err := checkEnum(r.m, f, fv); err != nil {
			return nil, nil, err
		}
		if err := r.checkRef(ctx, f, fv, tenant); err != nil {
			return nil, nil, err
		}

		v, err := encodeField(f, fv)
		if err != nil {
			return nil, nil, err
		}
		cols = append(cols, f.column)
		vals = append(vals, v)
	}
	if r.m.tenanted() {
		if err := d.tenants.verify(tenant); err != nil {
			return nil, nil, err
		}
		cols = append(cols, "tenant_id")
		vals = append(vals, tenant.String())
	}
	cols = append(cols, "geo")
	vals = append(vals, geo)
	return cols, vals, nil
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
	query := fmt.Sprintf("SELECT 1 FROM %q WHERE %q = ?", f.ref.table, f.ref.pkColumn())
	args := []any{target.String()}
	if f.ref.tenanted() {
		query += ` AND tenant_id = ?`
		args = append(args, tenant.String())
	}
	var one int
	if err := r.h.q().QueryRowContext(ctx, query, args...).Scan(&one); err != nil {
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
	cols, vals, err := r.prepareWrite(ctx, e, tenant, geo)
	if err != nil {
		return err
	}
	_, err = r.h.q().ExecContext(ctx, insertSQL(r.m.table, cols), vals...)
	return err
}

func (r *repo[T]) InsertMany(ctx context.Context, entities []*T, opts ...BatchOption) error {
	bo := batchOptions{}
	for _, o := range opts {
		o(&bo)
	}
	if bo.chunk <= 0 {
		bo.chunk = len(entities)
	}
	d := r.h.db()
	for start := 0; start < len(entities); start += bo.chunk {
		end := min(start+bo.chunk, len(entities))
		chunk := entities[start:end]
		err := d.Tx(ctx, func(tx Tx) error {
			cr := &repo[T]{h: tx, m: r.m}
			for _, e := range chunk {
				if err := cr.Insert(ctx, e); err != nil {
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

func (r *repo[T]) get(ctx context.Context, id ID) (*T, error) {
	tenant, _, err := r.scope(ctx)
	if err != nil {
		return nil, err
	}
	query := fmt.Sprintf("SELECT %s FROM %q WHERE %q = ?",
		selectList(r.m), r.m.table, r.m.pk.column)
	args := []any{id.String()}
	if r.m.tenanted() {
		query += ` AND tenant_id = ?`
		args = append(args, tenant.String())
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

func (r *repo[T]) Get(ctx context.Context, id ID) (*T, error) { return r.get(ctx, id) }

func (r *repo[T]) GetForUpdate(ctx context.Context, id ID) (*T, error) {
	if !r.h.inTx() {
		return nil, ErrRequiresTx
	}
	// SQLite: die Transaktion (txlock=immediate) hält bereits die Schreibsperre;
	// auf PG/YB ergänzt der Dialekt hier FOR UPDATE (Phase 4).
	return r.get(ctx, id)
}

func (r *repo[T]) Update(ctx context.Context, e *T) error {
	tenant, _, err := r.scope(ctx)
	if err != nil {
		return err
	}
	rv := reflect.ValueOf(e).Elem()
	pk := rv.FieldByIndex(r.m.pk.index).Interface().(ID)
	if pk.IsZero() {
		return ErrNotFound
	}

	var sets []string
	var vals []any
	var oldVersion int64
	for _, f := range r.m.fields {
		if f.pk || f.immutable {
			continue
		}
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
		v, err := encodeField(f, fv)
		if err != nil {
			return err
		}
		sets = append(sets, fmt.Sprintf("%q = ?", f.column))
		vals = append(vals, v)
	}

	query := fmt.Sprintf("UPDATE %q SET %s WHERE %q = ?", r.m.table, strings.Join(sets, ", "), r.m.pk.column)
	vals = append(vals, pk.String())
	if r.m.tenanted() {
		query += ` AND tenant_id = ?`
		vals = append(vals, tenant.String())
	}
	if r.m.version != nil {
		query += fmt.Sprintf(" AND %q = ?", r.m.version.column)
		vals = append(vals, oldVersion)
	}

	res, err := r.h.q().ExecContext(ctx, query, vals...)
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
			if _, gerr := r.get(ctx, pk); gerr == nil {
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
	cols, vals, err := r.prepareWrite(ctx, e, tenant, geo)
	if err != nil {
		return err
	}
	var updates []string
	for _, f := range r.m.fields {
		if f.pk || f.immutable {
			continue
		}
		updates = append(updates, fmt.Sprintf("%q = excluded.%q", f.column, f.column))
	}
	query := insertSQL(r.m.table, cols) + fmt.Sprintf(
		" ON CONFLICT (%q) DO UPDATE SET %s", r.m.pk.column, strings.Join(updates, ", "))
	_, err = r.h.q().ExecContext(ctx, query, vals...)
	return err
}

func (r *repo[T]) Delete(ctx context.Context, id ID) error {
	tenant, _, err := r.scope(ctx)
	if err != nil {
		return err
	}
	// restrict-Vorprüfung für sprechende Fehler (der FK würde es auch verhindern).
	for _, by := range r.m.referencedBy {
		for _, f := range by.fields {
			if f.ref == r.m && f.refOn == odRestrict {
				var one int
				q := fmt.Sprintf("SELECT 1 FROM %q WHERE %q = ? LIMIT 1", by.table, f.column)
				err := r.h.q().QueryRowContext(ctx, q, id.String()).Scan(&one)
				if err == nil {
					return fmt.Errorf("%w: %s.%s", ErrReferenceInUse, by.name, f.name)
				}
				if err != sql.ErrNoRows {
					return err
				}
			}
		}
	}
	query := fmt.Sprintf("DELETE FROM %q WHERE %q = ?", r.m.table, r.m.pk.column)
	args := []any{id.String()}
	if r.m.tenanted() {
		query += ` AND tenant_id = ?`
		args = append(args, tenant.String())
	}
	res, err := r.h.q().ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *repo[T]) SetGeo(ctx context.Context, id ID, home string, opts ...GeoOption) error {
	return fmt.Errorf("orm: SetGeo ist noch nicht implementiert (GeoFlexible-Mechanik, siehe doc/TASK.md)")
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
	cols := make([]string, len(m.fields))
	for i, f := range m.fields {
		cols[i] = f.column
	}
	if m.kind == kindEventSourced {
		cols = append(cols, "id", "aggregate_seq")
	}
	return quoteAll(cols...)
}
