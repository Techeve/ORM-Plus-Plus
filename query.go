package orm

import (
	"context"
	"fmt"
	"iter"
	"reflect"
	"strings"
	"time"
)

func nowUTC() time.Time { return time.Now().UTC() }

// Dir ist die Sortierrichtung.
type Dir int

const (
	Asc Dir = iota
	Desc
)

// Cond ist eine Filterbedingung des Query-Builders.
type Cond struct {
	op       string // "cmp", "like", "in", "null", "notnull", "and", "or", "not"
	field    string
	cmp      string
	value    any
	values   []any
	children []Cond
}

// Eq: Feld = Wert.
func Eq(field string, v any) Cond { return Cond{op: "cmp", field: field, cmp: "=", value: v} }

// Ne: Feld <> Wert.
func Ne(field string, v any) Cond { return Cond{op: "cmp", field: field, cmp: "<>", value: v} }

// Gt / Gte / Lt / Lte: Vergleiche.
func Gt(field string, v any) Cond  { return Cond{op: "cmp", field: field, cmp: ">", value: v} }
func Gte(field string, v any) Cond { return Cond{op: "cmp", field: field, cmp: ">=", value: v} }
func Lt(field string, v any) Cond  { return Cond{op: "cmp", field: field, cmp: "<", value: v} }
func Lte(field string, v any) Cond { return Cond{op: "cmp", field: field, cmp: "<=", value: v} }

// Like: Muster-Vergleich (SQL LIKE).
func Like(field, pattern string) Cond {
	return Cond{op: "like", field: field, value: pattern}
}

// In: Feld ∈ Wertemenge.
func In(field string, vs ...any) Cond { return Cond{op: "in", field: field, values: vs} }

// IsNull / NotNull.
func IsNull(field string) Cond  { return Cond{op: "null", field: field} }
func NotNull(field string) Cond { return Cond{op: "notnull", field: field} }

// And / Or / Not verknüpfen Bedingungen.
func And(conds ...Cond) Cond { return Cond{op: "and", children: conds} }
func Or(conds ...Cond) Cond  { return Cond{op: "or", children: conds} }
func Not(c Cond) Cond        { return Cond{op: "not", children: []Cond{c}} }

// Assignment ist eine Zuweisung für mengenbasierte Updates.
type Assignment struct {
	field string
	value any
}

// Set erzeugt eine Zuweisung Feld = Wert für UpdateSet (doc/API.md: orm.Set).
func Set(field string, v any) Assignment { return Assignment{field: field, value: v} }

// QueryBuilder baut typisierte Abfragen gegen ein Model.
type QueryBuilder[T any] interface {
	Where(c Cond) QueryBuilder[T]
	OrderBy(field string, dir Dir) QueryBuilder[T]
	Limit(n int) QueryBuilder[T]
	Offset(n int) QueryBuilder[T]
	All() ([]*T, error)
	Iter() iter.Seq2[*T, error]
	First() (*T, error)
	Count() (int64, error)
	Exists() (bool, error)
	UpdateSet(sets ...Assignment) (int64, error)
	Delete() (int64, error)
}

// Query baut eine typisierte Abfrage — für CRUD-Modelle wie für die
// Read-Models von ES-Modellen (dort inkl. Aggregat-Verdrahtung im Ergebnis).
func Query[T any](h Handle, ctx context.Context) QueryBuilder[T] {
	return &queryBuilder[T]{
		r:   &repo[T]{h: h, m: h.db().reg.models[reflect.TypeFor[T]()]},
		ctx: ctx,
	}
}

type orderTerm struct {
	field string
	dir   Dir
}

type queryBuilder[T any] struct {
	r      *repo[T]
	ctx    context.Context
	conds  []Cond
	orders []orderTerm
	limit  int
	offset int
}

func (q *queryBuilder[T]) Where(c Cond) QueryBuilder[T] {
	q.conds = append(q.conds, c)
	return q
}

func (q *queryBuilder[T]) OrderBy(field string, dir Dir) QueryBuilder[T] {
	q.orders = append(q.orders, orderTerm{field: field, dir: dir})
	return q
}

func (q *queryBuilder[T]) Limit(n int) QueryBuilder[T]  { q.limit = n; return q }
func (q *queryBuilder[T]) Offset(n int) QueryBuilder[T] { q.offset = n; return q }

// build erzeugt WHERE/ORDER/LIMIT inklusive Tenant-Scope.
func (q *queryBuilder[T]) build() (where string, args []any, tail string, err error) {
	if q.r.m == nil {
		return "", nil, "", fmt.Errorf("orm: Model %T ist nicht registriert", *new(T))
	}
	var parts []string
	if q.r.m.tenanted() {
		tenant, ok := tenantFrom(q.ctx)
		if !ok {
			return "", nil, "", ErrNoTenant
		}
		parts = append(parts, `tenant_id = ?`)
		args = append(args, tenant.String())
	}
	for _, c := range q.conds {
		sqlPart, condArgs, err := q.condSQL(c)
		if err != nil {
			return "", nil, "", err
		}
		parts = append(parts, sqlPart)
		args = append(args, condArgs...)
	}
	if len(parts) > 0 {
		where = " WHERE " + strings.Join(parts, " AND ")
	}

	var t strings.Builder
	if len(q.orders) > 0 {
		var terms []string
		for _, o := range q.orders {
			col, err := q.column(o.field)
			if err != nil {
				return "", nil, "", err
			}
			dir := "ASC"
			if o.dir == Desc {
				dir = "DESC"
			}
			terms = append(terms, fmt.Sprintf("%q %s", col, dir))
		}
		fmt.Fprintf(&t, " ORDER BY %s", strings.Join(terms, ", "))
	}
	if q.limit > 0 {
		fmt.Fprintf(&t, " LIMIT %d", q.limit)
	}
	if q.offset > 0 {
		if q.limit <= 0 {
			fmt.Fprintf(&t, " LIMIT %s", q.r.h.db().dial.limitAll())
		}
		fmt.Fprintf(&t, " OFFSET %d", q.offset)
	}
	return where, args, t.String(), nil
}

func (q *queryBuilder[T]) column(fieldName string) (string, error) {
	f := q.r.m.fieldByName(fieldName)
	if f == nil {
		return "", fmt.Errorf("orm: %s: unbekanntes Feld %q in Query", q.r.m.name, fieldName)
	}
	return f.column, nil
}

func (q *queryBuilder[T]) condSQL(c Cond) (string, []any, error) {
	switch c.op {
	case "cmp", "like", "in", "null", "notnull":
		col, err := q.column(c.field)
		if err != nil {
			return "", nil, err
		}
		f := q.r.m.fieldByName(c.field)
		switch c.op {
		case "cmp":
			v, err := encodeQueryValue(f, c.value)
			if err != nil {
				return "", nil, err
			}
			return fmt.Sprintf("%q %s ?", col, c.cmp), []any{v}, nil
		case "like":
			return fmt.Sprintf("%q LIKE ?", col), []any{c.value}, nil
		case "in":
			if len(c.values) == 0 {
				return "1 = 0", nil, nil
			}
			ph := make([]string, len(c.values))
			args := make([]any, len(c.values))
			for i, raw := range c.values {
				v, err := encodeQueryValue(f, raw)
				if err != nil {
					return "", nil, err
				}
				ph[i], args[i] = "?", v
			}
			return fmt.Sprintf("%q IN (%s)", col, strings.Join(ph, ", ")), args, nil
		case "null":
			return fmt.Sprintf("%q IS NULL", col), nil, nil
		default:
			return fmt.Sprintf("%q IS NOT NULL", col), nil, nil
		}
	case "and", "or":
		if len(c.children) == 0 {
			return "1 = 1", nil, nil
		}
		join := " AND "
		if c.op == "or" {
			join = " OR "
		}
		var parts []string
		var args []any
		for _, ch := range c.children {
			p, a, err := q.condSQL(ch)
			if err != nil {
				return "", nil, err
			}
			parts = append(parts, p)
			args = append(args, a...)
		}
		return "(" + strings.Join(parts, join) + ")", args, nil
	case "not":
		p, a, err := q.condSQL(c.children[0])
		if err != nil {
			return "", nil, err
		}
		return "NOT (" + p + ")", a, nil
	default:
		return "", nil, fmt.Errorf("orm: unbekannter Bedingungstyp %q", c.op)
	}
}

// encodeQueryValue wandelt Query-Parameter passend zur Spalte (ID→TEXT usw.).
func encodeQueryValue(f *field, v any) (any, error) {
	switch val := v.(type) {
	case ID:
		return val.String(), nil
	case time.Time:
		return val.UTC().Format(time.RFC3339Nano), nil
	case bool:
		if val {
			return int64(1), nil
		}
		return int64(0), nil
	default:
		return v, nil
	}
}

func (q *queryBuilder[T]) All() ([]*T, error) {
	where, args, tail, err := q.build()
	if err != nil {
		return nil, err
	}
	query := fmt.Sprintf("SELECT %s FROM %q", selectList(q.r.m), q.r.m.table) + where + tail
	rows, err := q.r.h.q().QueryContext(q.ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return scanModelRows[T](q.r.h, q.r.m, rows)
}

func (q *queryBuilder[T]) Iter() iter.Seq2[*T, error] {
	return func(yield func(*T, error) bool) {
		where, args, tail, err := q.build()
		if err != nil {
			yield(nil, err)
			return
		}
		query := fmt.Sprintf("SELECT %s FROM %q", selectList(q.r.m), q.r.m.table) + where + tail
		rows, err := q.r.h.q().QueryContext(q.ctx, query, args...)
		if err != nil {
			yield(nil, err)
			return
		}
		defer rows.Close()
		for rows.Next() {
			e, err := scanModelRow[T](q.r.h, q.r.m, rows)
			if !yield(e, err) || err != nil {
				return
			}
		}
		if err := rows.Err(); err != nil {
			yield(nil, err)
		}
	}
}

func (q *queryBuilder[T]) First() (*T, error) {
	q.limit = 1
	list, err := q.All()
	if err != nil {
		return nil, err
	}
	if len(list) == 0 {
		return nil, ErrNotFound
	}
	return list[0], nil
}

func (q *queryBuilder[T]) Count() (int64, error) {
	where, args, _, err := q.build()
	if err != nil {
		return 0, err
	}
	var n int64
	query := fmt.Sprintf("SELECT COUNT(*) FROM %q", q.r.m.table) + where
	err = q.r.h.q().QueryRowContext(q.ctx, query, args...).Scan(&n)
	return n, err
}

func (q *queryBuilder[T]) Exists() (bool, error) {
	n, err := q.Count()
	return n > 0, err
}

func (q *queryBuilder[T]) UpdateSet(sets ...Assignment) (int64, error) {
	if len(sets) == 0 {
		return 0, fmt.Errorf("orm: UpdateSet ohne Zuweisungen")
	}
	where, args, _, err := q.build()
	if err != nil {
		return 0, err
	}
	var assigns []string
	var vals []any
	for _, s := range sets {
		f := q.r.m.fieldByName(s.field)
		if f == nil {
			return 0, fmt.Errorf("orm: %s: unbekanntes Feld %q in UpdateSet", q.r.m.name, s.field)
		}
		if f.pk || f.immutable {
			return 0, fmt.Errorf("orm: %s.%s ist unveränderlich", q.r.m.name, s.field)
		}
		if len(f.enum) > 0 {
			if sv, ok := s.value.(string); ok {
				ok := false
				for _, allowed := range f.enum {
					if sv == allowed {
						ok = true
						break
					}
				}
				if !ok {
					return 0, fmt.Errorf("%w: %s.%s = %q", ErrInvalidValue, q.r.m.name, f.name, sv)
				}
			}
		}
		v, err := encodeQueryValue(f, s.value)
		if err != nil {
			return 0, err
		}
		assigns = append(assigns, fmt.Sprintf("%q = ?", f.column))
		vals = append(vals, v)
	}
	query := fmt.Sprintf("UPDATE %q SET %s", q.r.m.table, strings.Join(assigns, ", ")) + where
	res, err := q.r.h.q().ExecContext(q.ctx, query, append(vals, args...)...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (q *queryBuilder[T]) Delete() (int64, error) {
	where, args, _, err := q.build()
	if err != nil {
		return 0, err
	}
	query := fmt.Sprintf("DELETE FROM %q", q.r.m.table) + where
	res, err := q.r.h.q().ExecContext(q.ctx, query, args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
