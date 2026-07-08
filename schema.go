package orm

import (
	"context"
	"fmt"
	"reflect"
	"strings"
)

// applySchema legt fehlende Tabellen an und ergänzt fehlende Spalten
// (additiver Diff). Nicht-additive Änderungen sind Sache der
// Migrations-Zustandsmaschine (Phase 3).
func (d *DB) applySchema(ctx context.Context) error {
	models, err := d.reg.sortedByDeps()
	if err != nil {
		return err
	}
	for _, m := range models {
		existing, err := d.dial.tableColumns(d.sql, m.table)
		if err != nil {
			return fmt.Errorf("orm: Schema von %s lesen: %w", m.table, err)
		}
		if len(existing) == 0 {
			for _, stmt := range createTableSQL(m) {
				if _, err := d.sql.ExecContext(ctx, stmt); err != nil {
					return fmt.Errorf("orm: %s anlegen: %w (%s)", m.table, err, stmt)
				}
			}
			continue
		}
		have := map[string]bool{}
		for _, c := range existing {
			have[c] = true
		}
		for _, f := range m.fields {
			if !have[f.column] {
				stmt := fmt.Sprintf("ALTER TABLE %q ADD COLUMN %s", m.table, columnDDL(f, false))
				if _, err := d.sql.ExecContext(ctx, stmt); err != nil {
					return fmt.Errorf("orm: Spalte %s.%s ergänzen: %w", m.table, f.column, err)
				}
			}
		}
	}
	return nil
}

// createTableSQL erzeugt CREATE TABLE + Indizes für ein Model.
func createTableSQL(m *model) []string {
	var cols []string
	var constraints []string

	for _, f := range m.fields {
		cols = append(cols, columnDDL(f, true))
	}
	if m.tenanted() {
		cols = append(cols, `tenant_id TEXT NOT NULL REFERENCES ormpp_tenants (tenant_id)`)
	}
	cols = append(cols, `geo TEXT NOT NULL DEFAULT 'local'`)

	for _, f := range m.fields {
		if f.ref != nil {
			constraints = append(constraints, fmt.Sprintf(
				"FOREIGN KEY (%q) REFERENCES %q (%q) ON DELETE %s",
				f.column, f.ref.table, f.ref.pk.column, f.refOn.sql()))
		}
	}

	stmts := []string{fmt.Sprintf("CREATE TABLE %q (\n  %s\n)", m.table,
		strings.Join(append(cols, constraints...), ",\n  "))}

	// Einzelfeld-Indizes aus Tags:
	for _, f := range m.fields {
		switch {
		case f.unique:
			stmts = append(stmts, uniqueIndexSQL(m, []string{f.column}))
		case f.indexed:
			stmts = append(stmts, fmt.Sprintf("CREATE INDEX %q ON %q (%s)",
				fmt.Sprintf("ix_%s_%s", m.table, f.column), m.table, quoteAll(f.column)))
		}
	}
	// Zusammengesetzte Constraints aus Model-Optionen:
	for _, set := range m.opts.uniques {
		stmts = append(stmts, uniqueIndexSQL(m, columnsOf(m, set)))
	}
	for _, set := range m.opts.indexes {
		cols := columnsOf(m, set)
		stmts = append(stmts, fmt.Sprintf("CREATE INDEX %q ON %q (%s)",
			fmt.Sprintf("ix_%s_%s", m.table, strings.Join(cols, "_")), m.table, quoteAll(cols...)))
	}
	return stmts
}

// uniqueIndexSQL bezieht tenant_id automatisch ein: Eindeutigkeit gilt pro Tenant.
func uniqueIndexSQL(m *model, cols []string) string {
	full := cols
	if m.tenanted() {
		full = append([]string{"tenant_id"}, cols...)
	}
	return fmt.Sprintf("CREATE UNIQUE INDEX %q ON %q (%s)",
		fmt.Sprintf("ux_%s_%s", m.table, strings.Join(cols, "_")), m.table, quoteAll(full...))
}

func columnsOf(m *model, fieldNames []string) []string {
	cols := make([]string, len(fieldNames))
	for i, fn := range fieldNames {
		cols[i] = m.fieldByName(fn).column
	}
	return cols
}

func quoteAll(cols ...string) string {
	q := make([]string, len(cols))
	for i, c := range cols {
		q[i] = fmt.Sprintf("%q", c)
	}
	return strings.Join(q, ", ")
}

// columnDDL erzeugt die Spaltendefinition eines Feldes.
// inCreate steuert, ob PK-Klauseln erlaubt sind (ALTER ADD COLUMN kann das nicht).
func columnDDL(f *field, inCreate bool) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%q %s", f.column, sqlType(f))
	if f.pk && inCreate {
		b.WriteString(" PRIMARY KEY")
	}
	if !f.nullable && !f.pk {
		if inCreate {
			b.WriteString(" NOT NULL")
		} else {
			// ALTER ADD COLUMN mit NOT NULL braucht einen Default für Bestandszeilen.
			fmt.Fprintf(&b, " NOT NULL DEFAULT %s", zeroLiteral(f))
		}
	}
	if f.hasDefault {
		fmt.Fprintf(&b, " DEFAULT '%s'", strings.ReplaceAll(f.defaultVal, "'", "''"))
	}
	if len(f.enum) > 0 {
		quoted := make([]string, len(f.enum))
		for i, v := range f.enum {
			quoted[i] = "'" + strings.ReplaceAll(v, "'", "''") + "'"
		}
		fmt.Fprintf(&b, " CHECK (%q IN (%s))", f.column, strings.Join(quoted, ", "))
	}
	return b.String()
}

func sqlType(f *field) string {
	t := f.goType
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	switch {
	case f.json:
		return "TEXT"
	case t == idType:
		return "TEXT"
	case t == timeType:
		return "TEXT"
	case t.Kind() == reflect.String:
		return "TEXT"
	case t.Kind() == reflect.Bool:
		return "INTEGER"
	case t.Kind() >= reflect.Int && t.Kind() <= reflect.Uint64:
		return "INTEGER"
	case t.Kind() == reflect.Float32 || t.Kind() == reflect.Float64:
		return "REAL"
	case t.Kind() == reflect.Slice && t.Elem().Kind() == reflect.Uint8:
		return "BLOB"
	default:
		return "TEXT" // komplexe Typen brauchen das json-Tag; validiert die Registry künftig strenger
	}
}

func zeroLiteral(f *field) string {
	switch sqlType(f) {
	case "INTEGER":
		return "0"
	case "REAL":
		return "0"
	case "BLOB":
		return "x''"
	default:
		return "''"
	}
}
