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
		} else {
			have := map[string]bool{}
			for _, c := range existing {
				have[c] = true
			}
			for _, f := range m.fields {
				if !have[f.column] {
					ddl := columnDDL(f, false)
					if m.kind == kindEventSourced {
						ddl = esColumnDDL(f)
					}
					stmt := fmt.Sprintf("ALTER TABLE %q ADD COLUMN %s", m.table, ddl)
					if _, err := d.sql.ExecContext(ctx, stmt); err != nil {
						return fmt.Errorf("orm: Spalte %s.%s ergänzen: %w", m.table, f.column, err)
					}
				}
			}
		}
		if m.kind == kindEventSourced {
			if err := d.ensureESTables(ctx, m); err != nil {
				return err
			}
		}
	}
	return nil
}

// ensureESTables legt Event-Log und Snapshot-Tabelle eines ES-Models an.
func (d *DB) ensureESTables(ctx context.Context, m *model) error {
	for table, stmts := range map[string][]string{
		esEventsTable(m): esEventsSQL(m),
		esSnapsTable(m):  esSnapshotsSQL(m),
	} {
		existing, err := d.dial.tableColumns(d.sql, table)
		if err != nil {
			return fmt.Errorf("orm: Schema von %s lesen: %w", table, err)
		}
		if len(existing) > 0 {
			continue
		}
		for _, stmt := range stmts {
			if _, err := d.sql.ExecContext(ctx, stmt); err != nil {
				return fmt.Errorf("orm: %s anlegen: %w (%s)", table, err, stmt)
			}
		}
	}
	return nil
}

// createTableSQL erzeugt CREATE TABLE + Indizes für ein Model.
func createTableSQL(m *model) []string {
	if m.kind == kindEventSourced {
		return esReadModelSQL(m)
	}
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
		// Kein FK auf ES-Read-Models: deren Zeilen sind rebuildbare Artefakte
		// (RebuildProjection löscht sie temporär) — Prüfung nur engine-seitig.
		if f.ref != nil && f.ref.kind != kindEventSourced {
			constraints = append(constraints, fmt.Sprintf(
				"FOREIGN KEY (%q) REFERENCES %q (%q) ON DELETE %s",
				f.column, f.ref.table, f.ref.pkColumn(), f.refOn.sql()))
		}
	}

	stmts := []string{fmt.Sprintf("CREATE TABLE %q (\n  %s\n)", m.table,
		strings.Join(append(cols, constraints...), ",\n  "))}
	return append(stmts, indexStmts(m)...)
}

// indexStmts erzeugt die Index-DDL eines Models (Tags + Model-Optionen).
func indexStmts(m *model) []string {
	var stmts []string
	for _, f := range m.fields {
		switch {
		case f.unique:
			stmts = append(stmts, uniqueIndexSQL(m, []string{f.column}))
		case f.indexed:
			stmts = append(stmts, fmt.Sprintf("CREATE INDEX %q ON %q (%s)",
				fmt.Sprintf("ix_%s_%s", m.table, f.column), m.table, quoteAll(f.column)))
		}
	}
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

// esReadModelSQL erzeugt das Read-Model eines ES-Models: implizite "id" als
// PK, die Struct-Spalten ohne NOT-NULL/CHECK (Validierung ist Sache von
// Apply — die Zeile ist ein Projektions-Artefakt), plus aggregate_seq.
func esReadModelSQL(m *model) []string {
	cols := []string{`"id" TEXT PRIMARY KEY`}
	for _, f := range m.fields {
		cols = append(cols, esColumnDDL(f))
	}
	if m.tenanted() {
		cols = append(cols, `tenant_id TEXT NOT NULL REFERENCES ormpp_tenants (tenant_id)`)
	}
	cols = append(cols,
		`geo TEXT NOT NULL DEFAULT 'local'`,
		`"aggregate_seq" INTEGER NOT NULL DEFAULT 0`)
	stmts := []string{fmt.Sprintf("CREATE TABLE %q (\n  %s\n)", m.table, strings.Join(cols, ",\n  "))}
	return append(stmts, indexStmts(m)...)
}

func esColumnDDL(f *field) string {
	return fmt.Sprintf("%q %s", f.column, sqlType(f))
}

// esEventsSQL erzeugt den Append-only-Event-Log eines ES-Models.
func esEventsSQL(m *model) []string {
	t := esEventsTable(m)
	tenantCol := ""
	if m.tenanted() {
		tenantCol = "\n  \"tenant_id\" TEXT NOT NULL,"
	}
	return []string{
		fmt.Sprintf(`CREATE TABLE %q (
  "aggregate_id" TEXT NOT NULL,
  "aggregate_seq" INTEGER NOT NULL,%s
  "geo" TEXT NOT NULL,
  "seq" INTEGER NOT NULL,
  "event_id" TEXT NOT NULL,
  "occurred_at" TEXT NOT NULL,
  "type_id" INTEGER NOT NULL,
  "data" TEXT NOT NULL,
  PRIMARY KEY ("aggregate_id", "aggregate_seq")
)`, t, tenantCol),
		fmt.Sprintf("CREATE UNIQUE INDEX %q ON %q (%s)", "ux_"+t+"_geo_seq", t, quoteAll("geo", "seq")),
	}
}

// esSnapshotsSQL erzeugt die Snapshot-Tabelle eines ES-Models. Nicht
// append-only: KeepLast-Politik löscht ältere Stände.
func esSnapshotsSQL(m *model) []string {
	t := esSnapsTable(m)
	tenantCol := ""
	if m.tenanted() {
		tenantCol = "\n  \"tenant_id\" TEXT NOT NULL,"
	}
	return []string{fmt.Sprintf(`CREATE TABLE %q (
  "aggregate_id" TEXT NOT NULL,
  "aggregate_seq" INTEGER NOT NULL,%s
  "taken_at" TEXT NOT NULL,
  "state" BLOB NOT NULL,
  PRIMARY KEY ("aggregate_id", "aggregate_seq")
)`, t, tenantCol)}
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
