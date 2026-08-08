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
		existing, err := d.dial.tableColumns(d.q(), m.table)
		if err != nil {
			return fmt.Errorf("orm: Schema von %s lesen: %w", m.table, err)
		}
		if len(existing) == 0 {
			for _, stmt := range createTableSQL(d, m) {
				if err := d.execDDL(ctx, stmt); err != nil {
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
					ddl := columnDDL(d.dial, f, false)
					if m.kind == kindEventSourced {
						ddl = esColumnDDL(d.dial, f)
					}
					stmt := fmt.Sprintf("ALTER TABLE %q ADD COLUMN %s", m.table, ddl)
					if err := d.execDDL(ctx, stmt); err != nil {
						return fmt.Errorf("orm: Spalte %s.%s ergänzen: %w", m.table, f.column, err)
					}
				}
			}
			// Nachträglich deklarierte lookup-Felder: Index-Spalte (nullable —
			// Bestandszeilen füllt EncryptFields bzw. der nächste Schreibzugriff)
			// und Index additiv nachziehen.
			for _, f := range m.fields {
				if !f.lookup {
					continue
				}
				lc := f.lookupColumn()
				if !have[lc] {
					stmt := fmt.Sprintf("ALTER TABLE %q ADD COLUMN %q %s", m.table, lc, d.dial.columnType(kBlob))
					if err := d.execDDL(ctx, stmt); err != nil {
						return fmt.Errorf("orm: Lookup-Spalte %s.%s ergänzen: %w", m.table, lc, err)
					}
				}
				for _, stmt := range lookupIndexStmts(m, f, d.partModel(m), true) {
					if err := d.execDDL(ctx, stmt); err != nil {
						return fmt.Errorf("orm: Lookup-Index auf %s.%s: %w", m.table, lc, err)
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

// ensureESTables legt Event-Log (partitioniert, wo nativ), Archiv- und
// Snapshot-Tabelle eines ES-Models an. Neue Regionen ergänzen ihre
// Partition additiv bei jedem Migrate.
func (d *DB) ensureESTables(ctx context.Context, m *model) error {
	regions := d.regionPlacements()
	for table, stmts := range map[string][]string{
		esEventsTable(m):  esEventsSQL(d.dial, m, regions),
		esArchiveTable(m): esArchiveSQL(d, m),
		esSnapsTable(m):   esSnapshotsSQL(d, m),
	} {
		existing, err := d.dial.tableColumns(d.q(), table)
		if err != nil {
			return fmt.Errorf("orm: Schema von %s lesen: %w", table, err)
		}
		if len(existing) > 0 {
			continue
		}
		for _, stmt := range stmts {
			if err := d.execDDL(ctx, stmt); err != nil {
				return fmt.Errorf("orm: %s anlegen: %w (%s)", table, err, stmt)
			}
		}
	}
	// Bestands-Snapshots aus Ständen vor der Geo-Residenz: geo-Spalte
	// additiv ergänzen und aus dem Event-Log des Aggregats füllen —
	// der Snapshot residiert wie sein Aggregat.
	if err := d.ensureSnapshotGeo(ctx, m); err != nil {
		return err
	}
	// Nachträglich deklarierte Regionen zieht reconcileGeoPartitions bei
	// jedem Migrate nach — hier entstehen nur die Partitionen der bereits
	// bekannten Topologie, gleich mit der frisch angelegten Tabelle.
	return nil
}

func (d *DB) ensureSnapshotGeo(ctx context.Context, m *model) error {
	sn := esSnapsTable(m)
	cols, err := d.dial.tableColumns(d.q(), sn)
	if err != nil {
		return err
	}
	for _, c := range cols {
		if c == "geo" {
			return nil
		}
	}
	stmts := []string{
		fmt.Sprintf(`ALTER TABLE %q ADD COLUMN "geo" TEXT NOT NULL DEFAULT 'local'`, sn),
		fmt.Sprintf(`UPDATE %q SET "geo" = COALESCE(
			(SELECT e."geo" FROM %q e WHERE e."aggregate_id" = %q."aggregate_id" LIMIT 1), 'local')`,
			sn, esEventsTable(m), sn),
	}
	for _, s := range stmts {
		if err := d.execDDL(ctx, s); err != nil {
			return fmt.Errorf("orm: %s um geo ergänzen: %w", sn, err)
		}
	}
	return nil
}

// createTableSQL erzeugt CREATE TABLE + Indizes für ein Model. Bei aktiver
// Geo-Partitionierung (PG/YB + Topologie) entsteht die Tabelle als
// PARTITION BY LIST (geo) mit einer Partition je Region (samt Placement)
// plus DEFAULT-Partition — der PK muss dann den Partitionsschlüssel
// enthalten. FKs, deren ZIEL partitioniert ist, sind nicht darstellbar
// (Unique auf dem Ziel müsste geo enthalten) — dort prüft die Engine
// (checkRef, ondelete-Emulation), wie bei ES-Read-Models schon immer.
func createTableSQL(d *DB, m *model) []string {
	dial := d.dial
	if m.kind == kindEventSourced {
		return esReadModelSQL(d, m)
	}
	part := d.partModel(m)
	var cols []string
	var constraints []string

	for _, f := range m.fields {
		ddl := columnDDL(dial, f, true)
		if part && f.pk {
			// PK wandert in den Table-Constraint (pk, geo) — Partitions-
			// Anforderung: der Schlüssel muss den Partitionsschlüssel enthalten.
			ddl = fmt.Sprintf("%q %s NOT NULL", f.column, dial.columnType(colKindOf(f)))
		}
		cols = append(cols, ddl)
	}
	for _, f := range m.fields {
		if f.lookup {
			// Blind-Index-Spalte (nullable: leerer Klartext → NULL).
			cols = append(cols, fmt.Sprintf("%q %s", f.lookupColumn(), dial.columnType(kBlob)))
		}
	}
	if m.tenanted() {
		// FK von partitionierter Tabelle auf normale Tabelle ist erlaubt —
		// der Tenant-FK bleibt in beiden Formen erhalten.
		cols = append(cols, `tenant_id TEXT NOT NULL REFERENCES ormpp_tenants (tenant_id)`)
	}
	cols = append(cols, `geo TEXT NOT NULL DEFAULT 'local'`)
	if m.opts.geoMode == geoFlexible {
		// Heimat + lesende Replikat-Regionen pro Datensatz (JSON-Liste;
		// "*" = ReplicateAll, folgt der Topologie).
		cols = append(cols, `geo_replicas TEXT NOT NULL DEFAULT '[]'`)
	}
	if part {
		constraints = append(constraints, fmt.Sprintf("PRIMARY KEY (%s)", quoteAll(m.pk.column, "geo")))
	}

	for _, f := range m.fields {
		// Kein FK auf ES-Read-Models (rebuildbare Artefakte) und keiner auf
		// partitionierte Ziele — Prüfung dort engine-seitig.
		if f.ref != nil && f.ref.kind != kindEventSourced && !d.partModel(f.ref) {
			constraints = append(constraints, fmt.Sprintf(
				"FOREIGN KEY (%q) REFERENCES %q (%q) ON DELETE %s",
				f.column, f.ref.table, f.ref.pkColumn(), f.refOn.sql()))
		}
	}

	create := fmt.Sprintf("CREATE TABLE %q (\n  %s\n)", m.table,
		strings.Join(append(cols, constraints...), ",\n  "))
	var stmts []string
	if part {
		stmts = append(stmts, create+dial.partitionClause())
		stmts = append(stmts, dial.partitionSQL(m.table, d.regionPlacements())...)
	} else {
		stmts = append(stmts, create)
	}
	return append(stmts, indexStmts(m, part)...)
}

// indexStmts erzeugt die Index-DDL eines Models (Tags + Model-Optionen).
// Auf partitionierten Tabellen müssen Unique-Indizes den Partitionsschlüssel
// enthalten — physisch gilt Eindeutigkeit dann pro Geo; global prüft die
// Engine vor dem Schreiben (checkUniques).
func indexStmts(m *model, part bool) []string {
	var stmts []string
	for _, f := range m.fields {
		switch {
		case f.lookup:
			// unique wie index laufen über die Blind-Index-Spalte —
			// die Ciphertext-Spalte selbst bleibt indexfrei.
			stmts = append(stmts, lookupIndexStmts(m, f, part, false)...)
		case f.unique:
			stmts = append(stmts, uniqueIndexSQL(m, []string{f.column}, part))
		case f.indexed:
			stmts = append(stmts, fmt.Sprintf("CREATE INDEX %q ON %q (%s)",
				fmt.Sprintf("ix_%s_%s", m.table, f.column), m.table, quoteAll(f.column)))
		}
	}
	for _, set := range m.opts.uniques {
		stmts = append(stmts, uniqueIndexSQL(m, columnsOf(m, set), part))
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
func esReadModelSQL(d *DB, m *model) []string {
	dial := d.dial
	part := d.partModel(m)
	pkCol := `"id" TEXT PRIMARY KEY`
	if part {
		pkCol = `"id" TEXT NOT NULL`
	}
	cols := []string{pkCol}
	for _, f := range m.fields {
		cols = append(cols, esColumnDDL(dial, f))
	}
	if m.tenanted() {
		cols = append(cols, `tenant_id TEXT NOT NULL REFERENCES ormpp_tenants (tenant_id)`)
	}
	cols = append(cols,
		`geo TEXT NOT NULL DEFAULT 'local'`,
		fmt.Sprintf(`"aggregate_seq" %s NOT NULL DEFAULT 0`, dial.columnType(kInt)))
	create := fmt.Sprintf("CREATE TABLE %q (\n  %s\n)", m.table, strings.Join(cols, ",\n  "))
	var stmts []string
	if part {
		create = fmt.Sprintf("CREATE TABLE %q (\n  %s,\n  PRIMARY KEY (%s)\n)%s",
			m.table, strings.Join(cols, ",\n  "), quoteAll("id", "geo"), dial.partitionClause())
		stmts = append(stmts, create)
		stmts = append(stmts, dial.partitionSQL(m.table, d.regionPlacements())...)
	} else {
		stmts = append(stmts, create)
	}
	return append(stmts, indexStmts(m, part)...)
}

func esColumnDDL(dial dialect, f *field) string {
	return fmt.Sprintf("%q %s", f.column, dial.columnType(colKindOf(f)))
}

// esEventsSQL erzeugt den Append-only-Event-Log eines ES-Models. Auf
// PG/YB nativ PARTITION BY LIST (geo) — der PK muss dort den Partition-Key
// enthalten; die globale Eindeutigkeit von (aggregate_id, aggregate_seq)
// sichert das Aggregat-Geo-Pinning (ein Aggregat lebt in genau einer Region).
func esEventsSQL(dial dialect, m *model, regions []regionPlacement) []string {
	t := esEventsTable(m)
	intT := dial.columnType(kInt)
	tenantCol := ""
	if m.tenanted() {
		tenantCol = "\n  \"tenant_id\" TEXT NOT NULL,"
	}
	pk := `"aggregate_id", "aggregate_seq"`
	part := dial.partitionClause()
	if part != "" {
		pk += `, "geo"`
	}
	stmts := []string{fmt.Sprintf(`CREATE TABLE %q (
  "aggregate_id" TEXT NOT NULL,
  "aggregate_seq" %s NOT NULL,%s
  "geo" TEXT NOT NULL,
  "seq" %s NOT NULL,
  "event_id" TEXT NOT NULL,
  "occurred_at" TEXT NOT NULL,
  "type_id" %s NOT NULL,
  "data" TEXT NOT NULL,
  PRIMARY KEY (%s)
)%s`, t, intT, tenantCol, intT, intT, pk, part)}
	stmts = append(stmts, dial.partitionSQL(t, regions)...)
	return append(stmts,
		fmt.Sprintf("CREATE UNIQUE INDEX %q ON %q (%s)", "ux_"+t+"_geo_seq", t, quoteAll("geo", "seq")))
}

// esArchiveSQL erzeugt die Archiv-Nebentabelle: gleiche Spalten wie der
// Hot-Log, SQL-abfragbar (Reads laufen transparent als UNION über
// Hot + Archiv). Bei aktiver Geo-Partitionierung ebenfalls partitioniert —
// archivierte Events sind dieselben Tenant-Daten und residieren gleich.
func esArchiveSQL(d *DB, m *model) []string {
	dial := d.dial
	t := esArchiveTable(m)
	intT := dial.columnType(kInt)
	tenantCol := ""
	if m.tenanted() {
		tenantCol = "\n  \"tenant_id\" TEXT NOT NULL,"
	}
	pk, clause := `"aggregate_id", "aggregate_seq"`, ""
	if d.partModel(m) {
		pk += `, "geo"`
		clause = dial.partitionClause()
	}
	stmts := []string{fmt.Sprintf(`CREATE TABLE %q (
  "aggregate_id" TEXT NOT NULL,
  "aggregate_seq" %s NOT NULL,%s
  "geo" TEXT NOT NULL,
  "seq" %s NOT NULL,
  "event_id" TEXT NOT NULL,
  "occurred_at" TEXT NOT NULL,
  "type_id" %s NOT NULL,
  "data" TEXT NOT NULL,
  PRIMARY KEY (%s)
)%s`, t, intT, tenantCol, intT, intT, pk, clause)}
	if d.partModel(m) {
		stmts = append(stmts, dial.partitionSQL(t, d.regionPlacements())...)
	}
	return append(stmts,
		fmt.Sprintf("CREATE INDEX %q ON %q (%s)", "ix_"+t+"_geo_seq", t, quoteAll("geo", "seq")))
}

// esSnapshotsSQL erzeugt die Snapshot-Tabelle eines ES-Models. Nicht
// append-only: KeepLast-Politik löscht ältere Stände. Trägt seit der
// Geo-Residenz eine geo-Spalte — der Snapshot ist derselbe Zustand wie
// das Read-Model und muss gleich residieren.
func esSnapshotsSQL(d *DB, m *model) []string {
	dial := d.dial
	t := esSnapsTable(m)
	tenantCol := ""
	if m.tenanted() {
		tenantCol = "\n  \"tenant_id\" TEXT NOT NULL,"
	}
	pk, clause := `"aggregate_id", "aggregate_seq"`, ""
	if d.partModel(m) {
		pk += `, "geo"`
		clause = dial.partitionClause()
	}
	stmts := []string{fmt.Sprintf(`CREATE TABLE %q (
  "aggregate_id" TEXT NOT NULL,
  "aggregate_seq" %s NOT NULL,%s
  "geo" TEXT NOT NULL DEFAULT 'local',
  "taken_at" TEXT NOT NULL,
  "state" %s NOT NULL,
  PRIMARY KEY (%s)
)%s`, t, dial.columnType(kInt), tenantCol, dial.columnType(kBlob), pk, clause)}
	if d.partModel(m) {
		stmts = append(stmts, dial.partitionSQL(t, d.regionPlacements())...)
	}
	return stmts
}

// uniqueIndexSQL bezieht tenant_id automatisch ein: Eindeutigkeit gilt pro
// Tenant. Auf partitionierten Tabellen zusätzlich geo (Pflicht des
// Partitionsschlüssels) — die tenant-globale Eindeutigkeit sichert dort
// checkUniques vor dem Schreiben.
func uniqueIndexSQL(m *model, cols []string, part bool) string {
	full := cols
	if m.tenanted() {
		full = append([]string{"tenant_id"}, cols...)
	}
	if part {
		full = append(append([]string{}, full...), "geo")
	}
	return fmt.Sprintf("CREATE UNIQUE INDEX %q ON %q (%s)",
		fmt.Sprintf("ux_%s_%s", m.table, strings.Join(cols, "_")), m.table, quoteAll(full...))
}

// lookupIndexStmts erzeugt die Index-DDL einer Blind-Index-Spalte —
// unique gemäß Feld-Tag, additiv (Migrate auf Bestandstabellen) mit
// IF NOT EXISTS statt Neuanlage.
func lookupIndexStmts(m *model, f *field, part, additive bool) []string {
	lc := f.lookupColumn()
	ine := ""
	if additive {
		ine = "IF NOT EXISTS "
	}
	if f.unique {
		full := []string{lc}
		if m.tenanted() {
			full = append([]string{"tenant_id"}, full...)
		}
		if part {
			full = append(full, "geo")
		}
		return []string{fmt.Sprintf("CREATE UNIQUE INDEX %s%q ON %q (%s)",
			ine, fmt.Sprintf("ux_%s_%s", m.table, lc), m.table, quoteAll(full...))}
	}
	return []string{fmt.Sprintf("CREATE INDEX %s%q ON %q (%s)",
		ine, fmt.Sprintf("ix_%s_%s", m.table, lc), m.table, quoteAll(lc))}
}

// columnsOf bildet Feldnamen auf ihre Query-Spalten ab — bei lookup-Feldern
// die Index-Spalte (Model-Uniques/-Indizes vergleichen den Blind-Index).
func columnsOf(m *model, fieldNames []string) []string {
	cols := make([]string, len(fieldNames))
	for i, fn := range fieldNames {
		f := m.fieldByName(fn)
		if f.lookup {
			cols[i] = f.lookupColumn()
		} else {
			cols[i] = f.column
		}
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
func columnDDL(dial dialect, f *field, inCreate bool) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%q %s", f.column, dial.columnType(colKindOf(f)))
	if f.pk && inCreate {
		b.WriteString(" PRIMARY KEY")
	}
	if !f.nullable && !f.pk {
		if inCreate {
			b.WriteString(" NOT NULL")
		} else {
			// ALTER ADD COLUMN mit NOT NULL braucht einen Default für Bestandszeilen.
			fmt.Fprintf(&b, " NOT NULL DEFAULT %s", dial.zeroLiteral(colKindOf(f)))
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

// colKindOf klassifiziert ein Feld backend-neutral; den physischen Typ
// liefert der Dialekt (SQLite: TEXT/INTEGER/REAL/BLOB, PG: BIGINT/JSONB/BYTEA …).
func colKindOf(f *field) colKind {
	t := f.goType
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	switch {
	case f.encrypted:
		return kBlob // Ciphertext, unabhängig vom Go-Typ
	case f.json:
		return kJSON
	case t == idType, t == timeType:
		return kText
	case t.Kind() == reflect.String:
		return kText
	case t.Kind() == reflect.Bool:
		return kBool
	case t.Kind() >= reflect.Int && t.Kind() <= reflect.Uint64:
		return kInt
	case t.Kind() == reflect.Float32 || t.Kind() == reflect.Float64:
		return kFloat
	case t.Kind() == reflect.Slice && t.Elem().Kind() == reflect.Uint8:
		return kBlob
	default:
		return kText // komplexe Typen brauchen das json-Tag; validiert die Registry künftig strenger
	}
}
