package orm

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"
)

// Physische Geo-Residenz: Partitionen gegen die deklarierte Topologie
// abgleichen, Placements binden, Datensätze und Mandanten umziehen.
//
// Die Bindung an ein Placement passiert ausschließlich beim CREATE TABLE
// der Partition. ALTER TABLE … SET TABLESPACE ist kein Ersatz: YugabyteDB
// meldet Erfolg und schreibt den Katalog um, bewegt die Tablets ohne
// ysql_beta_feature_tablespace_alteration aber nicht — der Katalog
// behauptete dann eine Platzierung, die es nicht gibt.

// esEventKey ist der fachliche Schlüssel einer Event-Zeile (ohne geo) —
// er identifiziert beim Umhängen zwischen Partitionen bereits kopierte
// Zeilen.
var esEventKey = []string{"aggregate_id", "aggregate_seq"}

const (
	geoMoveBatch = 500
	geoLeaseTTL  = 30 * time.Second
)

// aggregateGeo liest die gepinnte Heimatregion eines Aggregats aus dem
// Event-Log — aus dem Hot-Log, sonst aus dem Archiv (alles archiviert).
func (d *DB) aggregateGeo(ctx context.Context, m *model, aggID string) (string, error) {
	for _, t := range []string{esEventsTable(m), esArchiveTable(m)} {
		var geo string
		err := d.q().QueryRowContext(ctx, fmt.Sprintf(
			`SELECT "geo" FROM %q WHERE "aggregate_id" = ? LIMIT 1`, t), aggID).Scan(&geo)
		if err == nil {
			return geo, nil
		}
		if err != sql.ErrNoRows {
			return "", err
		}
	}
	return "local", nil
}

// regionPlacements liefert die deklarierte Topologie als Placement-Liste:
// dedupliziert (die letzte Deklaration gewinnt) und stabil sortiert.
func (d *DB) regionPlacements() []regionPlacement {
	at := map[string]int{}
	var out []regionPlacement
	for _, r := range d.regionDecls {
		if i, ok := at[r.name]; ok {
			out[i].placement = r.placement
			continue
		}
		at[r.name] = len(out)
		out = append(out, regionPlacement(r))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].name < out[j].name })
	return out
}

// geoTableSpec beschreibt eine geo-partitionierte Tabelle eines Models:
// den fachlichen Schlüssel (identifiziert Zeilen beim Umhängen) und die
// DDL für eine Neuanlage in partitionierter Form.
type geoTableSpec struct {
	table  string
	key    []string
	create func() []string
}

// geoTables liefert die Tabellen eines Models, die der Geo-Partitionierung
// unterliegen. Der Event-Log fehlt bewusst: er ist auf PG/YB seit jeher
// partitioniert und braucht nie einen Umbau — nur den Regions-Abgleich.
func (d *DB) geoTables(m *model) []geoTableSpec {
	if !d.partModel(m) {
		return nil
	}
	if m.kind == kindEventSourced {
		return []geoTableSpec{
			{m.table, []string{"id"}, func() []string { return esReadModelSQL(d, m) }},
			{esArchiveTable(m), esEventKey, func() []string { return esArchiveSQL(d, m) }},
			{esSnapsTable(m), esEventKey, func() []string { return esSnapshotsSQL(d, m) }},
		}
	}
	return []geoTableSpec{
		{m.table, []string{m.pk.column}, func() []string { return createTableSQL(d, m) }},
	}
}

// reconcileGeoPartitions gleicht die physische Ablage gegen die aktive
// Topologie ab. Läuft bei JEDEM Migrate:
//
//   - Bestands-Tabellen, die vor der Topologie (oder vor dieser Version)
//     unpartitioniert entstanden sind, werden in die partitionierte Form
//     überführt (rebuildPartitioned).
//   - Fehlende Regions-Partitionen werden nachgelegt; Zeilen, die bis
//     dahin in der DEFAULT-Partition lagen, ziehen mit um.
func (d *DB) reconcileGeoPartitions(ctx context.Context) error {
	regs := d.regionPlacements()
	if !d.geoPartitioned() {
		return nil // Backend kollabiert alle Regionen, oder keine Topologie deklariert
	}
	if err := d.verifyPlacements(regs); err != nil {
		return err
	}
	// Ein Leader genügt: gleichzeitige Umbauten/Adopt-Läufe zweier
	// Instanzen würden sich um dieselben Zeilen streiten. Wer die Lease
	// nicht bekommt, lässt den Halter arbeiten.
	ok, err := d.acquireLease(ctx, "geo-partitions", geoLeaseTTL)
	if err != nil || !ok {
		return err
	}
	defer d.releaseLease(ctx, "geo-partitions")

	for _, m := range d.reg.ordered {
		for _, spec := range d.geoTables(m) {
			kind, err := d.dial.tableKind(d.q(), spec.table)
			if err != nil {
				return err
			}
			if kind == 'r' {
				if err := d.rebuildPartitioned(ctx, spec); err != nil {
					return fmt.Errorf("orm: %s partitionieren: %w", spec.table, err)
				}
			}
			if err := d.reconcilePartitions(ctx, spec.table, spec.key, regs); err != nil {
				return err
			}
		}
		if m.kind == kindEventSourced && m.opts.geoMode != geoGlobal {
			if err := d.reconcilePartitions(ctx, esEventsTable(m), esEventKey, regs); err != nil {
				return err
			}
		}
	}
	return nil
}

// rebuildPartitioned überführt eine unpartitionierte Bestands-Tabelle in
// die partitionierte Form: eingehende FKs lösen (auf ein partitioniertes
// Ziel sind sie nicht darstellbar — die Engine prüft weiter), Indizes
// beiseite benennen, Tabelle wegbenennen, partitioniert neu anlegen,
// Zeilen kopieren, Alt-Tabelle fallen lassen.
//
// Bewusst Schritt für Schritt statt einer großen Transaktion (YB) — jede
// Anweisung ist für sich wiederholbar, ein abgebrochener Lauf wird beim
// nächsten Migrate an der Marke `<tabelle>_vorgeo` fortgesetzt. Während
// der Kopie sichtbare Zeilen sind vollständig, sobald die Kopie durch ist;
// Schreibzugriffe treffen ab der Neuanlage die neue Tabelle und gehen
// nicht verloren (die Kopie überspringt vorhandene Schlüssel).
func (d *DB) rebuildPartitioned(ctx context.Context, spec geoTableSpec) error {
	old := spec.table + "_vorgeo"
	oldKind, err := d.dial.tableKind(d.q(), old)
	if err != nil {
		return err
	}
	curKind, err := d.dial.tableKind(d.q(), spec.table)
	if err != nil {
		return err
	}
	if curKind == 'r' && oldKind != 0 {
		return fmt.Errorf("orm: %s und %s existieren beide unpartitioniert — manuell auflösen", spec.table, old)
	}

	if curKind == 'r' {
		fks, err := d.dial.incomingFKs(d.q(), spec.table)
		if err != nil {
			return err
		}
		for _, fk := range fks {
			if _, err := d.q().ExecContext(ctx, fmt.Sprintf(
				`ALTER TABLE %q DROP CONSTRAINT IF EXISTS %q`, fk[0], fk[1])); err != nil {
				return err
			}
		}
		// Indexnamen sind schemaweit — die Neuanlage würde sonst kollidieren.
		idxs, err := d.dial.tableIndexes(d.q(), spec.table)
		if err != nil {
			return err
		}
		for _, ix := range idxs {
			if strings.HasSuffix(ix, "_vorgeo") {
				continue
			}
			if _, err := d.q().ExecContext(ctx, fmt.Sprintf(
				`ALTER INDEX %q RENAME TO %q`, ix, ix+"_vorgeo")); err != nil {
				return err
			}
		}
		if _, err := d.q().ExecContext(ctx, fmt.Sprintf(
			`ALTER TABLE %q RENAME TO %q`, spec.table, old)); err != nil {
			return err
		}
		curKind = 0
	}
	if curKind == 0 {
		for _, stmt := range spec.create() {
			if _, err := d.q().ExecContext(ctx, stmt); err != nil {
				return fmt.Errorf("%w (%s)", err, strings.Join(strings.Fields(stmt), " "))
			}
		}
	}
	if oldKind, err = d.dial.tableKind(d.q(), old); err != nil || oldKind == 0 {
		return err
	}

	// Kopie über die Schnittmenge der Spalten (die Alt-Tabelle kann
	// zusätzliche deprecated-Spalten tragen), idempotent per Schlüssel.
	oldCols, err := d.dial.tableColumns(d.q(), old)
	if err != nil {
		return err
	}
	newCols, err := d.dial.tableColumns(d.q(), spec.table)
	if err != nil {
		return err
	}
	have := map[string]bool{}
	for _, c := range newCols {
		have[c] = true
	}
	var cols []string
	for _, c := range oldCols {
		if have[c] {
			cols = append(cols, c)
		}
	}
	match := make([]string, len(spec.key))
	for i, k := range spec.key {
		match[i] = fmt.Sprintf("p.%q = a.%q", k, k)
	}
	if _, err := d.q().ExecContext(ctx, fmt.Sprintf(
		`INSERT INTO %q (%s) SELECT %s FROM %q a WHERE NOT EXISTS (SELECT 1 FROM %q p WHERE %s)`,
		spec.table, quoteAll(cols...), prefixedCols("a", cols), old, spec.table,
		strings.Join(match, " AND "))); err != nil {
		return err
	}
	_, err = d.q().ExecContext(ctx, fmt.Sprintf(`DROP TABLE %q`, old))
	return err
}

func prefixedCols(alias string, cols []string) string {
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = fmt.Sprintf("%s.%q", alias, c)
	}
	return strings.Join(out, ", ")
}

// verifyPlacements prüft, dass jedes deklarierte Placement im Backend
// existiert. ORM++ legt keine Tablespaces an: Replikatzahl und
// Placement-Blöcke sind eine Betriebsentscheidung, keine ORM-Entscheidung.
func (d *DB) verifyPlacements(regs []regionPlacement) error {
	for _, r := range regs {
		if r.placement == "" {
			continue
		}
		ok, err := d.dial.placementExists(d.q(), r.placement)
		if err != nil {
			return fmt.Errorf("orm: Placement %q prüfen: %w", r.placement, err)
		}
		if !ok {
			return fmt.Errorf("orm: Region %s: Placement %q existiert nicht — Tablespace anlegen (Betriebsaufgabe, ORM++ legt keine an): %w",
				r.name, r.placement, ErrPlacementNotFound)
		}
	}
	return nil
}

func (d *DB) reconcilePartitions(ctx context.Context, table string, key []string, regs []regionPlacement) error {
	cols, err := d.dial.tableColumns(d.q(), table)
	if err != nil {
		return fmt.Errorf("orm: Schema von %s lesen: %w", table, err)
	}
	if len(cols) == 0 {
		return nil // Tabelle noch nicht angelegt
	}
	have, err := d.dial.geoPartitions(d.q(), table)
	if err != nil {
		return fmt.Errorf("orm: Partitionen von %s lesen: %w", table, err)
	}
	for _, r := range regs {
		space, exists := have[r.name]
		if exists {
			if r.placement != "" && space != r.placement {
				return placementDriftErr(table, r, space)
			}
			continue
		}
		if err := d.createGeoPartition(ctx, table, key, r, have); err != nil {
			return fmt.Errorf("orm: Partition für Region %s auf %s: %w", r.name, table, err)
		}
	}
	return nil
}

// placementDriftErr: die Partition liegt woanders als deklariert. Bewusst
// ein Fehler und kein stiller Abgleich — die Zeilen liegen dann nicht in
// der Region, die die Deklaration verspricht.
func placementDriftErr(table string, r regionPlacement, space string) error {
	at := "im Default-Tablespace"
	if space != "" {
		at = fmt.Sprintf("in Tablespace %q", space)
	}
	return fmt.Errorf(`orm: %s: Partition der Region %s liegt %s, deklariert ist %q. `+
		`ALTER TABLE … SET TABLESPACE bindet sie nicht um (YugabyteDB meldet Erfolg, bewegt die Tablets aber nicht) — `+
		`eine Neubindung heißt DETACH, Neuanlage mit Tablespace, Kopie, ATTACH und ist eine Betriebsentscheidung. `+
		`Alternativ die Deklaration auf %q zurücksetzen: %w`,
		table, r.name, at, r.placement, space, ErrPlacementMismatch)
}

// createGeoPartition legt die Partition einer Region an. Hält die
// DEFAULT-Partition bereits Zeilen dieser Region, lehnen PG/YB die direkte
// Anlage ab — dann wird die Partition daneben gebaut, die Zeilen ziehen um
// und sie wird angeschlossen. Ein abgebrochener Lauf hinterlässt die noch
// nicht angeschlossene Tabelle; der nächste Aufruf führt ihn zu Ende.
func (d *DB) createGeoPartition(ctx context.Context, table string, key []string, r regionPlacement, have map[string]string) error {
	pending, err := d.dial.tableColumns(d.q(), geoPartName(table, r.name))
	if err != nil {
		return err
	}
	stranded := int64(0)
	if _, ok := have[geoDefaultRegion]; ok {
		if stranded, err = d.countGeoRows(ctx, geoPartName(table, geoDefaultRegion), r.name); err != nil {
			return err
		}
	}
	stmts := d.dial.partitionSQL(table, []regionPlacement{r})
	if len(pending) > 0 || stranded > 0 {
		stmts = d.dial.adoptRegionSQL(table, key, r)
	}
	for _, s := range stmts {
		if _, err := d.q().ExecContext(ctx, s); err != nil {
			return fmt.Errorf("%w (%s)", err, strings.Join(strings.Fields(s), " "))
		}
	}
	return nil
}

func (d *DB) countGeoRows(ctx context.Context, table, geo string) (int64, error) {
	var n int64
	err := d.q().QueryRowContext(ctx,
		fmt.Sprintf(`SELECT COUNT(*) FROM %q WHERE "geo" = ?`, table), geo).Scan(&n)
	return n, err
}

// RemoveRegion entfernt eine Region physisch aus dem Cluster: die
// Partitionen werden abgeräumt und das Topologie-Register auf 'removed'
// gesetzt. Hält die Region noch Daten, schlägt der Aufruf fehl und nennt
// die betroffenen Tabellen — Zeilen zieht MoveTenant/SetGeo um, nicht
// diese Operation.
//
// Voraussetzung: die Region ist in dieser App-Ausgabe nicht mehr
// deklariert. Sonst legte der nächste Migrate sie sofort wieder an.
func (d *DB) RemoveRegion(ctx context.Context, name string) error {
	if !d.migrated {
		return fmt.Errorf("orm: Migrate muss vor RemoveRegion laufen")
	}
	if d.regions[name] {
		return fmt.Errorf("orm: Region %s ist noch in Topology deklariert — erst aus der Deklaration nehmen und ausrollen", name)
	}
	var withData []string
	var drop []string
	for _, m := range d.reg.ordered {
		tables := []string{m.table}
		partTables := map[string]bool{m.table: d.partModel(m)}
		if m.kind == kindEventSourced {
			ev, ar, sn := esEventsTable(m), esArchiveTable(m), esSnapsTable(m)
			tables = append(tables, ev, ar, sn)
			partTables[ev] = d.dial.partitionClause() != "" && m.opts.geoMode != geoGlobal
			partTables[ar] = d.partModel(m)
			partTables[sn] = d.partModel(m)
		}
		for _, t := range tables {
			cols, err := d.dial.tableColumns(d.q(), t)
			if err != nil {
				return err
			}
			if len(cols) == 0 {
				continue
			}
			n, err := d.countGeoRows(ctx, t, name)
			if err != nil {
				return err
			}
			if n > 0 {
				withData = append(withData, fmt.Sprintf("%s (%d)", t, n))
				continue
			}
			if partTables[t] {
				drop = append(drop, geoPartName(t, name))
			}
		}
	}
	if len(withData) > 0 {
		return fmt.Errorf("orm: Region %s hält noch Daten: %s — vorher mit MoveTenant/SetGeo umziehen: %w",
			name, strings.Join(withData, ", "), ErrRegionHasData)
	}
	for _, t := range drop {
		if _, err := d.q().ExecContext(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %q`, t)); err != nil {
			return fmt.Errorf("orm: Partition %s entfernen: %w", t, err)
		}
	}
	_, err := d.q().ExecContext(ctx,
		`UPDATE ormpp_geo_regions SET status = 'removed' WHERE name = ?`, name)
	return err
}

// --- Umzug ---

// geoFilter schränkt einen Umzug ein: auf einen Mandanten oder auf einen
// einzelnen Datensatz.
type geoFilter struct {
	cond string // SQL-Bedingung mit ?-Platzhaltern, ohne führendes AND
	args []any
}

// MoveTenant verlegt sämtliche Daten eines Mandanten in eine andere
// Region: CRUD-Zeilen, ES-Read-Models, Event-Log und Archiv. Auf
// partitionierten Backends wandern die Zeilen dabei physisch in die
// Partition der Zielregion, auf kollabierten Backends bleibt es beim
// Spaltenwert (Verhaltensgleichheit).
//
// Der Umzug läuft batchweise und ist idempotent: nach einem Abbruch setzt
// ein erneuter Aufruf ihn fort. GeoGlobal-Modelle bleiben unberührt —
// sie sind per Definition in allen Regionen vorhanden.
func (d *DB) MoveTenant(ctx context.Context, tenant ID, toRegion string) error {
	if !d.migrated {
		return fmt.Errorf("orm: Migrate muss vor MoveTenant laufen")
	}
	if tenant.IsZero() {
		return ErrNoTenant
	}
	if !d.validGeo(toRegion) {
		return fmt.Errorf("%w: %q", ErrRegionNotActive, toRegion)
	}
	if err := d.tenants.verify(tenant); err != nil {
		return err
	}
	f := geoFilter{cond: `"tenant_id" = ?`, args: []any{tenant.String()}}
	for _, m := range d.reg.ordered {
		if !m.tenanted() || m.opts.geoMode == geoGlobal {
			continue
		}
		if err := d.moveModel(ctx, d, m, f, toRegion); err != nil {
			return fmt.Errorf("orm: %s nach %s verlegen: %w", m.name, toRegion, err)
		}
	}
	return nil
}

// moveModel zieht die vom Filter erfassten Zeilen eines Models um: bei ES
// zuerst den Event-Log (mit Neuvergabe der Geo-Sequenz), dann Archiv,
// Snapshots und Read-Model. Auf partitionierten Backends bewegt das
// UPDATE des Partitionsschlüssels die Zeilen physisch (Row Movement).
func (d *DB) moveModel(ctx context.Context, h Handle, m *model, f geoFilter, to string) error {
	if m.kind == kindEventSourced {
		if err := d.moveEvents(ctx, h, esEventsTable(m), f, to); err != nil {
			return err
		}
		for _, t := range []string{esArchiveTable(m), esSnapsTable(m)} {
			if err := moveGeoColumn(ctx, h, t, f, to); err != nil {
				return err
			}
		}
	}
	return moveGeoColumn(ctx, h, m.table, f, to)
}

func moveGeoColumn(ctx context.Context, h Handle, table string, f geoFilter, to string) error {
	args := append([]any{to}, f.args...)
	_, err := h.q().ExecContext(ctx, fmt.Sprintf(
		`UPDATE %q SET "geo" = ? WHERE %s AND "geo" <> ?`, table, f.cond),
		append(args, to)...)
	return err
}

// moveEvents hängt Event-Zeilen in die Zielregion um. Die Geo-Sequenz ist
// pro Region monoton, deshalb bekommen die Zeilen am Ziel neue seq-Werte
// am dortigen Ende — mit den alten würden sie mit bestehenden Events der
// Zielregion kollidieren. Projektionen der Zielregion sehen sie danach als
// Nachzügler und wenden sie erneut an (idempotent, at-least-once).
//
// Läuft der Aufruf bereits in einer Transaktion, bleibt er darin: eine
// eigene Transaktion aufzumachen wäre auf SQLite (eine Schreibverbindung)
// ein Deadlock und anderswo ein Schreibzugriff am Aufrufer vorbei.
func (d *DB) moveEvents(ctx context.Context, h Handle, ev string, f geoFilter, to string) error {
	for {
		var n int
		var err error
		if h.inTx() {
			n, err = moveEventBatch(ctx, h, ev, f, to)
		} else {
			err = d.Tx(ctx, func(tx Tx) error {
				n, err = moveEventBatch(ctx, tx, ev, f, to)
				return err
			})
		}
		if err != nil || n == 0 {
			return err
		}
	}
}

func moveEventBatch(ctx context.Context, h Handle, ev string, f geoFilter, to string) (int, error) {
	type eventKey struct {
		agg  string
		aseq int64
		geo  string
	}
	rows, err := h.q().QueryContext(ctx, fmt.Sprintf(
		`SELECT "aggregate_id", "aggregate_seq", "geo" FROM %q
		 WHERE %s AND "geo" <> ? ORDER BY "geo", "seq" LIMIT %d`, ev, f.cond, geoMoveBatch),
		append(append([]any{}, f.args...), to)...)
	if err != nil {
		return 0, err
	}
	var batch []eventKey
	for rows.Next() {
		var k eventKey
		if err := rows.Scan(&k.agg, &k.aseq, &k.geo); err != nil {
			rows.Close()
			return 0, err
		}
		batch = append(batch, k)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(batch) == 0 {
		return 0, nil
	}
	var top int64
	if err := h.q().QueryRowContext(ctx, fmt.Sprintf(
		`SELECT COALESCE(MAX("seq"), 0) FROM %q WHERE "geo" = ?`, ev), to).Scan(&top); err != nil {
		return 0, err
	}
	upd := fmt.Sprintf(`UPDATE %q SET "geo" = ?, "seq" = ?
		WHERE "aggregate_id" = ? AND "aggregate_seq" = ? AND "geo" = ?`, ev)
	var moved int
	for i, k := range batch {
		res, err := h.q().ExecContext(ctx, upd, to, top+int64(i)+1, k.agg, k.aseq, k.geo)
		if err != nil {
			return 0, err
		}
		n, _ := res.RowsAffected()
		moved += int(n)
	}
	// Ohne diese Prüfung liefe die Schleife endlos, wenn ein Nebenläufiger
	// die Zeilen zwischen SELECT und UPDATE verändert hat.
	if moved == 0 {
		return 0, fmt.Errorf("orm: %s: %d Zeilen zum Umzug gefunden, keine verändert — nebenläufige Änderung?", ev, len(batch))
	}
	return moved, nil
}
