package orm

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"time"
)

// Import spielt einen Export-Strom (Format von Export) zurück in einen
// Tenant. Damit wird aus der DSGVO-Auskunft eine Sicherung.
//
// Ersetzen, nicht mischen — und zwar mit erkennbarem Zwischenzustand:
// Der Tenant steht während des Imports auf Status "importing"; jeder
// Schreibzugriff scheitert in dieser Zeit mit ErrImportIncomplete. Bricht
// der Import ab, bleibt der Status stehen: der Tenant ist danach
// nachweislich unvollständig statt still halb gefüllt. Ein erneuter
// Import räumt den Rest weg und läuft von vorn — Wiederholen führt also
// zum korrekten Endstand.
//
// Der Zielzustand des Tenants entscheidet über den Einstieg:
//
//   - active und leer   → Import
//   - active mit Daten  → ErrTenantNotEmpty (bewusst kein stilles Überschreiben)
//   - archived          → Bestand wird ersetzt (Archive nimmt keine Schreibzugriffe an)
//   - importing         → Wiederaufnahme, Rest wird verworfen
//
// Weitere Zusagen:
//
//   - Verschlüsselte Felder liegen im Export im Klartext (Auskunft) und
//     werden mit dem AKTUELLEN Schlüssel der Zieldatenbank neu
//     verschlüsselt. Ein Import ist damit nebenbei der Weg für einen
//     Schlüsselwechsel.
//   - Geo: es gilt die Heimatregion des Ziels, nicht die des Exports —
//     dieselbe Semantik wie MoveTenant. Bei mehreren Regionen ist
//     orm.WithGeo Pflicht (ErrNoGeo), sonst zerstreute ein Rückspielen
//     die Daten wieder über die Regionen.
//   - Read-Models werden NICHT aus dem Strom übernommen, sondern aus den
//     importierten Events neu projiziert — sie sind abgeleiteter Zustand,
//     und nur so ist das Ergebnis garantiert konsistent.
//   - Events landen am Ende der Geo-Sequenz der Zielregion. Ein Append
//     nach dem Import setzt damit nahtlos fort, und Projektionen sehen
//     die importierten Events als Nachzügler (at-least-once).
//   - Der Vorgang wird in ormpp_schema_history auditiert (wie Purge).
func (t *TenantRegistry) Import(ctx context.Context, id ID, r io.Reader, opts ...ImportOption) error {
	d := t.d
	if !d.migrated {
		return fmt.Errorf("orm: Migrate muss vor Import laufen")
	}
	cfg := importCfg{}
	for _, o := range opts {
		o(&cfg)
	}
	info, err := t.Get(ctx, id)
	if err != nil {
		return err
	}
	geo, err := d.dataGeo(ctx)
	if err != nil {
		return err
	}
	// GeoFlexible: die Replikat-Liste kommt wie bei jedem anderen Schreiben
	// aus dem Context (der Export trägt sie nicht — sie ist Platzierung,
	// keine Nutzdaten).
	g, _ := geoFrom(ctx)
	replicas, err := d.replicasJSON(g)
	if err != nil {
		return err
	}

	switch info.Status {
	case "active":
		tbl, err := d.tenantHasData(ctx, id)
		if err != nil {
			return err
		}
		if tbl != "" {
			return fmt.Errorf("%w: %s hält noch Zeilen — Tenant vorher archivieren (Import ersetzt dann) oder purgen",
				ErrTenantNotEmpty, tbl)
		}
	case "archived", "importing":
		if err := d.wipeTenant(ctx, id); err != nil {
			return err
		}
	default:
		return fmt.Errorf("orm: Import: unerwarteter Tenant-Status %q", info.Status)
	}

	if err := t.beginImport(ctx, id); err != nil {
		return err
	}
	if err := t.runImport(ctx, id, geo, replicas, r, cfg); err != nil {
		return err
	}
	if err := t.finishImport(ctx, id); err != nil {
		return err
	}
	_, err = d.q().ExecContext(ctx, `
		INSERT INTO ormpp_schema_history (version, phase_from, phase_to, applied_at, applied_by)
		VALUES (?, 'tenant-import', ?, ?, ?)`,
		d.schemaVersion, id.String(), nowUTC().Format(time.RFC3339Nano), d.instanceID.String())
	return err
}

// ImportOption konfiguriert einen Import.
type ImportOption func(*importCfg)

type importCfg struct{ allowDrift bool }

// AllowSchemaDrift lässt einen Export zu, dessen Schemastand nicht dem
// aktuellen entspricht. Ohne diese Option lehnt Import ab
// (ErrExportSchemaMismatch) — ein 35 Tage alter Sicherungspunkt überlebt
// leicht zwei Releases, und Zeilen eines fremden Schemas still
// einzuspielen ist der teuerste aller Fehler.
//
// Mit der Option gilt: Event-Payloads laufen ohnehin durch die
// vorhandene Upcaster-Kette (die Typen müssen deklariert sein, sonst
// ErrUnknownEventType); Zeilen werden über die Feldnamen zugeordnet —
// entfallene Felder fallen weg, neue bleiben leer.
func AllowSchemaDrift() ImportOption { return func(c *importCfg) { c.allowDrift = true } }

const importBatch = 500

// importRecord ist eine Zeile des Export-Stroms.
type importRecord struct {
	Type  string          `json:"type"`
	Model string          `json:"model"`
	Data  json.RawMessage `json:"data"`

	// Kopfzeile (seit v1.2.0; ältere Exporte tragen sie nicht).
	SchemaVersion *int   `json:"schema_version"`
	Models        string `json:"models"`
}

type importEvent struct {
	ID           string          `json:"id"`
	Type         string          `json:"type"`
	Subject      string          `json:"subject"`
	Time         time.Time       `json:"time"`
	Data         json.RawMessage `json:"data"`
	AggregateSeq int64           `json:"aggregateseq"`
}

type importSnapshot struct {
	AggregateID  string          `json:"aggregate_id"`
	AggregateSeq int64           `json:"aggregate_seq"`
	TakenAt      string          `json:"taken_at"`
	State        json.RawMessage `json:"state"`
}

// pending sammelt einen Stapel, der in EINER Transaktion landet — der
// Strom wird nie ganz in den Speicher gelesen (Exporte großer Tenants
// sind hunderte MB).
type pending struct {
	rows   []pendingRow
	events []pendingEvent
	snaps  []pendingSnap
}

type pendingRow struct {
	m    *model
	cols []string
	vals []any
}

type pendingEvent struct {
	m        *model
	aggID    string
	aggSeq   int64
	eventID  string
	occurred string
	typeID   int64
	data     string
}

type pendingSnap struct {
	m *model
	s importSnapshot
}

func (p *pending) len() int { return len(p.rows) + len(p.events) + len(p.snaps) }

func (t *TenantRegistry) runImport(ctx context.Context, id ID, geo, replicas string, r io.Reader, cfg importCfg) error {
	d := t.d
	dec := json.NewDecoder(r)
	var p pending
	esSeen := map[*model]bool{}
	first := true
	// Nur Exporte ab v1.2.0 tragen eine Schlusszeile; ältere sind auf
	// Vollständigkeit nicht prüfbar.
	terminated, sawEnd := false, false

	for {
		var rec importRecord
		if err := dec.Decode(&rec); err == io.EOF {
			break
		} else if err != nil {
			return fmt.Errorf("orm: Import: Strom lesen: %w", err)
		}
		if first {
			if rec.Type != "tenant" {
				return fmt.Errorf("orm: Import: erste Zeile ist %q, erwartet die Kopfzeile \"tenant\"", rec.Type)
			}
			if err := d.checkExportSchema(rec, cfg); err != nil {
				return err
			}
			terminated = rec.SchemaVersion != nil
			first = false
			continue
		}
		switch {
		case rec.Type == "tenant":
			return fmt.Errorf("orm: Import: zweite Kopfzeile im Strom")
		case rec.Type == "end":
			sawEnd = true
			continue
		case sawEnd:
			return fmt.Errorf("orm: Import: Sätze nach der Schlusszeile")
		}

		m := d.reg.byName[rec.Model]
		if m == nil {
			return fmt.Errorf("orm: Import: Model %q ist nicht registriert", rec.Model)
		}
		if !m.tenanted() {
			return fmt.Errorf("orm: Import: Model %q ist TenantFree und gehört nicht in einen Tenant-Export", rec.Model)
		}
		switch rec.Type {
		case "row":
			// Read-Model-Zeilen sind abgeleitet: sie werden aus den
			// importierten Events neu projiziert, nicht übernommen.
			if m.kind == kindEventSourced {
				continue
			}
			pr, err := d.importRow(m, rec.Data, id, geo, replicas)
			if err != nil {
				return err
			}
			p.rows = append(p.rows, pr)
		case "event":
			pe, err := d.importEvent(m, rec.Data)
			if err != nil {
				return err
			}
			p.events = append(p.events, pe)
			esSeen[m] = true
		case "snapshot":
			var s importSnapshot
			if err := json.Unmarshal(rec.Data, &s); err != nil {
				return fmt.Errorf("orm: Import: Snapshot %s: %w", m.name, err)
			}
			p.snaps = append(p.snaps, pendingSnap{m: m, s: s})
			esSeen[m] = true
		default:
			return fmt.Errorf("orm: Import: unbekannter Satztyp %q", rec.Type)
		}

		if p.len() >= importBatch {
			if err := d.flushImport(ctx, &p, id, geo); err != nil {
				return err
			}
		}
	}
	if first {
		return fmt.Errorf("orm: Import: leerer Strom (keine Kopfzeile)")
	}
	if terminated && !sawEnd {
		return fmt.Errorf("%w: Schlusszeile fehlt — der Strom ist abgeschnitten", ErrImportIncomplete)
	}
	if err := d.flushImport(ctx, &p, id, geo); err != nil {
		return err
	}

	// Read-Models aus den importierten Events aufbauen.
	for m := range esSeen {
		if err := d.reprojectTenant(ctx, m, id, geo); err != nil {
			return fmt.Errorf("orm: Import: Read-Model %s aufbauen: %w", m.name, err)
		}
	}
	return nil
}

// checkExportSchema vergleicht den Schemastand des Exports mit dem
// aktuellen. Exporte ohne Kopfangabe (vor v1.2.0) gelten als unbekannt.
func (d *DB) checkExportSchema(rec importRecord, cfg importCfg) error {
	if cfg.allowDrift {
		return nil
	}
	if rec.SchemaVersion == nil {
		return fmt.Errorf("%w: Export ohne Schemaangabe (vor v1.2.0) — Stand nicht prüfbar, "+
			"mit orm.AllowSchemaDrift() bewusst zulassen", ErrExportSchemaMismatch)
	}
	if *rec.SchemaVersion != d.schemaVersion {
		return fmt.Errorf("%w: Export hat Schemaversion %d, diese Anlage %d",
			ErrExportSchemaMismatch, *rec.SchemaVersion, d.schemaVersion)
	}
	if sum := d.reg.checksum(); rec.Models != "" && rec.Models != sum {
		return fmt.Errorf("%w: Modelle weichen ab (Export %s, Anlage %s) bei gleicher Schemaversion %d",
			ErrExportSchemaMismatch, rec.Models, sum, d.schemaVersion)
	}
	return nil
}

// importRow dekodiert eine Zeile und kodiert sie für das Ziel — dabei
// werden encrypted-Felder mit dem aktuellen Schlüssel neu verschlüsselt.
func (d *DB) importRow(m *model, raw json.RawMessage, tenant ID, geo, replicas string) (pendingRow, error) {
	inst := reflect.New(m.goType)
	if err := json.Unmarshal(raw, inst.Interface()); err != nil {
		return pendingRow{}, fmt.Errorf("orm: Import: Zeile %s: %w", m.name, err)
	}
	rv := inst.Elem()
	cols := make([]string, 0, len(m.fields)+3)
	vals := make([]any, 0, len(m.fields)+3)
	for _, f := range m.fields {
		v, err := encodeField(d, f, rv.FieldByIndex(f.index))
		if err != nil {
			return pendingRow{}, fmt.Errorf("orm: Import: Zeile %s.%s: %w", m.name, f.name, err)
		}
		cols = append(cols, f.column)
		vals = append(vals, v)
	}
	cols = append(cols, "tenant_id", "geo")
	vals = append(vals, tenant.String(), geo)
	if m.opts.geoMode == geoFlexible {
		cols = append(cols, "geo_replicas")
		vals = append(vals, replicas)
	}
	return pendingRow{m: m, cols: cols, vals: vals}, nil
}

func (d *DB) importEvent(m *model, raw json.RawMessage) (pendingEvent, error) {
	var ev importEvent
	if err := json.Unmarshal(raw, &ev); err != nil {
		return pendingEvent{}, fmt.Errorf("orm: Import: Event %s: %w", m.name, err)
	}
	typeID, ok := d.esTypes.idOf(ev.Type)
	if !ok {
		return pendingEvent{}, fmt.Errorf("%w: %q (%s) — der Typ ist in dieser Anlage nicht deklariert",
			ErrUnknownEventType, ev.Type, m.name)
	}
	if ev.Subject == "" || ev.AggregateSeq < 1 {
		return pendingEvent{}, fmt.Errorf("orm: Import: Event %s ohne Aggregat-Bezug (subject=%q, aggregateseq=%d)",
			m.name, ev.Subject, ev.AggregateSeq)
	}
	return pendingEvent{
		m: m, aggID: ev.Subject, aggSeq: ev.AggregateSeq, eventID: ev.ID,
		occurred: ev.Time.UTC().Format(time.RFC3339Nano), typeID: typeID, data: string(ev.Data),
	}, nil
}

// flushImport schreibt einen Stapel in einer Transaktion. Die Geo-Sequenz
// der Events wird erst hier vergeben — am Ende der Zielregion, wie beim
// Umzug (siehe moveEvents).
func (d *DB) flushImport(ctx context.Context, p *pending, tenant ID, geo string) error {
	if p.len() == 0 {
		return nil
	}
	err := d.Tx(ctx, func(tx Tx) error {
		q := tx.q()
		for _, r := range p.rows {
			if _, err := q.ExecContext(ctx, insertSQL(r.m.table, r.cols), r.vals...); err != nil {
				return fmt.Errorf("orm: Import: %s einfügen: %w", r.m.name, err)
			}
		}
		tops := map[*model]int64{}
		for _, e := range p.events {
			top, ok := tops[e.m]
			if !ok {
				if err := q.QueryRowContext(ctx, e.m.es.sqlGeoSeq, geo).Scan(&top); err != nil {
					return err
				}
			}
			top++
			tops[e.m] = top
			args := []any{e.aggID, e.aggSeq, geo, top, e.eventID, e.occurred, e.typeID, e.data}
			if e.m.tenanted() {
				args = append(args, tenant.String())
			}
			if _, err := q.ExecContext(ctx, e.m.es.sqlInsert, args...); err != nil {
				return fmt.Errorf("orm: Import: Event %s einfügen: %w", e.m.name, err)
			}
		}
		for _, s := range p.snaps {
			cols := []string{"aggregate_id", "aggregate_seq", "geo", "taken_at", "state"}
			vals := []any{s.s.AggregateID, s.s.AggregateSeq, geo, s.s.TakenAt, []byte(s.s.State)}
			if s.m.tenanted() {
				cols = append(cols, "tenant_id")
				vals = append(vals, tenant.String())
			}
			if _, err := q.ExecContext(ctx, insertSQL(esSnapsTable(s.m), cols), vals...); err != nil {
				return fmt.Errorf("orm: Import: Snapshot %s einfügen: %w", s.m.name, err)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	p.rows, p.events, p.snaps = nil, nil, nil
	return nil
}

// reprojectTenant baut das Read-Model eines ES-Models für einen Tenant aus
// den Events auf. Erst die Aggregat-Liste einsammeln, dann schreiben: der
// Lese-Cursor darf nicht offen bleiben, während geschrieben wird (SQLite
// hat genau eine Schreibverbindung).
func (d *DB) reprojectTenant(ctx context.Context, m *model, tenant ID, geo string) error {
	query := fmt.Sprintf(`SELECT DISTINCT "aggregate_id" FROM %q
		WHERE "tenant_id" = ? AND "aggregate_id" > ? ORDER BY "aggregate_id" LIMIT %d`,
		esEventsTable(m), importBatch)
	cursor := ""
	for {
		rows, err := d.qr().QueryContext(ctx, query, tenant.String(), cursor)
		if err != nil {
			return err
		}
		var ids []string
		for rows.Next() {
			var s string
			if err := rows.Scan(&s); err != nil {
				rows.Close()
				return err
			}
			ids = append(ids, s)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		if len(ids) == 0 {
			return nil
		}
		cursor = ids[len(ids)-1]
		err = d.Tx(ctx, func(tx Tx) error {
			for _, aggID := range ids {
				if err := d.projectAggregate(ctx, tx.q(), m, aggID, tenant, geo); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
}

// --- Tenant-Zustand ---

// beginImport markiert den Tenant als „im Import" — die Marke bleibt bei
// einem Abbruch stehen und macht ihn erkennbar unvollständig. Der
// Tenant-Status geht zusätzlich auf archived, damit auch eine Instanz mit
// altem Cache keine Schreibzugriffe durchlässt.
func (t *TenantRegistry) beginImport(ctx context.Context, id ID) error {
	if _, err := t.d.q().ExecContext(ctx,
		`UPDATE ormpp_tenants SET status = 'archived' WHERE tenant_id = ?`, id.String()); err != nil {
		return err
	}
	if _, err := t.d.q().ExecContext(ctx, `
		INSERT INTO ormpp_tenant_imports (tenant_id, started_at, started_by) VALUES (?, ?, ?)
		ON CONFLICT (tenant_id) DO UPDATE SET started_at = excluded.started_at, started_by = excluded.started_by`,
		id.String(), nowUTC().Format(time.RFC3339Nano), t.d.instanceID.String()); err != nil {
		return err
	}
	t.mu.Lock()
	t.cache[id] = "importing"
	t.mu.Unlock()
	return nil
}

func (t *TenantRegistry) finishImport(ctx context.Context, id ID) error {
	if _, err := t.d.q().ExecContext(ctx,
		`DELETE FROM ormpp_tenant_imports WHERE tenant_id = ?`, id.String()); err != nil {
		return err
	}
	if _, err := t.d.q().ExecContext(ctx,
		`UPDATE ormpp_tenants SET status = 'active' WHERE tenant_id = ?`, id.String()); err != nil {
		return err
	}
	t.mu.Lock()
	t.cache[id] = "active"
	t.mu.Unlock()
	return nil
}

// tenantHasData liefert die erste Tabelle, die noch Zeilen des Tenants hält.
func (d *DB) tenantHasData(ctx context.Context, id ID) (string, error) {
	for _, tbl := range d.tenantTables() {
		var n int
		if err := d.qr().QueryRowContext(ctx,
			fmt.Sprintf(`SELECT COUNT(*) FROM %q WHERE "tenant_id" = ?`, tbl), id.String()).Scan(&n); err != nil {
			return "", err
		}
		if n > 0 {
			return tbl, nil
		}
	}
	return "", nil
}

// tenantTables listet alle Tabellen mit tenant_id — in Löschreihenfolge
// (Abhängige vor Zielen).
func (d *DB) tenantTables() []string {
	ordered, err := d.reg.sortedByDeps()
	if err != nil {
		ordered = d.reg.ordered
	}
	var out []string
	for i := len(ordered) - 1; i >= 0; i-- {
		m := ordered[i]
		if !m.tenanted() {
			continue
		}
		out = append(out, m.table)
		if m.kind == kindEventSourced {
			out = append(out, esEventsTable(m), esArchiveTable(m), esSnapsTable(m))
		}
	}
	return out
}

// wipeTenant löscht alle Daten eines Tenants, lässt die Tenant-Zeile aber
// stehen (anders als Purge, das auch den Namen entfernt).
func (d *DB) wipeTenant(ctx context.Context, id ID) error {
	return d.Tx(ctx, func(tx Tx) error { return d.deleteTenantData(ctx, tx, id) })
}

// deleteTenantData löscht die Nutzdaten eines Tenants inklusive der
// Alt-Tabellen einer laufenden Migration.
func (d *DB) deleteTenantData(ctx context.Context, tx Tx, id ID) error {
	tid := id.String()
	if _, err := tx.q().ExecContext(ctx,
		`DELETE FROM ormpp_tenant_imports WHERE tenant_id = ?`, tid); err != nil {
		return err
	}
	for _, tbl := range d.tenantTables() {
		if _, err := tx.q().ExecContext(ctx,
			fmt.Sprintf("DELETE FROM %q WHERE tenant_id = ?", tbl), tid); err != nil {
			return fmt.Errorf("orm: %s leeren: %w", tbl, err)
		}
	}
	d.dwMu.Lock()
	active := d.activeReplace
	d.dwMu.Unlock()
	for tbl, cr := range active {
		if !cr.oldM.tenanted() {
			continue
		}
		if _, err := tx.q().ExecContext(ctx,
			fmt.Sprintf("DELETE FROM %q WHERE tenant_id = ?", tbl), tid); err != nil {
			return fmt.Errorf("orm: Alt-Tabelle %s leeren: %w", tbl, err)
		}
	}
	return nil
}
