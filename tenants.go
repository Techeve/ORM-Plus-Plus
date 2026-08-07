package orm

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"sync"
	"time"
)

// TenantInfo beschreibt einen Tenant im eingebauten Register.
type TenantInfo struct {
	ID        ID
	Name      string
	Status    string // "active" | "archived" | "importing" (Import läuft oder brach ab)
	CreatedAt time.Time
}

// TenantRegistry ist das eingebaute Tenant-Register (ormpp_tenants).
// Jeder Insert wird gegen dieses Register verifiziert (ErrUnknownTenant).
type TenantRegistry struct {
	d *DB

	mu    sync.RWMutex
	cache map[ID]string // tenant_id → status
}

func newTenantRegistry(d *DB) *TenantRegistry {
	return &TenantRegistry{d: d, cache: map[ID]string{}}
}

// bootstrap legt den SingleTenant an und lädt den Verifikations-Cache.
func (t *TenantRegistry) bootstrap(ctx context.Context) error {
	_, err := t.d.q().ExecContext(ctx, `
		INSERT INTO ormpp_tenants (tenant_id, name, status, created_at)
		VALUES (?, 'single', 'active', ?) ON CONFLICT (tenant_id) DO NOTHING`,
		SingleTenant.String(), time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		return fmt.Errorf("orm: SingleTenant anlegen: %w", err)
	}
	return t.reload(ctx)
}

// tenantSelect liefert Tenant-Zeilen samt abgeleitetem Status: ein
// laufender oder abgebrochener Import überschreibt ihn mit "importing".
const tenantSelect = `SELECT t.tenant_id, t.name,
		CASE WHEN i.tenant_id IS NULL THEN t.status ELSE 'importing' END, t.created_at
	FROM ormpp_tenants t LEFT JOIN ormpp_tenant_imports i ON i.tenant_id = t.tenant_id`

func (t *TenantRegistry) reload(ctx context.Context) error {
	rows, err := t.d.q().QueryContext(ctx, `SELECT t.tenant_id,
			CASE WHEN i.tenant_id IS NULL THEN t.status ELSE 'importing' END
		FROM ormpp_tenants t LEFT JOIN ormpp_tenant_imports i ON i.tenant_id = t.tenant_id`)
	if err != nil {
		return err
	}
	defer rows.Close()
	fresh := map[ID]string{}
	for rows.Next() {
		var id ID
		var status string
		if err := rows.Scan(&id, &status); err != nil {
			return err
		}
		fresh[id] = status
	}
	if err := rows.Err(); err != nil {
		return err
	}
	t.mu.Lock()
	t.cache = fresh
	t.mu.Unlock()
	return nil
}

// verify prüft eine Tenant-ID gegen den Cache (Insert-Verifikation).
func (t *TenantRegistry) verify(tenant ID) error {
	t.mu.RLock()
	status, ok := t.cache[tenant]
	t.mu.RUnlock()
	switch {
	case ok && status == "active":
		return nil
	case ok && status == "importing":
		// Ein abgebrochener Import lässt den Status stehen: der Tenant ist
		// erkennbar unvollständig statt still halb gefüllt.
		return fmt.Errorf("%w: %s", ErrImportIncomplete, tenant)
	default:
		return fmt.Errorf("%w: %s", ErrUnknownTenant, tenant)
	}
}

// Create legt einen neuen Tenant an.
func (t *TenantRegistry) Create(ctx context.Context, info TenantInfo) (TenantInfo, error) {
	if info.ID.IsZero() {
		info.ID = NewID()
	}
	info.Status = "active"
	info.CreatedAt = time.Now().UTC()
	_, err := t.d.q().ExecContext(ctx, `
		INSERT INTO ormpp_tenants (tenant_id, name, status, created_at) VALUES (?, ?, ?, ?)`,
		info.ID.String(), info.Name, info.Status, info.CreatedAt.Format(time.RFC3339Nano))
	if err != nil {
		return TenantInfo{}, fmt.Errorf("orm: Tenant anlegen: %w", err)
	}
	t.mu.Lock()
	t.cache[info.ID] = "active"
	t.mu.Unlock()
	return info, nil
}

// Get liest einen Tenant.
func (t *TenantRegistry) Get(ctx context.Context, id ID) (TenantInfo, error) {
	row := t.d.q().QueryRowContext(ctx, tenantSelect+` WHERE t.tenant_id = ?`, id.String())
	return scanTenant(row)
}

// List liefert alle Tenants.
func (t *TenantRegistry) List(ctx context.Context) ([]TenantInfo, error) {
	rows, err := t.d.q().QueryContext(ctx, tenantSelect+` ORDER BY t.created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []TenantInfo
	for rows.Next() {
		info, err := scanTenant(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, info)
	}
	return out, rows.Err()
}

// Archive archiviert einen Tenant: neue Schreibvorgänge werden blockiert,
// Bestandsdaten bleiben lesbar. Kein Hard-Delete.
func (t *TenantRegistry) Archive(ctx context.Context, id ID) error {
	res, err := t.d.q().ExecContext(ctx,
		`UPDATE ormpp_tenants SET status = 'archived' WHERE tenant_id = ?`, id.String())
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	t.mu.Lock()
	t.cache[id] = "archived"
	t.mu.Unlock()
	return nil
}

// Export schreibt den vollständigen Datenauszug eines Tenants als JSON
// Lines: eine Kopfzeile, dann je Zeile ein Datensatz — alle tenant-
// gebundenen Modelle, bei ES-Modellen zusätzlich Events (als CloudEvents,
// inkl. Archiv) und Snapshots. Verschlüsselte Felder werden entschlüsselt
// (DSGVO-Auskunft). Auch archivierte Tenants sind exportierbar.
func (t *TenantRegistry) Export(ctx context.Context, id ID, w io.Writer) error {
	d := t.d
	if !d.migrated {
		return fmt.Errorf("orm: Migrate muss vor Export laufen")
	}
	info, err := t.Get(ctx, id)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(w)
	// Schemastand in die Kopfzeile: ohne ihn kann ein Import nicht wissen,
	// von welchem Stand der Strom kommt (Zusatzfelder, alte Leser ignorieren
	// sie). Modelle topologisch sortiert (Referenzziele zuerst), damit der
	// Strom in seiner eigenen Reihenfolge importierbar ist.
	if err := enc.Encode(map[string]any{
		"type": "tenant", "data": info,
		"schema_version": d.schemaVersion,
		"models":         d.reg.checksum(),
		"exported_at":    nowUTC().Format(time.RFC3339Nano),
	}); err != nil {
		return err
	}
	ordered, err := d.reg.sortedByDeps()
	if err != nil {
		return err
	}
	for _, m := range ordered {
		if !m.tenanted() {
			continue
		}
		if err := d.exportRows(ctx, enc, m, id); err != nil {
			return err
		}
		if m.kind == kindEventSourced {
			if err := d.exportEvents(ctx, enc, m, id); err != nil {
				return err
			}
			if err := d.exportSnapshots(ctx, enc, m, id); err != nil {
				return err
			}
		}
	}
	// Schlusszeile: ohne sie ist ein abgeschnittener Strom von einem
	// vollständigen nicht zu unterscheiden — ein halb geschriebener
	// Sicherungspunkt sähe beim Zurückspielen aus wie ein ganzer.
	return enc.Encode(map[string]any{"type": "end"})
}

// exportRows schreibt die Zeilen eines Models (CRUD oder ES-Read-Model).
func (d *DB) exportRows(ctx context.Context, enc *json.Encoder, m *model, tenant ID) error {
	query := fmt.Sprintf("SELECT %s FROM %q WHERE tenant_id = ? ORDER BY %q",
		selectList(m), m.table, m.pkColumn())
	rows, err := d.qr().QueryContext(ctx, query, tenant.String())
	if err != nil {
		return err
	}
	defer rows.Close()
	n := len(m.fields)
	es := m.kind == kindEventSourced
	if es {
		n += 2 // id, aggregate_seq (selectList)
	}
	for rows.Next() {
		raws := make([]any, n)
		ptrs := make([]any, n)
		for i := range raws {
			ptrs[i] = &raws[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return err
		}
		inst := reflect.New(m.goType)
		for i, f := range m.fields {
			if err := decodeField(d, f, inst.Elem().FieldByIndex(f.index), raws[i]); err != nil {
				return err
			}
		}
		rec := map[string]any{"type": "row", "model": m.name, "data": inst.Interface()}
		if es {
			var aggID ID
			if err := aggID.Scan(raws[len(m.fields)]); err != nil {
				return err
			}
			seq, _ := rawInt(raws[len(m.fields)+1])
			rec["id"] = aggID.String()
			rec["aggregate_seq"] = seq
		}
		if err := enc.Encode(rec); err != nil {
			return err
		}
	}
	return rows.Err()
}

// exportEvents schreibt den Event-Strom (Hot + Archiv) als CloudEvents-JSON.
func (d *DB) exportEvents(ctx context.Context, enc *json.Encoder, m *model, tenant ID) error {
	query := fmt.Sprintf(`SELECT %s FROM %s WHERE tenant_id = ? ORDER BY aggregate_id, aggregate_seq`,
		esEventSelect(m), esEventsFrom(m, true))
	rows, err := d.qr().QueryContext(ctx, query, tenant.String())
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		r, err := scanEventRow(m, rows)
		if err != nil {
			return err
		}
		ce := d.cloudEvent(m, r)
		var data any = ce.Data
		if json.Valid(ce.Data) {
			data = json.RawMessage(ce.Data)
		}
		// CloudEvents-1.0-JSON-Format (Attribute kleingeschrieben).
		if err := enc.Encode(map[string]any{"type": "event", "model": m.name, "data": map[string]any{
			"specversion": ce.SpecVersion, "id": ce.ID, "source": ce.Source, "type": ce.Type,
			"subject": ce.Subject, "time": ce.Time, "datacontenttype": ce.DataContentType,
			"data": data, "tenant": ce.Tenant.String(), "geo": ce.Geo,
			"sequence": ce.Sequence, "aggregateseq": ce.AggregateSeq,
		}}); err != nil {
			return err
		}
	}
	return rows.Err()
}

// exportSnapshots schreibt die Snapshots eines ES-Models.
func (d *DB) exportSnapshots(ctx context.Context, enc *json.Encoder, m *model, tenant ID) error {
	query := fmt.Sprintf(`SELECT aggregate_id, aggregate_seq, taken_at, state FROM %q
		WHERE tenant_id = ? ORDER BY aggregate_id, aggregate_seq`, esSnapsTable(m))
	rows, err := d.qr().QueryContext(ctx, query, tenant.String())
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var aggID, taken string
		var seq int64
		var state []byte
		if err := rows.Scan(&aggID, &seq, &taken, &state); err != nil {
			return err
		}
		var st any = state
		if json.Valid(state) {
			st = json.RawMessage(state)
		}
		if err := enc.Encode(map[string]any{"type": "snapshot", "model": m.name, "data": map[string]any{
			"aggregate_id": aggID, "aggregate_seq": seq, "taken_at": taken, "state": st,
		}}); err != nil {
			return err
		}
	}
	return rows.Err()
}

// Purge löscht ALLE Daten eines Tenants physisch — über alle tenant-
// gebundenen Tabellen, Event-Logs, Archive und Snapshots hinweg, inklusive
// der Alt-Tabellen einer laufenden Migration und der Tenant-Zeile selbst
// (Recht auf Vergessenwerden). Zweistufig: der Tenant muss archiviert sein
// (sonst ErrTenantNotArchived); der Vorgang läuft atomar und wird in
// ormpp_schema_history auditiert.
func (t *TenantRegistry) Purge(ctx context.Context, id ID) error {
	d := t.d
	if !d.migrated {
		return fmt.Errorf("orm: Migrate muss vor Purge laufen")
	}
	info, err := t.Get(ctx, id)
	if err != nil {
		return err
	}
	if info.Status != "archived" {
		return ErrTenantNotArchived
	}
	tid := id.String()

	err = d.Tx(ctx, func(tx Tx) error {
		// Löschreihenfolge: Abhängige vor Zielen (FK-sicher).
		if err := d.deleteTenantData(ctx, tx, id); err != nil {
			return err
		}
		// Audit-Eintrag, dann die Tenant-Zeile selbst (Name ist personenbezogen).
		if _, err := tx.q().ExecContext(ctx, `
			INSERT INTO ormpp_schema_history (version, phase_from, phase_to, applied_at, applied_by)
			VALUES (0, 'tenant-purge', ?, ?, ?)`,
			tid, nowUTC().Format(time.RFC3339Nano), d.instanceID.String()); err != nil {
			return err
		}
		_, err := tx.q().ExecContext(ctx, `DELETE FROM ormpp_tenants WHERE tenant_id = ?`, tid)
		return err
	})
	if err != nil {
		return err
	}
	t.mu.Lock()
	delete(t.cache, id)
	t.mu.Unlock()
	return nil
}

type rowScanner interface{ Scan(dest ...any) error }

func scanTenant(r rowScanner) (TenantInfo, error) {
	var info TenantInfo
	var created string
	if err := r.Scan(&info.ID, &info.Name, &info.Status, &created); err != nil {
		if err == sql.ErrNoRows {
			return TenantInfo{}, ErrNotFound
		}
		return TenantInfo{}, err
	}
	ts, err := time.Parse(time.RFC3339Nano, created)
	if err != nil {
		return TenantInfo{}, err
	}
	info.CreatedAt = ts
	return info, nil
}
