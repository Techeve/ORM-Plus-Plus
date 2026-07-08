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
	Status    string // "active" | "archived"
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

func (t *TenantRegistry) reload(ctx context.Context) error {
	rows, err := t.d.q().QueryContext(ctx, `SELECT tenant_id, status FROM ormpp_tenants`)
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
	if !ok || status != "active" {
		return fmt.Errorf("%w: %s", ErrUnknownTenant, tenant)
	}
	return nil
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
	row := t.d.q().QueryRowContext(ctx,
		`SELECT tenant_id, name, status, created_at FROM ormpp_tenants WHERE tenant_id = ?`, id.String())
	return scanTenant(row)
}

// List liefert alle Tenants.
func (t *TenantRegistry) List(ctx context.Context) ([]TenantInfo, error) {
	rows, err := t.d.q().QueryContext(ctx,
		`SELECT tenant_id, name, status, created_at FROM ormpp_tenants ORDER BY created_at`)
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
	if err := enc.Encode(map[string]any{"type": "tenant", "data": info}); err != nil {
		return err
	}
	for _, m := range d.reg.ordered {
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
	return nil
}

// exportRows schreibt die Zeilen eines Models (CRUD oder ES-Read-Model).
func (d *DB) exportRows(ctx context.Context, enc *json.Encoder, m *model, tenant ID) error {
	query := fmt.Sprintf("SELECT %s FROM %q WHERE tenant_id = ? ORDER BY %q",
		selectList(m), m.table, m.pkColumn())
	rows, err := d.q().QueryContext(ctx, query, tenant.String())
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
	rows, err := d.q().QueryContext(ctx, query, tenant.String())
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
	rows, err := d.q().QueryContext(ctx, query, tenant.String())
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

// Purge löscht alle Daten eines archivierten Tenants physisch (Phase 5).
func (t *TenantRegistry) Purge(ctx context.Context, id ID) error {
	info, err := t.Get(ctx, id)
	if err != nil {
		return err
	}
	if info.Status != "archived" {
		return ErrTenantNotArchived
	}
	return fmt.Errorf("orm: Tenants().Purge ist noch nicht implementiert (siehe doc/TASK.md)")
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
