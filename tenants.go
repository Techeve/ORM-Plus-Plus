package orm

import (
	"context"
	"database/sql"
	"fmt"
	"io"
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
	_, err := t.d.sql.ExecContext(ctx, `
		INSERT INTO ormpp_tenants (tenant_id, name, status, created_at)
		VALUES (?, 'single', 'active', ?) ON CONFLICT (tenant_id) DO NOTHING`,
		SingleTenant.String(), time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		return fmt.Errorf("orm: SingleTenant anlegen: %w", err)
	}
	return t.reload(ctx)
}

func (t *TenantRegistry) reload(ctx context.Context) error {
	rows, err := t.d.sql.QueryContext(ctx, `SELECT tenant_id, status FROM ormpp_tenants`)
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
	_, err := t.d.sql.ExecContext(ctx, `
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
	row := t.d.sql.QueryRowContext(ctx,
		`SELECT tenant_id, name, status, created_at FROM ormpp_tenants WHERE tenant_id = ?`, id.String())
	return scanTenant(row)
}

// List liefert alle Tenants.
func (t *TenantRegistry) List(ctx context.Context) ([]TenantInfo, error) {
	rows, err := t.d.sql.QueryContext(ctx,
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
	res, err := t.d.sql.ExecContext(ctx,
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

// Export schreibt den vollständigen Datenauszug eines Tenants (Phase 5).
func (t *TenantRegistry) Export(ctx context.Context, id ID, w io.Writer) error {
	return fmt.Errorf("orm: Tenants().Export ist noch nicht implementiert (siehe TASK.md)")
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
	return fmt.Errorf("orm: Tenants().Purge ist noch nicht implementiert (siehe TASK.md)")
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
