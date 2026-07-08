package orm

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// instanceTTL: Instanzen ohne Heartbeat länger als dies gelten als beendet.
const instanceTTL = 60 * time.Second

// heartbeatEvery: Mindestabstand zwischen zwei Heartbeats des Workers.
const heartbeatEvery = 5 * time.Second

// registerInstance trägt diese Instanz ins Instanzregister ein bzw.
// aktualisiert ihre Schema-Version und den Heartbeat.
func (d *DB) registerInstance(ctx context.Context) error {
	now := nowUTC().Format(time.RFC3339Nano)
	_, err := d.q().ExecContext(ctx, `
		INSERT INTO ormpp_instances (instance_id, hostname, geo, migration_role, app_version, schema_version, started_at, last_heartbeat)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (instance_id) DO UPDATE SET
			schema_version = excluded.schema_version,
			app_version = excluded.app_version,
			last_heartbeat = excluded.last_heartbeat`,
		d.instanceID.String(), d.hostname, d.opts.instanceGeo, int(d.opts.migrationRole),
		d.opts.appVersion, d.schemaVersion, now, now)
	if err != nil {
		return fmt.Errorf("orm: Instanz registrieren: %w", err)
	}
	return nil
}

// heartbeat aktualisiert den Lebenszeichen-Stempel dieser Instanz.
func (d *DB) heartbeat(ctx context.Context) error {
	_, err := d.q().ExecContext(ctx,
		`UPDATE ormpp_instances SET last_heartbeat = ? WHERE instance_id = ?`,
		nowUTC().Format(time.RFC3339Nano), d.instanceID.String())
	return err
}

// deregisterInstance entfernt die Instanz aus dem Register und gibt ihre
// Leases frei (bei Close).
func (d *DB) deregisterInstance(ctx context.Context) {
	_, _ = d.q().ExecContext(ctx, `DELETE FROM ormpp_instances WHERE instance_id = ?`, d.instanceID.String())
	_, _ = d.q().ExecContext(ctx, `DELETE FROM ormpp_leases WHERE holder = ?`, d.instanceID.String())
}

// livingOlderInstance liefert eine lebende Instanz mit Schema-Version < version
// ("" wenn keine) — die Vorbedingung von FinalizeMigration.
func (d *DB) livingOlderInstance(ctx context.Context, version int) (string, int, error) {
	rows, err := d.q().QueryContext(ctx,
		`SELECT instance_id, schema_version, last_heartbeat FROM ormpp_instances WHERE schema_version < ?`, version)
	if err != nil {
		return "", 0, err
	}
	defer rows.Close()
	cutoff := nowUTC().Add(-instanceTTL)
	for rows.Next() {
		var id, beat string
		var v int
		if err := rows.Scan(&id, &v, &beat); err != nil {
			return "", 0, err
		}
		t, err := time.Parse(time.RFC3339Nano, beat)
		if err != nil {
			continue
		}
		if t.After(cutoff) {
			return id, v, nil
		}
	}
	return "", 0, rows.Err()
}

// --- Leases mit Fencing (ormpp_leases) ---
// Koordination läuft über Lease-Zeilen, nicht über Advisory-Locks —
// YugabyteDB-kompatibel; auf SQLite trivial korrekt (eine Schreib-Connection).

// acquireLease nimmt oder erneuert eine Lease. false, wenn ein anderer
// Halter sie noch gültig hält.
func (d *DB) acquireLease(ctx context.Context, name string, ttl time.Duration) (bool, error) {
	me := d.instanceID.String()
	acquired := false
	err := d.Tx(ctx, func(tx Tx) error {
		var holder, expires string
		var token int64
		err := tx.q().QueryRowContext(ctx,
			`SELECT holder, fencing_token, expires_at FROM ormpp_leases WHERE name = ?`, name).
			Scan(&holder, &token, &expires)
		now := nowUTC()
		until := now.Add(ttl).Format(time.RFC3339Nano)
		switch err {
		case sql.ErrNoRows:
			_, err := tx.q().ExecContext(ctx,
				`INSERT INTO ormpp_leases (name, holder, fencing_token, expires_at) VALUES (?, ?, 1, ?)`,
				name, me, until)
			acquired = err == nil
			return err
		case nil:
			if holder == me {
				_, err := tx.q().ExecContext(ctx,
					`UPDATE ormpp_leases SET expires_at = ? WHERE name = ?`, until, name)
				acquired = err == nil
				return err
			}
			exp, perr := time.Parse(time.RFC3339Nano, expires)
			if perr == nil && exp.After(now) {
				return nil // gültig bei fremdem Halter
			}
			_, err := tx.q().ExecContext(ctx,
				`UPDATE ormpp_leases SET holder = ?, fencing_token = ?, expires_at = ? WHERE name = ?`,
				me, token+1, until, name)
			acquired = err == nil
			return err
		default:
			return err
		}
	})
	return acquired, err
}

func (d *DB) releaseLease(ctx context.Context, name string) {
	_, _ = d.q().ExecContext(ctx,
		`DELETE FROM ormpp_leases WHERE name = ? AND holder = ?`, name, d.instanceID.String())
}
