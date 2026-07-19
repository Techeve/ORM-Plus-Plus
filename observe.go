package orm

import (
	"context"
	"fmt"
	"time"
)

// Observability zeigt dem Betreiber die physische Wahrheit — die einzige
// Stelle, an der Backends sich unterscheiden dürfen (auf SQLite erscheint
// ehrlich eine Region "local" mit einem Worker).

// MigrationStatus beschreibt den Zustand der laufenden (oder zuletzt
// abgeschlossenen) Migration.
type MigrationStatus struct {
	Phase          string
	CurrentVersion int
	TargetVersion  int // 0 = keine Migration unterwegs
	Geo            map[string]GeoProgress
}

// GeoProgress ist der Migrations-Fortschritt einer Region.
type GeoProgress struct {
	Percent float64 // erledigte / registrierte Backfill-Schritte
	Workers int     // lebende Instanzen mit MigrationWorker-Rolle
}

// MigrationStatus liefert Phase, Versionen und Fortschritt pro Region.
func (d *DB) MigrationStatus(ctx context.Context) (MigrationStatus, error) {
	st, err := d.readSchemaState(ctx)
	if err != nil {
		return MigrationStatus{}, err
	}
	ms := MigrationStatus{
		Phase:          st.phase,
		CurrentVersion: st.current,
		TargetVersion:  st.target,
		Geo:            map[string]GeoProgress{},
	}
	if st.target > 0 {
		rows, err := d.qr().QueryContext(ctx, `
			SELECT geo, COUNT(*), SUM(CASE WHEN state = 'done' THEN 1 ELSE 0 END)
			FROM ormpp_migration_progress WHERE version > ? AND version <= ? GROUP BY geo`,
			st.current, st.target)
		if err != nil {
			return MigrationStatus{}, err
		}
		for rows.Next() {
			var geo string
			var total, done int
			if err := rows.Scan(&geo, &total, &done); err != nil {
				rows.Close()
				return MigrationStatus{}, err
			}
			p := ms.Geo[geo]
			if total > 0 {
				p.Percent = 100 * float64(done) / float64(total)
			}
			ms.Geo[geo] = p
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return MigrationStatus{}, err
		}
		// Rein additive Migration (keine Schritte): Phase entscheidet.
		if len(ms.Geo) == 0 {
			p := GeoProgress{}
			if st.phase == phaseDualWrite || st.phase == phaseFinalizing {
				p.Percent = 100
			}
			ms.Geo["local"] = p
		}
	}
	// Lebende Migrations-Worker je Region.
	cutoff := nowUTC().Add(-instanceTTL).Format(time.RFC3339Nano)
	rows, err := d.qr().QueryContext(ctx, `
		SELECT geo, COUNT(*) FROM ormpp_instances
		WHERE migration_role = ? AND last_heartbeat > ? GROUP BY geo`,
		int(MigrationWorker), cutoff)
	if err != nil {
		return MigrationStatus{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var geo string
		var n int
		if err := rows.Scan(&geo, &n); err != nil {
			return MigrationStatus{}, err
		}
		p := ms.Geo[geo]
		p.Workers = n
		ms.Geo[geo] = p
	}
	return ms, rows.Err()
}

// Health ist der Betriebszustand des Clusters.
type Health struct {
	Instances   []InstanceInfo  // lebende und kürzlich gesehene Instanzen
	Projections []ProjectionLag // Rückstand je Konsument/Region
	Regions     []RegionInfo    // Topologie-Status
}

// InstanceInfo ist eine Zeile des Instanzregisters.
type InstanceInfo struct {
	ID            string
	Hostname      string
	Geo           string
	Role          Role
	AppVersion    string
	SchemaVersion int
	LastHeartbeat time.Time
	Alive         bool // Heartbeat innerhalb der TTL
}

// ProjectionLag ist der Rückstand eines Konsumenten hinter dem Event-Log.
type ProjectionLag struct {
	Consumer string // "projection:<tabelle>" oder "view:<name>"
	Geo      string
	Lag      int64 // Events hinter der Log-Spitze
}

// RegionInfo ist eine Region des Topologie-Registers.
type RegionInfo struct {
	Name      string
	Status    string
	Placement string
}

// Health liefert Instanzen, Projektions-Rückstände und Regionen.
func (d *DB) Health(ctx context.Context) (Health, error) {
	var h Health

	rows, err := d.qr().QueryContext(ctx, `
		SELECT instance_id, hostname, geo, migration_role, app_version, schema_version, last_heartbeat
		FROM ormpp_instances ORDER BY started_at`)
	if err != nil {
		return h, err
	}
	cutoff := nowUTC().Add(-instanceTTL)
	for rows.Next() {
		var i InstanceInfo
		var role int
		var beat string
		if err := rows.Scan(&i.ID, &i.Hostname, &i.Geo, &role, &i.AppVersion, &i.SchemaVersion, &beat); err != nil {
			rows.Close()
			return h, err
		}
		i.Role = Role(role)
		if t, err := time.Parse(time.RFC3339Nano, beat); err == nil {
			i.LastHeartbeat = t
			i.Alive = t.After(cutoff)
		}
		h.Instances = append(h.Instances, i)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return h, err
	}

	// Rückstand: Log-Spitze je Geo (Hot — Archiviertes ist per Definition
	// konsumiert) minus Checkpoint des Konsumenten.
	for _, m := range d.reg.ordered {
		if m.kind != kindEventSourced {
			continue
		}
		tops := map[string]int64{}
		rows, err := d.qr().QueryContext(ctx,
			fmt.Sprintf(`SELECT geo, MAX(seq) FROM %q GROUP BY geo`, esEventsTable(m)))
		if err != nil {
			return h, err
		}
		for rows.Next() {
			var geo string
			var top int64
			if err := rows.Scan(&geo, &top); err != nil {
				rows.Close()
				return h, err
			}
			tops[geo] = top
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return h, err
		}
		consumers := []string{"projection:" + m.table}
		d.reactorMu.Lock()
		for _, r := range d.reactors {
			if d.reg.models[r.typ] == m {
				consumers = append(consumers, "view:"+r.name)
			}
		}
		d.reactorMu.Unlock()
		for geo, top := range tops {
			for _, c := range consumers {
				cp, err := getCheckpoint(ctx, d.q(), c, geo)
				if err != nil {
					return h, err
				}
				h.Projections = append(h.Projections, ProjectionLag{Consumer: c, Geo: geo, Lag: max(0, top-cp)})
			}
		}
	}

	rrows, err := d.qr().QueryContext(ctx,
		`SELECT name, status, placement FROM ormpp_geo_regions ORDER BY name`)
	if err != nil {
		return h, err
	}
	defer rrows.Close()
	for rrows.Next() {
		var r RegionInfo
		if err := rrows.Scan(&r.Name, &r.Status, &r.Placement); err != nil {
			return h, err
		}
		h.Regions = append(h.Regions, r)
	}
	if err := rrows.Err(); err != nil {
		return h, err
	}
	// Ohne deklarierte Topologie: ehrlich die eine implizite Region.
	if len(h.Regions) == 0 {
		h.Regions = []RegionInfo{{Name: "local", Status: "active"}}
	}
	return h, nil
}
