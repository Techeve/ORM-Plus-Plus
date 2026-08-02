package orm

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"sync"
	"time"
)

type ctxType = context.Context

func bgCtx() context.Context { return context.Background() }

// Handle ist entweder *DB oder eine laufende Transaktion (Tx) —
// Repositories arbeiten auf beidem identisch.
type Handle interface {
	db() *DB
	q() queryer
	inTx() bool
}

// DB ist die zentrale ORM++-Instanz einer Anwendung.
type DB struct {
	sql     *sql.DB
	sqlRead *sql.DB // getrennter Lese-Pool (SQLite/WAL); nil = sql verwenden
	dial    dialect
	opts    openOptions
	reg     *registry
	tenants *TenantRegistry

	schemaVersion int
	regions       map[string]bool // deklarierte Topologie; leer = implizit "local"
	regionDecls   []RegionDecl    // inkl. Placement — persistiert in ormpp_geo_regions
	migrated      bool

	// Migration (Phase 3):
	instanceID    ID
	hostname      string
	migrations    map[int][]MigrationStep // Version → Schritte (MigrationTo)
	dwMu          sync.Mutex
	activeReplace map[string]*compiledReplace // Alt-Tabelle → Schritt (Dual-Write-Drain)
	lastBeat      time.Time                   // nur vom Worker-Goroutine benutzt
	leaseUntil    map[string]time.Time        // Lease-Gültigkeit im Speicher (nur Worker-Goroutine)

	// Event Sourcing (Phase 2):
	esTypes   *typeDict                   // Typ-Wörterbuch, geladen bei Migrate
	upcasters map[string]map[int]upcaster // Event-Name → fromVersion → Upcaster
	reactorMu sync.Mutex
	reactors  []*reactor
	watchMu   sync.Mutex
	watchers  map[*watcher]struct{}
	wake      chan struct{} // weckt den Worker nach Append

	workerCancel context.CancelFunc
	workerWG     sync.WaitGroup
}

func (d *DB) db() *DB    { return d }
func (d *DB) q() queryer { return dialq{q: d.sql, dial: d.dial} }
func (d *DB) inTx() bool { return false }

// qr liefert den Lese-Queryer: der getrennte Lese-Pool, wo vorhanden
// (SQLite/WAL), sonst der normale Pool. Nur für Reads AUSSERHALB von
// Transaktionen — in einer Tx müssen Reads die Tx-Verbindung nutzen.
func (d *DB) qr() queryer {
	if d.sqlRead != nil {
		return dialq{q: d.sqlRead, dial: d.dial}
	}
	return dialq{q: d.sql, dial: d.dial}
}

// readQ wählt den richtigen Lese-Queryer für ein Handle.
func readQ(h Handle) queryer {
	if h.inTx() {
		return h.q()
	}
	return h.db().qr()
}

// Tx ist eine laufende Transaktion.
type Tx interface {
	Handle
}

type txHandle struct {
	parent *DB
	tx     *sql.Tx
}

func (t *txHandle) db() *DB    { return t.parent }
func (t *txHandle) q() queryer { return dialq{q: t.tx, dial: t.parent.dial} }
func (t *txHandle) inTx() bool { return true }

// Open baut die Verbindung auf und initialisiert die Instanz.
// Schema-Änderungen macht erst Migrate.
func Open(driver Driver, opts ...OpenOption) (*DB, error) {
	o := openOptions{
		instanceGeo:         "local",
		defaultSnapshotEach: 100,
	}
	for _, fn := range opts {
		fn(&o)
	}
	sdb, dial, err := driver.connect()
	if err != nil {
		return nil, err
	}
	var rdb *sql.DB
	if rp, ok := driver.(readPooler); ok {
		if rdb, err = rp.connectRead(); err != nil {
			_ = sdb.Close()
			return nil, err
		}
	}
	host, _ := os.Hostname()
	d := &DB{
		sql:           sdb,
		sqlRead:       rdb,
		dial:          dial,
		opts:          o,
		reg:           newRegistry(),
		schemaVersion: 1,
		regions:       map[string]bool{},
		upcasters:     map[string]map[int]upcaster{},
		watchers:      map[*watcher]struct{}{},
		wake:          make(chan struct{}, 1),
		instanceID:    NewID(),
		hostname:      host,
		migrations:    map[int][]MigrationStep{},
		leaseUntil:    map[string]time.Time{},
	}
	d.tenants = newTenantRegistry(d)
	return d, nil
}

// Close stoppt die Worker, meldet die Instanz im Register ab (Leases werden
// freigegeben) und schließt den Verbindungspool.
func (d *DB) Close() error {
	if d.workerCancel != nil {
		d.workerCancel()
		d.workerWG.Wait()
		d.workerCancel = nil
	}
	if d.migrated {
		d.deregisterInstance(bgCtx())
	}
	if d.sqlRead != nil {
		_ = d.sqlRead.Close()
	}
	return d.sql.Close()
}

// Register deklariert ein Model. Validierungsfehler werden gesammelt und
// schlagen bei Migrate fehl (Register selbst hat laut API keinen Fehlerkanal).
func Register[T any](d *DB, mode ModelMode, opts ...ModelOption) {
	register(d.reg, reflect.TypeFor[T](), mode, opts...)
}

// SchemaVersion deklariert die Schema-Version, die diese App-Ausgabe erwartet.
func SchemaVersion(d *DB, v int) { d.schemaVersion = v }

// RegionDecl ist eine deklarierte Region.
type RegionDecl struct {
	name      string
	placement string
}

// RegionOption konfiguriert eine Region.
type RegionOption func(*RegionDecl)

// Placement ordnet die Region einem physischen Standort zu (YB-Tablespace).
func Placement(p string) RegionOption {
	return func(r *RegionDecl) { r.placement = p }
}

// Region deklariert eine Region der Topologie.
func Region(name string, opts ...RegionOption) RegionDecl {
	r := RegionDecl{name: name}
	for _, o := range opts {
		o(&r)
	}
	return r
}

// Topology deklariert die Regionen des Clusters. Auf SQLite/Single-Region-
// Backends kollabieren alle Regionen physisch auf eine — die Deklaration
// bleibt gültig und Daten-Geos werden gegen sie validiert.
func Topology(d *DB, regions ...RegionDecl) {
	for _, r := range regions {
		d.regions[r.name] = true
		d.regionDecls = append(d.regionDecls, r)
	}
}

// persistTopology schreibt die deklarierte Topologie ins Register
// ormpp_geo_regions (additiv; Lebenszyklus bootstrapping/draining folgt
// in Phase 5 — deklarierte Regionen sind hier sofort active).
func (d *DB) persistTopology(ctx context.Context) error {
	for _, r := range d.regionDecls {
		if _, err := d.q().ExecContext(ctx, `
			INSERT INTO ormpp_geo_regions (name, status, placement, topology_version, created_at)
			VALUES (?, 'active', ?, 1, ?)
			ON CONFLICT (name) DO UPDATE SET placement = excluded.placement`,
			r.name, r.placement, nowUTC().Format(time.RFC3339Nano)); err != nil {
			return fmt.Errorf("orm: Region %s registrieren: %w", r.name, err)
		}
	}
	return nil
}

// geoPartitioned: physische Geo-Partitionierung ist aktiv — das Backend
// partitioniert nativ UND eine Topologie ist deklariert. Steuert nur die
// physische Ablage (DDL, Conflict-Targets), nie Verhalten.
func (d *DB) geoPartitioned() bool {
	return d.dial.partitionClause() != "" && len(d.regions) > 0
}

// partModel: die Tabellen dieses Models sind physisch geo-partitioniert.
// GeoGlobal-Modelle sind per Definition in allen Regionen — unpartitioniert.
func (d *DB) partModel(m *model) bool {
	return d.geoPartitioned() && m.opts.geoMode != geoGlobal
}

// geoEngine: die Engine übernimmt Constraint-Semantik, die auf
// partitionierten Tabellen kein natives Pendant hat (FK-Ziel, Cross-Geo-
// Unique, Upsert-Zielwahl). Bewusst NUR an der Topologie festgemacht,
// nicht am Backend: SQLite kollabiert physisch, verhält sich aber
// identisch — sonst bräche die Verhaltensgleichheit.
func (d *DB) geoEngine(m *model) bool {
	return len(d.regions) > 0 && m.opts.geoMode != geoGlobal
}

// validGeo prüft ein Daten-Geo gegen die deklarierte Topologie.
func (d *DB) validGeo(geo string) bool {
	if len(d.regions) == 0 {
		return geo == "local"
	}
	return d.regions[geo]
}

// dataGeo löst das Daten-Geo aus dem Context auf (mit Topologie-Validierung).
func (d *DB) dataGeo(ctx context.Context) (string, error) {
	if g, ok := geoFrom(ctx); ok {
		if !d.validGeo(g.home) {
			return "", fmt.Errorf("%w: %q", ErrRegionNotActive, g.home)
		}
		return g.home, nil
	}
	return d.defaultGeo()
}

// defaultGeo liefert das Daten-Geo, wenn keins im Context steht.
func (d *DB) defaultGeo() (string, error) {
	if len(d.regions) == 0 {
		return "local", nil
	}
	if len(d.regions) == 1 {
		for r := range d.regions {
			return r, nil
		}
	}
	return "", ErrNoGeo
}

// StartWorkers startet die Hintergrund-Verarbeitung dieser Instanz:
// eingebaute Projektionen, OnEvent-Reaktoren und Snapshot-Erzeugung.
// Auf SQLite trivial ein Prozess (Lease-Koordination kommt mit Phase 3/4).
// ctx-Abbruch oder Close stoppt die Worker sauber.
func (d *DB) StartWorkers(ctx context.Context) error {
	if d.workerCancel != nil {
		return nil
	}
	wctx, cancel := context.WithCancel(ctx)
	d.workerCancel = cancel
	d.workerWG.Add(1)
	go d.workerLoop(wctx)
	return nil
}

// Tenants liefert das eingebaute Tenant-Register.
func (d *DB) Tenants() *TenantRegistry { return d.tenants }

// Tx führt fn in einer Transaktion aus; Fehler oder Panic ⇒ Rollback.
func (d *DB) Tx(ctx context.Context, fn func(tx Tx) error) error {
	stx, err := d.sql.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	th := &txHandle{parent: d, tx: stx}
	defer func() {
		if p := recover(); p != nil {
			_ = stx.Rollback()
			panic(p)
		}
	}()
	if err := fn(th); err != nil {
		_ = stx.Rollback()
		return err
	}
	return stx.Commit()
}

// Migrate validiert die Registry, bootstrapt Systemtabellen, registriert die
// Instanz und bringt das Schema auf die deklarierte Version: Erstinstallation
// direkt, gleiche Version als No-op (mit Drift-Prüfung), höhere Version über
// die Online-Zustandsmaschine expanding → backfill → dual-write. Der
// Abschluss (Contract) ist explizit: FinalizeMigration.
func (d *DB) Migrate(ctx context.Context, plans ...MigrationPlan) error {
	plan := MigrationPlan{BatchSize: 1000}
	if len(plans) > 0 {
		plan = plans[0]
		if plan.BatchSize <= 0 {
			plan.BatchSize = 1000
		}
	}
	if err := d.reg.resolve(); err != nil {
		return err
	}
	// encrypted-Felder: Provider ist Pflicht; ES-Modelle folgen später
	// (Event-Payloads/Snapshots — siehe doc/API.md §5.5).
	for _, m := range d.reg.ordered {
		for _, f := range m.fields {
			if !f.encrypted {
				continue
			}
			if d.opts.keys == nil {
				return fmt.Errorf("orm: %s.%s ist encrypted — orm.Encryption(KeyProvider) bei Open fehlt", m.name, f.name)
			}
			if m.kind == kindEventSourced {
				return fmt.Errorf("orm: %s.%s: encrypted auf EventSourced-Modellen kommt später (v1: CRUD)", m.name, f.name)
			}
		}
	}
	if err := d.bootstrapSystemTables(ctx); err != nil {
		return err
	}
	if err := d.registerInstance(ctx); err != nil {
		return err
	}
	// Vor jeder DDL: ein deklariertes Placement, das es nicht gibt, soll
	// als solches gemeldet werden und nicht als Tablespace-Fehler mitten
	// im CREATE TABLE.
	if err := d.verifyPlacements(d.regionPlacements()); err != nil {
		return err
	}

	st, err := d.readSchemaState(ctx)
	if err != nil {
		return err
	}
	sum := d.reg.checksum()

	switch {
	case st.current == 0:
		// Erstinstallation: direkt auf die deklarierte Version, keine Schritte.
		if err := d.applySchema(ctx); err != nil {
			return err
		}
		if err := d.writeSchemaState(ctx, d.q(), schemaState{
			current: d.schemaVersion, phase: phaseIdle, checksum: sum,
		}); err != nil {
			return err
		}
	case d.schemaVersion < st.current:
		return fmt.Errorf("orm: deklarierte SchemaVersion %d ist älter als DB-Stand %d", d.schemaVersion, st.current)
	case d.schemaVersion == st.current:
		if sum != st.checksum {
			return fmt.Errorf("%w (DB-Stand v%d)", ErrSchemaDrift, st.current)
		}
		// Idempotenter No-op — auch für Alt-Instanzen während einer laufenden
		// Migration einer neueren Generation.
	default:
		if err := d.runUpgrade(ctx, st, plan); err != nil {
			return err
		}
	}

	// Bewusst außerhalb der Versions-Fallunterscheidung: ES-Nebentabellen
	// (inkl. der geo-Spalte der Snapshots) müssen auch bei unveränderter
	// Schema-Version entstehen — Bibliotheks-Upgrades ändern die
	// App-Schema-Version nicht. Idempotent, No-op wenn alles da ist.
	for _, m := range d.reg.ordered {
		if m.kind == kindEventSourced {
			if err := d.ensureESTables(ctx, m); err != nil {
				return err
			}
		}
	}
	if err := d.persistTopology(ctx); err != nil {
		return err
	}
	// Ebenso versionsunabhängig: eine Region, die erst nachträglich
	// deklariert wird, ändert die Schema-Version nicht — ihre Partitionen
	// (und der Umbau unpartitionierter Bestands-Tabellen) müssen trotzdem
	// passieren.
	if err := d.reconcileGeoPartitions(ctx); err != nil {
		return err
	}
	if err := d.tenants.bootstrap(ctx); err != nil {
		return err
	}
	if err := d.bootstrapEventTypes(ctx); err != nil {
		return err
	}
	if err := d.validateUpcasters(); err != nil {
		return err
	}
	d.migrated = true
	return nil
}

func (d *DB) bootstrapSystemTables(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS ormpp_schema_state (
			id INTEGER PRIMARY KEY CHECK (id = 1),
			schema_version INTEGER NOT NULL,
			target_version INTEGER NOT NULL DEFAULT 0,
			phase TEXT NOT NULL DEFAULT 'idle',
			models_checksum TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)`,
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS ormpp_schema_history (
			id %s,
			version INTEGER NOT NULL,
			phase_from TEXT NOT NULL,
			phase_to TEXT NOT NULL,
			applied_at TEXT NOT NULL,
			applied_by TEXT NOT NULL
		)`, d.dial.autoPK()),
		`CREATE TABLE IF NOT EXISTS ormpp_instances (
			instance_id TEXT PRIMARY KEY,
			hostname TEXT NOT NULL,
			geo TEXT NOT NULL,
			migration_role INTEGER NOT NULL,
			app_version TEXT NOT NULL,
			schema_version INTEGER NOT NULL,
			started_at TEXT NOT NULL,
			last_heartbeat TEXT NOT NULL
		)`,
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS ormpp_leases (
			name TEXT PRIMARY KEY,
			holder TEXT NOT NULL,
			fencing_token %s NOT NULL,
			expires_at TEXT NOT NULL
		)`, d.dial.columnType(kInt)),
		`CREATE TABLE IF NOT EXISTS ormpp_deprecated (
			model TEXT NOT NULL,
			column_name TEXT NOT NULL,
			deprecated_in INTEGER NOT NULL,
			PRIMARY KEY (model, column_name)
		)`,
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS ormpp_migration_progress (
			version INTEGER NOT NULL,
			step TEXT NOT NULL,
			geo TEXT NOT NULL,
			last_key TEXT NOT NULL DEFAULT '',
			rows_done %s NOT NULL DEFAULT 0,
			state TEXT NOT NULL DEFAULT '',
			updated_at TEXT NOT NULL,
			PRIMARY KEY (version, step, geo)
		)`, d.dial.columnType(kInt)),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS ormpp_dualwrite_queue (
			id %s,
			tbl TEXT NOT NULL,
			pk TEXT NOT NULL,
			op TEXT NOT NULL,
			changed_at TEXT NOT NULL
		)`, d.dial.autoPK()),
		`CREATE TABLE IF NOT EXISTS ormpp_tenants (
			tenant_id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			status TEXT NOT NULL CHECK (status IN ('active','archived')),
			created_at TEXT NOT NULL
		)`,
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS ormpp_event_types (
			type_id %s PRIMARY KEY,
			type TEXT NOT NULL UNIQUE
		)`, d.dial.columnType(kInt)),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS ormpp_checkpoints (
			consumer TEXT NOT NULL,
			geo TEXT NOT NULL,
			seq %s NOT NULL,
			PRIMARY KEY (consumer, geo)
		)`, d.dial.columnType(kInt)),
		`CREATE TABLE IF NOT EXISTS ormpp_geo_regions (
			name TEXT PRIMARY KEY,
			status TEXT NOT NULL CHECK (status IN ('bootstrapping','active','draining','removed')),
			placement TEXT NOT NULL DEFAULT '',
			topology_version INTEGER NOT NULL DEFAULT 1,
			created_at TEXT NOT NULL
		)`,
	}
	for _, s := range stmts {
		if _, err := d.q().ExecContext(ctx, s); err != nil {
			return fmt.Errorf("orm: Systemtabellen anlegen: %w", err)
		}
	}
	// Bestands-DBs aus Phase 1/2: neue Zustandsspalten additiv ergänzen.
	cols, err := d.dial.tableColumns(d.q(), "ormpp_schema_state")
	if err != nil {
		return err
	}
	have := map[string]bool{}
	for _, c := range cols {
		have[c] = true
	}
	for col, ddl := range map[string]string{
		"target_version": `ALTER TABLE ormpp_schema_state ADD COLUMN target_version INTEGER NOT NULL DEFAULT 0`,
		"phase":          `ALTER TABLE ormpp_schema_state ADD COLUMN phase TEXT NOT NULL DEFAULT 'idle'`,
	} {
		if !have[col] {
			if _, err := d.q().ExecContext(ctx, ddl); err != nil {
				return fmt.Errorf("orm: ormpp_schema_state erweitern: %w", err)
			}
		}
	}
	return nil
}
