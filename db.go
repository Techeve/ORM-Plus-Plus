package orm

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
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
	dial    dialect
	opts    openOptions
	reg     *registry
	tenants *TenantRegistry

	schemaVersion  int
	regions        map[string]bool // deklarierte Topologie; leer = implizit "local"
	migrationsToDo []int           // registrierte MigrationTo-Versionen (Phase 3)
	migrated       bool
}

func (d *DB) db() *DB    { return d }
func (d *DB) q() queryer { return d.sql }
func (d *DB) inTx() bool { return false }

// Tx ist eine laufende Transaktion.
type Tx interface {
	Handle
}

type txHandle struct {
	parent *DB
	tx     *sql.Tx
}

func (t *txHandle) db() *DB    { return t.parent }
func (t *txHandle) q() queryer { return t.tx }
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
	d := &DB{
		sql:           sdb,
		dial:          dial,
		opts:          o,
		reg:           newRegistry(),
		schemaVersion: 1,
		regions:       map[string]bool{},
	}
	d.tenants = newTenantRegistry(d)
	return d, nil
}

// Close schließt den Verbindungspool.
func (d *DB) Close() error { return d.sql.Close() }

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
	}
}

// validGeo prüft ein Daten-Geo gegen die deklarierte Topologie.
func (d *DB) validGeo(geo string) bool {
	if len(d.regions) == 0 {
		return geo == "local"
	}
	return d.regions[geo]
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

// MigrationTo registriert Migrationsschritte zu einer Version (Phase 3:
// Schritte werden noch nicht ausgeführt; die Registrierung ist vorbereitet).
func MigrationTo(d *DB, version int, steps ...MigrationStep) {
	d.migrationsToDo = append(d.migrationsToDo, version)
}

// MigrationStep ist ein Schritt einer versionierten Migration.
type MigrationStep interface{ migrationStep() }

// Batch liefert einem BatchScript Zeilen häppchenweise (Phase 3).
type Batch struct{}

type batchScript struct{ name string }

func (batchScript) migrationStep() {}

// BatchScript deklariert ein freies Batch-Migrationsskript (Phase 3).
func BatchScript(name string, fn func(ctx context.Context, b Batch) error) MigrationStep {
	return batchScript{name: name}
}

// FinalizeMigration schließt eine Migration ab (Phase 3).
func (d *DB) FinalizeMigration(ctx context.Context, version int) error {
	return fmt.Errorf("orm: FinalizeMigration ist noch nicht implementiert (Phase 3): %w", ErrMigrationPending)
}

// StartWorkers startet die Hintergrund-Verarbeitung. In Phase 1 ein No-op
// (Projektionen/Reaktoren kommen in Phase 2, Leases in Phase 2/3).
func (d *DB) StartWorkers(ctx context.Context) error { return nil }

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

// Migrate validiert die Registry, bootstrapt Systemtabellen, prüft
// Version/Drift und wendet den additiven Schema-Diff an.
func (d *DB) Migrate(ctx context.Context) error {
	if err := d.reg.resolve(); err != nil {
		return err
	}
	for _, m := range d.reg.ordered {
		if m.kind == kindEventSourced {
			return fmt.Errorf("orm: %s: EventSourced-Modelle sind noch nicht implementiert (Phase 2, siehe doc/TASK.md)", m.name)
		}
	}

	if err := d.bootstrapSystemTables(ctx); err != nil {
		return err
	}

	storedVersion, storedChecksum, err := d.readSchemaState(ctx)
	if err != nil {
		return err
	}
	sum := d.reg.checksum()

	switch {
	case storedVersion == 0:
		// Erstinstallation.
	case d.schemaVersion < storedVersion:
		return fmt.Errorf("orm: deklarierte SchemaVersion %d ist älter als DB-Stand %d", d.schemaVersion, storedVersion)
	case sum != storedChecksum && d.schemaVersion == storedVersion:
		return fmt.Errorf("%w (DB-Stand v%d)", ErrSchemaDrift, storedVersion)
	}

	if err := d.applySchema(ctx); err != nil {
		return err
	}
	if err := d.writeSchemaState(ctx, d.schemaVersion, sum); err != nil {
		return err
	}
	if err := d.tenants.bootstrap(ctx); err != nil {
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
			models_checksum TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS ormpp_tenants (
			tenant_id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			status TEXT NOT NULL CHECK (status IN ('active','archived')),
			created_at TEXT NOT NULL
		)`,
	}
	for _, s := range stmts {
		if _, err := d.sql.ExecContext(ctx, s); err != nil {
			return fmt.Errorf("orm: Systemtabellen anlegen: %w", err)
		}
	}
	return nil
}

func (d *DB) readSchemaState(ctx context.Context) (version int, checksum string, err error) {
	row := d.sql.QueryRowContext(ctx, `SELECT schema_version, models_checksum FROM ormpp_schema_state WHERE id = 1`)
	switch err := row.Scan(&version, &checksum); err {
	case nil:
		return version, checksum, nil
	case sql.ErrNoRows:
		return 0, "", nil
	default:
		return 0, "", err
	}
}

func (d *DB) writeSchemaState(ctx context.Context, version int, checksum string) error {
	_, err := d.sql.ExecContext(ctx, `
		INSERT INTO ormpp_schema_state (id, schema_version, models_checksum, updated_at)
		VALUES (1, ?, ?, ?)
		ON CONFLICT (id) DO UPDATE SET schema_version = excluded.schema_version,
			models_checksum = excluded.models_checksum, updated_at = excluded.updated_at`,
		version, checksum, time.Now().UTC().Format(time.RFC3339Nano))
	return err
}
