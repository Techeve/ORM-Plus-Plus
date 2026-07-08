package orm

import "time"

// --- Open-Optionen ---

// Role bestimmt, ob eine Instanz Migrations-Backfill-Shards übernehmen darf.
type Role int

const (
	MigrationNone Role = iota
	MigrationWorker
)

type openOptions struct {
	instanceGeo         string
	migrationRole       Role
	appVersion          string
	defaultSnapshotEach int
	eventTypePrefix     string
}

// OpenOption konfiguriert Open.
type OpenOption func(*openOptions)

// InstanceGeo deklariert die Region, in der dieser Prozess läuft.
func InstanceGeo(geo string) OpenOption {
	return func(o *openOptions) { o.instanceGeo = geo }
}

// MigrationRole legt fest, ob die Instanz Migrations-Worker ist.
func MigrationRole(r Role) OpenOption {
	return func(o *openOptions) { o.migrationRole = r }
}

// AppVersion hinterlegt die Anwendungsversion im Instanzregister.
func AppVersion(v string) OpenOption {
	return func(o *openOptions) { o.appVersion = v }
}

// DefaultSnapshotEvery setzt den globalen Snapshot-Default für ES-Modelle.
func DefaultSnapshotEvery(n int) OpenOption {
	return func(o *openOptions) { o.defaultSnapshotEach = n }
}

// EventTypePrefix setzt das Präfix für CloudEvents-Typen.
func EventTypePrefix(p string) OpenOption {
	return func(o *openOptions) { o.eventTypePrefix = p }
}

// --- Model-Modus & -Optionen ---

type modelKind int

const (
	kindCRUD modelKind = iota
	kindEventSourced
)

// ModelMode bestimmt die Persistenzart eines Models.
type ModelMode struct{ kind modelKind }

// CRUD deklariert klassische Persistenz.
func CRUD() ModelMode { return ModelMode{kind: kindCRUD} }

// EventSourced deklariert Event-Sourcing-Persistenz (Phase 2).
func EventSourced() ModelMode { return ModelMode{kind: kindEventSourced} }

type geoMode int

const (
	geoScoped geoMode = iota
	geoGlobal
	geoFlexible
)

type modelOptions struct {
	tenantFree   bool
	geoMode      geoMode
	writeForward bool
	uniques      [][]string
	indexes      [][]string

	snapshotEvery    int
	snapshotMaxAge   time.Duration
	snapshotKeepLast int
	snapshotDisabled bool
}

// ModelOption konfiguriert die Registrierung eines Models.
type ModelOption func(*modelOptions)

// TenantFree deklariert ein Model ohne Tenant-Spalte und -Filter —
// für technische Tabellen ohne Nutzerdaten.
func TenantFree() ModelOption {
	return func(m *modelOptions) { m.tenantFree = true }
}

// GeoScoped: genau eine Region pro Datensatz (Default).
func GeoScoped() ModelOption {
	return func(m *modelOptions) { m.geoMode = geoScoped }
}

// GeoGlobal: Model ist in allen Regionen vorhanden (Stammdaten).
func GeoGlobal() ModelOption {
	return func(m *modelOptions) { m.geoMode = geoGlobal }
}

// GeoFlexibleOption konfiguriert GeoFlexible.
type GeoFlexibleOption func(*modelOptions)

// WriteForwarding leitet Schreibzugriffe auf Replikate an die Heimatregion weiter.
func WriteForwarding() GeoFlexibleOption {
	return func(m *modelOptions) { m.writeForward = true }
}

// WriteHomeOnly lehnt Schreibzugriffe auf Replikate ab (Default).
func WriteHomeOnly() GeoFlexibleOption {
	return func(m *modelOptions) { m.writeForward = false }
}

// GeoFlexible: Heimatregion + Replikate pro Datensatz wählbar.
func GeoFlexible(opts ...GeoFlexibleOption) ModelOption {
	return func(m *modelOptions) {
		m.geoMode = geoFlexible
		for _, o := range opts {
			o(m)
		}
	}
}

// Unique deklariert einen Unique-Constraint über mehrere Felder
// (Go-Feldnamen). Tenant/Geo werden automatisch einbezogen.
func Unique(fields ...string) ModelOption {
	return func(m *modelOptions) { m.uniques = append(m.uniques, fields) }
}

// Index deklariert einen zusammengesetzten Sekundärindex (Go-Feldnamen).
func Index(fields ...string) ModelOption {
	return func(m *modelOptions) { m.indexes = append(m.indexes, fields) }
}

// SnapshotEvery: Snapshot alle n Events pro Aggregat.
func SnapshotEvery(n int) ModelOption {
	return func(m *modelOptions) { m.snapshotEvery = n }
}

// SnapshotMaxAge: Snapshot spätestens nach d (ODER-Bedingung).
func SnapshotMaxAge(d time.Duration) ModelOption {
	return func(m *modelOptions) { m.snapshotMaxAge = d }
}

// SnapshotKeepLast: Anzahl aufbewahrter Snapshots (Default 2).
func SnapshotKeepLast(n int) ModelOption {
	return func(m *modelOptions) { m.snapshotKeepLast = n }
}

// SnapshotDisabled schaltet Snapshots für das Model ab.
func SnapshotDisabled() ModelOption {
	return func(m *modelOptions) { m.snapshotDisabled = true }
}

// --- Batch-Optionen ---

type batchOptions struct {
	chunk int
}

// BatchOption konfiguriert InsertMany.
type BatchOption func(*batchOptions)

// Chunked teilt InsertMany in Chunks zu n Zeilen; jeder Chunk ist eine
// eigene Transaktion (nicht atomar über das Ganze).
func Chunked(n int) BatchOption {
	return func(b *batchOptions) { b.chunk = n }
}
