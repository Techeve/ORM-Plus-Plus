# ORM++ — API-Referenz (Design)

Verbindliche Spezifikation der öffentlichen Go-API von ORM++. Dieses Dokument ist der Vertrag, gegen den implementiert wird; Abweichungen in der Implementierung sind Fehler oder müssen hier nachgezogen werden. Architektur-Hintergründe und physisches Schema: siehe [ROADMAP.md](ROADMAP.md).

**Status:** Design abgeschlossen, Implementierung ausstehend. Import-Pfad:

```go
import orm "gitlab.techeve.de/orm-plus-plus/orm-plus-plus"
```

---

## Inhalt

1. [Grundprinzip](#1-grundprinzip)
2. [Instanziierung & Verbindung](#2-instanziierung--verbindung)
3. [Topologie (Geo-Regionen)](#3-topologie-geo-regionen)
4. [Kontext: Tenant & Geo](#4-kontext-tenant--geo)
5. [Modelle deklarieren](#5-modelle-deklarieren)
6. [CRUD-Modelle: Schreiben, Lesen, Queries, Transaktionen](#6-crud-modelle)
7. [Event-Sourcing-Modelle](#7-event-sourcing-modelle)
8. [Schema-Versionen & Migration](#8-schema-versionen--migration)
9. [Worker & Clusterbetrieb](#9-worker--clusterbetrieb)
10. [Observability](#10-observability)
11. [Fehler](#11-fehler)

---

## 1. Grundprinzip

**Verhaltensgleichheit über allen Backends.** Für die konsumierende Anwendung ist irrelevant, welche Datenbank darunter liegt (SQLite, PostgreSQL, YugabyteDB). Jede Deklaration wird auf jedem Backend akzeptiert und semantisch erfüllt — nativ, wo die DB es kann; emuliert oder kollabiert, wo nicht. App-Code verzweigt **nie** nach dem Backend; dieselbe Anwendung läuft byte-identisch auf allen dreien.

Konsequenzen für die API:

- Es gibt keine Funktion, die das Backend preisgibt (kein `db.Kind()`). Einzige Ausnahme: Observability-APIs (Abschnitt 10) zeigen dem *Betreiber* die physische Wahrheit.
- Eine Topologie mit fünf Regionen auf SQLite ist gültig — SQLite hat implizit die eine Region `local`, alle deklarierten Regionen mappen darauf. Antworten der Verhaltens-API bleiben semantisch korrekt.
- Die Anwendung schreibt **kein SQL** und kennt keine Tabellen-, Treiber- oder Dialektdetails.

---

## 2. Instanziierung & Verbindung

### 2.1 Open

```go
func Open(driver Driver, opts ...OpenOption) (*DB, error)
```

Baut den Verbindungspool auf, validiert das Instanz-Geo gegen die Topologie, registriert die Instanz im Instanzregister (mit Heartbeat) und lädt das Typ-Wörterbuch. `Open` führt **keine** Schema-Änderungen aus — das macht `Migrate` (Abschnitt 8).

**Treiber:**

```go
orm.SQLite(path string)      // eingebettet; Demo/Desktop/Tests. WAL-Modus, eine Schreib-Connection.
orm.Postgres(dsn string)     // Server, On-Prem (pgx-Pool)
orm.Yugabyte(dsn string)     // verteilt; DSN sollte die regional nahen Endpunkte enthalten
```

**Optionen:**

| Option | Bedeutung | Default |
|---|---|---|
| `orm.InstanceGeo(geo string)` | Region, in der dieser Prozess läuft. Bestimmt, welche Hintergrund-Arbeit (Projektions-/Migrations-Shards) die Instanz übernehmen darf. | `"local"` |
| `orm.MigrationRole(role)` | `orm.MigrationNone` \| `orm.MigrationWorker` — darf diese Instanz Backfill-Shards übernehmen? | `MigrationNone` |
| `orm.AppVersion(v string)` | Version der Anwendung, landet im Instanzregister (Betriebs-Sicht). | `""` |
| `orm.DefaultSnapshotEvery(n int)` | Globaler Snapshot-Default für alle ES-Modelle. | `100` |
| `orm.EventTypePrefix(p string)` | Präfix für CloudEvents-Typen, z. B. `"de.techeve.dns"`. | Modulpfad-basiert |

**Beispiel — kleinste sinnvolle App (SQLite, kein Geo, kein Tenant-Multibetrieb):**

```go
db, err := orm.Open(orm.SQLite("./app.db"))
if err != nil { ... }
defer db.Close()
```

**Beispiel — Cluster-Instanz (Yugabyte, Region US-Ost, darf migrieren):**

```go
db, err := orm.Open(orm.Yugabyte(dsnUSEast),
    orm.InstanceGeo("us-east"),
    orm.MigrationRole(orm.MigrationWorker),
    orm.AppVersion("1.4.2"),
)
```

### 2.2 Close

```go
func (db *DB) Close() error
```

Meldet die Instanz im Register ab (Leases werden freigegeben, Worker gestoppt) und schließt den Pool.

---

## 3. Topologie (Geo-Regionen)

Die Topologie beschreibt, **welche Regionen der Cluster hat**. Sie ist Cluster-Zustand (Tabelle `ormpp_geo_regions`), keine Instanz-Konfiguration: Jede Instanz deklariert dieselbe Topologie (gleiches Binary); wer beim `Migrate` die Bootstrap-Lease gewinnt, wendet Differenzen an. Es gibt keine besondere „erste Instanz".

```go
func Topology(db *DB, regions ...RegionDecl)
func Region(name string, opts ...RegionOption) RegionDecl

orm.Placement(cloudPlacement string)   // YB: Tablespace-/Placement-Zuordnung
```

**Beispiel:**

```go
orm.Topology(db,
    orm.Region("eu-central", orm.Placement("cloud1.eu-central-1")),
    orm.Region("us-east",    orm.Placement("cloud1.us-east-1")),
    orm.Region("ap-south",   orm.Placement("cloud1.ap-south-1")),
)
```

**Regeln:**

- Keine `Topology`-Deklaration ⇒ implizite Region `local`. Der einfachste Fall braucht null Geo-Code.
- **Region hinzufügen** = zusätzliche `orm.Region(...)`-Zeile + Rollout. Additiv, kein Sondermodus. Die neue Region durchläuft den Lebenszyklus `bootstrapping → active`; während `bootstrapping` werden `GeoGlobal`-Modelle und `ReplicateAll`-Datensätze nachrepliziert, Schreiben in die Region ist bis `active` fail-closed gesperrt.
- **Region entfernen** = Status `draining` setzen (Betriebsaktion, Stufe 2 auch per API): keine neuen Daten; Datensätze mit *Heimat* in der Region ziehen per geo-verteiltem Backfill um; Replikate werden verworfen; erst dann `removed`.
- Auf SQLite/Single-Region-Postgres kollabieren alle Regionen auf eine — die Deklaration bleibt gültig (Grundprinzip).

---

## 4. Kontext: Tenant & Geo

Tenant und Geo sind **Pflicht-Scope jeder Datenoperation** und hängen am `context.Context` — nie an Funktionssignaturen.

```go
func WithTenant(ctx context.Context, tenant uuid.UUID) context.Context
func WithGeo(ctx context.Context, home string, opts ...GeoOption) context.Context

orm.ReplicateTo(regions ...string)   // Kopien in weiteren Regionen (nur GeoFlexible)
orm.ReplicateAll()                   // Kopien in allen aktiven Regionen, folgt der Topologie
```

**Regeln:**

- Fehlender Tenant ⇒ `orm.ErrNoTenant` (fail-closed). Nichts läuft versehentlich tenant-los. Single-Tenant-Apps setzen einmal beim Start einen konstanten Tenant.
- Fehlendes Geo ⇒ Default-Region der Topologie (`local`, wenn keine Topologie deklariert ist); bei Mehr-Regionen-Topologie ohne Default ⇒ `orm.ErrNoGeo`.
- `WithGeo` bestimmt das **Daten-Geo** (wohin ein Datensatz gehört) — unabhängig vom Instanz-Geo. Eine EU-Instanz darf einen US-Datensatz schreiben; er landet korrekt in der US-Partition (mit Latenz).
- Geo auf eine nicht-aktive Region ⇒ `orm.ErrRegionNotActive`.

**Beispiel — typischer Request-Handler:**

```go
func (s *Server) handle(w http.ResponseWriter, r *http.Request) {
    ctx := orm.WithTenant(r.Context(), tenantFromAuth(r))
    ctx = orm.WithGeo(ctx, regionFromUser(r))
    // ... alle ORM-Aufrufe mit diesem ctx
}
```

---

## 5. Modelle deklarieren

### 5.1 Registrierung

```go
func Register[T any](db *DB, mode ModelMode, opts ...ModelOption)

orm.CRUD()            // klassische Persistenz
orm.EventSourced()    // Event-Sourcing (Struct muss orm.Aggregate einbetten)
```

Registrierung passiert beim Start, vor `Migrate`. Die Registry validiert das Struct (Tags, PK, Einbettung) und kompiliert den Mapping-Plan einmalig (keine Reflection im Hot Path).

**Geo-Modi** (Model-Option, Default `GeoScoped`):

| Option | Bedeutung |
|---|---|
| `orm.GeoScoped()` | Jeder Datensatz liegt in genau einer Region (Normalfall). |
| `orm.GeoGlobal()` | Model ist in **allen** Regionen vorhanden (Stammdaten: Tenants, Nutzer, Pläne). YB: natives Replika-Placement + Follower-Reads. Schreiben zahlt Cross-Region-Konsens — für schreibintensive Modelle ungeeignet (Registry warnt). |
| `orm.GeoFlexible(opts...)` | **Pro Datensatz** wählbar: Heimatregion + lesende Replikate. Sub-Optionen: `orm.WriteForwarding()` (Update auf Replikat wird an Heimat weitergeleitet) oder `orm.WriteHomeOnly()` (Fehler). |

**Snapshot-Optionen** (nur ES-Modelle, überschreiben den globalen Default):

```go
orm.SnapshotEvery(n int)             // alle n Events pro Aggregat
orm.SnapshotMaxAge(d time.Duration)  // ODER-Bedingung: spätestens nach d
orm.SnapshotKeepLast(n int)          // Aufbewahrung, Default 2
orm.SnapshotDisabled()               // reine Logs, werden nie gefaltet geladen
```

### 5.2 Struct-Tags (CRUD-Modelle und Read-Models)

```go
type ProviderAccount struct {
    ID        orm.ID    `orm:"pk"`
    Name      string    `orm:"index"`
    Email     string    `orm:"unique"`
    Labels    []string  `orm:"json"`
    Version   int64     `orm:"version"`      // optimistisches Locking
    CreatedAt time.Time `orm:"autocreate"`
    UpdatedAt time.Time `orm:"autoupdate"`
    Notes     string    `orm:"deprecated"`   // markiert, fällt bei Finalisierung
}
```

| Tag | Bedeutung |
|---|---|
| `pk` | Primärschlüssel (genau einer; `orm.ID` ist UUIDv7) |
| `index`, `unique` | Sekundär-/Unique-Index |
| `json` | Verschachtelte Werte als JSON(B)-Spalte (v1; Untertabellen in Stufe 2) |
| `version` | Spalte für optimistisches Locking bei `Update` |
| `autocreate`, `autoupdate` | Zeitstempel-Pflege durch die Engine |
| `deprecated` | Feld ist zur Entfernung markiert (Expand/Contract, Abschnitt 8) |
| `-` | Feld wird nicht persistiert |

`tenant_id` und die Geo-Spalten deklariert man **nicht** — sie sind implizit in jeder Tabelle vorhanden und werden ausschließlich über den Context gesteuert.

### 5.3 Vollständiges Deklarationsbeispiel

```go
// CRUD, tenant-scoped, eine Region pro Datensatz:
orm.Register[ProviderAccount](db, orm.CRUD())

// CRUD-Stammdaten, überall vorhanden:
orm.Register[Tenant](db, orm.CRUD(), orm.GeoGlobal())

// CRUD, Regionen pro Datensatz wählbar:
orm.Register[SyncGroup](db, orm.CRUD(),
    orm.GeoFlexible(orm.WriteForwarding()),
)

// Event-sourced (Details Abschnitt 7):
orm.Register[DNSZone](db, orm.EventSourced(),
    orm.Events(
        orm.E[ZoneCreated]("zone.created"),
        orm.E[RecordAdded]("zone.record_added"),
        orm.E[ZoneDeleted]("zone.deleted"),
    ),
    orm.SnapshotEvery(200),
    orm.SnapshotKeepLast(2),
)
```

---

## 6. CRUD-Modelle

### 6.1 Repository

```go
func Repo[T any](h Handle) Repository[T]     // Handle: *DB oder Tx
```

```go
type Repository[T any] interface {
    Insert(ctx context.Context, entity *T) error          // füllt pk/autocreate
    Get(ctx context.Context, id orm.ID) (*T, error)       // ErrNotFound
    Update(ctx context.Context, entity *T) error          // ErrVersionConflict bei `version`-Tag
    Upsert(ctx context.Context, entity *T) error
    Delete(ctx context.Context, id orm.ID) error
    SetGeo(ctx context.Context, id orm.ID, home string, opts ...GeoOption) error  // nur GeoFlexible
    Query(ctx context.Context) QueryBuilder[T]
}
```

**Beispiel:**

```go
accounts := orm.Repo[ProviderAccount](db)

acc := &ProviderAccount{Name: "Cloudflare Prod", Email: "ops@example.org"}
err := accounts.Insert(ctx, acc)          // acc.ID, acc.CreatedAt gefüllt

acc, err = accounts.Get(ctx, acc.ID)
acc.Name = "Cloudflare Production"
err = accounts.Update(ctx, acc)           // Versionskonflikt ⇒ ErrVersionConflict
err = accounts.Delete(ctx, acc.ID)
```

**Beispiel — GeoFlexible-Datensatz mit Replikaten:**

```go
groups := orm.Repo[SyncGroup](db)

ctx := orm.WithTenant(ctx, tenant)
ctx = orm.WithGeo(ctx, "eu-central", orm.ReplicateTo("us-east", "ap-south"))
err := groups.Insert(ctx, g)              // Heimat EU, lesende Kopien US + AP

// Später umziehen / Replikate ändern (engine-geführt):
err = groups.SetGeo(ctx, g.ID, "us-east", orm.ReplicateTo("eu-central"))
```

Lesen ist **lokal-bevorzugt**: Existiert in der Region der lesenden Instanz eine Kopie, kommt sie von dort; sonst antwortet die Heimatregion.

### 6.2 Query-Builder

```go
type QueryBuilder[T any] interface {
    Where(cond Cond) QueryBuilder[T]
    OrderBy(field string, dir Dir) QueryBuilder[T]     // orm.Asc | orm.Desc
    Limit(n int) QueryBuilder[T]
    Offset(n int) QueryBuilder[T]
    All() ([]*T, error)
    First() (*T, error)                                 // ErrNotFound
    Count() (int64, error)
    Exists() (bool, error)
}
```

**Bedingungen:**

```go
orm.Eq(field, v)   orm.Ne(field, v)
orm.Gt(field, v)   orm.Gte(field, v)   orm.Lt(field, v)   orm.Lte(field, v)
orm.Like(field, pattern)               orm.In(field, vs...)
orm.IsNull(field)  orm.NotNull(field)
orm.And(conds...)  orm.Or(conds...)    orm.Not(cond)
```

Feldnamen sind die Go-Feldnamen des Structs (die Engine mappt auf Spalten). Unbekannte Felder ⇒ Fehler beim Bauen, nicht zur Laufzeit in der DB.

**Beispiel:**

```go
list, err := accounts.Query(ctx).
    Where(orm.And(
        orm.Like("Name", "Cloud%"),
        orm.Gte("CreatedAt", since),
    )).
    OrderBy("CreatedAt", orm.Desc).
    Limit(20).
    All()
```

Der Tenant-Filter (und Geo-Scope) wird **immer** automatisch injiziert — er ist nicht abschaltbar und taucht in keiner Query auf.

### 6.3 Transaktionen

```go
func (db *DB) Tx(ctx context.Context, fn func(tx orm.Tx) error) error
```

```go
err := db.Tx(ctx, func(tx orm.Tx) error {
    if err := orm.Repo[ProviderAccount](tx).Insert(ctx, acc); err != nil { return err }
    return orm.Repo[AuditNote](tx).Insert(ctx, note)
})
```

Rollback bei Fehler-Rückgabe oder Panic. Verschachtelte `Tx` nutzen Savepoints. Event-Appends (Abschnitt 7) dürfen in derselben Transaktion mit CRUD-Schreibvorgängen stehen.

---

## 7. Event-Sourcing-Modelle

### 7.1 Deklaration

Ein ES-Model besteht aus: dem Zustand (Struct mit eingebettetem `orm.Aggregate`), den Event-Payloads und genau einer Pflichtfunktion `Apply`.

```go
type DNSZone struct {
    orm.Aggregate                       // bringt ID/Version/Laden/History/… mit

    Name    string   `orm:"index,unique"`
    Records []Record `orm:"json"`
    Status  string   `orm:"index"`
}

// Event-Payloads: reine Daten-Structs, nur das Delta.
type ZoneCreated struct{ Name string }
type RecordAdded struct{ Record Record }
type ZoneDeleted struct{}

// Die einzige Pflicht des Entwicklers — pure Funktion, keine DB-Berührung.
// Sie ist zugleich Projektions-, Rebuild- UND Snapshot-Logik.
func (z *DNSZone) Apply(e orm.Event) error {
    switch ev := e.Payload.(type) {
    case ZoneCreated:  z.Name, z.Status = ev.Name, "active"
    case RecordAdded:  z.Records = append(z.Records, ev.Record)
    case ZoneDeleted:  z.Status = "deleted"
    }
    return nil
}
```

Events werden bei der Registrierung benannt (Abschnitt 5.3). Der volle CloudEvents-Typ ergibt sich aus `EventTypePrefix + Name + ".v" + Version`, z. B. `de.techeve.dns.zone.record_added.v1`.

### 7.2 Schreiben: Append

```go
// Neues Aggregat:
zone := orm.New[DNSZone](db)                       // erzeugt ID (UUIDv7)
pos, err := zone.Append(ctx, ZoneCreated{Name: "example.org"})

// Bestehendes Aggregat: laden, dann anhängen.
zone, err := orm.Load[DNSZone](ctx, db, zoneID)
pos, err = zone.Append(ctx, RecordAdded{Record: rec})
```

- `Append` hängt ein oder mehrere Events **atomar** an (ein Aufruf = eine Transaktion) und erwartet implizit die geladene Aggregat-Version — ist inzwischen jemand dazwischengekommen: `orm.ErrVersionConflict`. Dann: `Refresh` + Entscheidung + erneut anhängen.
- Rückgabe `pos orm.Position` ist die Event-Position (Konsistenz-Token für `WaitFor`).
- „Löschen" ist ein Event (`ZoneDeleted{}`) — die Historie bleibt. Physisches Löschen (z. B. DSGVO) ist eine explizite Verwaltungsoperation (Stufe 2).
- Jedes `Append` löst automatisch die Trigger-Kette aus: eingebaute Projektion → `OnEvent`-Reaktoren → `Watch`-Streams.

### 7.3 Lesen: aktueller Zustand

```go
// Aus dem Read-Model (schnell; kann Millisekunden hinter dem letzten Append liegen):
zone, err := orm.Load[DNSZone](ctx, db, zoneID)

// Read-your-writes: warten, bis die Projektion die eigene Schreibposition erreicht hat:
zone, err = orm.Load[DNSZone](ctx, db, zoneID, orm.WaitFor(pos, 2*time.Second))

// Query-Builder — identisch zu CRUD, läuft gegen das Read-Model:
active, err := orm.Query[DNSZone](db, ctx).
    Where(orm.Eq("Status", "active")).
    OrderBy("Name", orm.Asc).
    All()
```

`Load` lädt intern: letzter Snapshot + Restevents durch `Apply` — transparent, der Aufrufer merkt nichts davon.

### 7.4 Lesen: Historie & Zeitreisen

Alle Funktionen existieren durch die `orm.Aggregate`-Einbettung von Haus aus und greifen transparent auf Snapshots und Archiv-Partitionen zu:

```go
zone.ID()          zone.Version()          zone.UpdatedAt()

old, err := zone.AtVersion(ctx, 42)         // Zustand nach Event 42
old, err  = zone.AtTime(ctx, timestamp)     // Zustand zu einem Zeitpunkt

// Event-Strom eines Aggregats (Audit, Drift-Verlauf) — als CloudEvents:
for ev, err := range zone.History(ctx) {
    fmt.Println(ev.Sequence, ev.Type, ev.Time, ev.Data)
}

err = zone.Refresh(ctx)                                  // nachladen
err = zone.Refresh(ctx, orm.WaitFor(pos, time.Second))   // mit Konsistenz-Token
```

**Event-Strom über alle Aggregate eines Models** (für eigene Konsumenten/Integrationen):

```go
for ev, err := range orm.Stream[DNSZone](ctx, db, orm.From(pos)) { ... }
```

Ordnungsgarantien: strikt pro Aggregat; monoton pro Region. Keine Totalordnung über Regionen (`orm.Position` ist ein Cursor-Vektor, einer je Geo).

### 7.5 Trigger: Reaktoren & Live-Streams

**`OnEvent` — der verlässliche Pfad.** Persistent, at-least-once, checkpointed, lease-koordiniert, rebuildfähig. Für abgeleitete Read-Views, Suche-Indizes, Benachrichtigungs-Fanout:

```go
orm.OnEvent[DNSZone](db, "zone.*",
    func(ctx context.Context, ce orm.CloudEvent, tx orm.Tx) error {
        return updateZoneDashboardView(ctx, tx, ce)   // läuft transaktional
    },
)
```

Handler müssen **idempotent** sein (at-least-once). Muster: Event-ID in der Ziel-View mitschreiben und Duplikate ignorieren.

**`Watch` — der schnelle Pfad.** Flüchtige Live-Benachrichtigung an verbundene Clients (SSE/WebSocket); wer nicht zuhört, verpasst nichts Dauerhaftes:

```go
for ce := range orm.Watch[DNSZone](ctx, db, orm.From(pos)) {
    hub.Broadcast(ce)
}
```

**Rebuild** einer Projektion oder View aus dem Event-Strom:

```go
err := orm.RebuildProjection[DNSZone](ctx, db)
err  = orm.RebuildView(ctx, db, "zone-dashboard")     // benannter OnEvent-Konsument
```

### 7.6 Event-Schema-Versionen: Upcaster

Events sind unveränderlich. Ändert sich ein Format, wird eine neue Version registriert und ein Upcaster transformiert alte Events **beim Lesen**:

```go
// Registrierung: RecordAdded ist jetzt v2.
orm.Register[DNSZone](db, orm.EventSourced(),
    orm.Events(
        orm.E[ZoneCreated]("zone.created"),
        orm.E[RecordAdded]("zone.record_added", orm.V(2)),
    ),
)

// v1 → v2 (Feld umbenannt); Kette v1→v2→v3 wird automatisch durchlaufen:
orm.Upcast(db, "zone.record_added", 1,
    func(old RecordAddedV1) (RecordAdded, error) {
        return RecordAdded{Record: old.Entry}, nil
    },
)
```

Fehlt ein Upcaster für eine in der DB vorhandene alte Version ⇒ Startfehler (nicht erst beim Lesen).

### 7.7 Snapshots

Snapshots sind **kein API-Thema für den Normalfall** — sie entstehen automatisch nach der Model-Politik (Abschnitt 5.1), asynchron, nie im Schreibpfad. Ein Snapshot ist der per `Apply` gefaltete, serialisierte Aggregat-Zustand; es gibt keine separate Snapshot-Berechnungsfunktion.

Opt-in für Sonderfälle (eigene Serialisierung — Caches ausklammern, Spezialformat):

```go
func (z *DNSZone) SnapshotMarshal() ([]byte, error)
func (z *DNSZone) SnapshotUnmarshal(b []byte) error
```

---

## 8. Schema-Versionen & Migration

### 8.1 Deklaration

```go
orm.SchemaVersion(db, 3)   // Schema-Version, die diese App-Ausgabe erwartet

orm.MigrationTo(db, 3,     // Schritte von 2 nach 3; ältere MigrationTo bleiben im Code
    orm.ReplaceModel[ZoneV2, DNSZone](func(ctx context.Context, old ZoneV2) (DNSZone, error) {
        return DNSZone{ /* Umbau */ }, nil
    }),
    orm.BatchScript("normalize-records", func(ctx context.Context, b orm.Batch) error {
        // b liefert Zeilen häppchenweise; Checkpoint verwaltet die Engine
        return nil
    }),
)
```

- **Additive Änderungen** (neue Spalte/Index/Model) brauchen keinen Schritt — Auto-Diff.
- **Entfallende Felder** werden mit `deprecated` markiert, nie automatisch gelöscht.
- **Drift-Schutz:** Modelle geändert ohne Versions-Erhöhung ⇒ Startfehler (Checksum-Vergleich).

### 8.2 Ausführung

```go
err := db.Migrate(ctx)                          // Normalfall

err = db.Migrate(ctx, orm.MigrationPlan{        // Cluster-Feintuning (optional)
    WorkersPerGeo: map[string]int{"eu-central": 4, "us-east": 2},
    BatchSize:     5000,
    Throttle:      orm.RowsPerSecond(20000),
})
```

`Migrate` ist idempotent und bei aktueller Version ein No-op (nur Instanz-Registrierung). Zustandsmaschine: `idle → expanding → backfill → dual-write → finalizing → idle`.

- **expanding:** additive DDL, global, ein Leader (Lease).
- **backfill:** geo-parallel; Arbeitseinheit ist der Shard `(Schritt, Geo, Schlüsselbereich)`, vergeben per Lease nur an Worker **derselben Region** (`MigrationRole(MigrationWorker)`). Wiederaufnehmbar, drosselbar.
- **dual-write:** neue Instanzen schreiben in alte und neue Struktur; alte Instanzen laufen unverändert weiter. Beide App-Generationen koexistieren.
- **finalizing:** explizit —

```go
err := db.FinalizeMigration(ctx, 3)
```

Vorbedingung (von ORM++ geprüft): keine lebende Instanz mit älterer Schema-Version im Instanzregister, alle Regionen mit Backfill fertig. Dann: Dual-Write beenden, `deprecated`-Felder und Alt-Tabellen entfernen.

---

## 9. Worker & Clusterbetrieb

### 9.1 Worker starten

```go
err := db.StartWorkers(ctx)
```

Startet die Hintergrund-Verarbeitung dieser Instanz: Projektionen, `OnEvent`-Reaktoren, Snapshot- und Archiv-Worker, Geo-Replikation, ggf. Migrations-Shards. Koordination über **Leases mit Fencing** in der DB — pro Projektion/Partition arbeitet clusterweit genau eine Instanz; fällt sie aus (Heartbeat-TTL), übernimmt eine andere **derselben Region**. `ctx`-Abbruch oder `db.Close()` stoppt die Worker sauber.

### 9.2 Deployment-Muster

**Ein Prozess (SQLite, Desktop/Demo):**

```go
db, _ := orm.Open(orm.SQLite("./app.db"))
registerModels(db)                 // gemeinsame Funktion: Register + Topology + SchemaVersion
db.Migrate(ctx)
db.StartWorkers(ctx)               // trivial: eine Region, ein Prozess
```

**N gleichartige App-Instanzen (Postgres/Yugabyte):** identischer Code wie oben — jede Instanz ruft `Migrate` (nur der Lease-Gewinner arbeitet) und `StartWorkers` (Leases verteilen die Arbeit). Kein Sonderfall.

**Dedizierte Worker-Prozesse:** gleiches Binary-Muster, die Instanz bedient nur Hintergrund-Arbeit:

```go
db, _ := orm.Open(orm.Yugabyte(dsn),
    orm.InstanceGeo("eu-central"),
    orm.MigrationRole(orm.MigrationWorker),
)
registerModels(db)
db.Migrate(ctx)
db.StartWorkers(ctx)
<-ctx.Done()                        // kein HTTP-Server — nur Worker
```

App-Instanzen setzen dann `MigrationRole(MigrationNone)` und verzichten optional auf `StartWorkers`, wenn Projektionen ausschließlich von den Worker-Prozessen laufen sollen.

**Geo-verteiltes Cluster (Rollout-Reihenfolge bei Migration):**

1. Neue App-Version (SchemaVersion n+1) regionsweise ausrollen — `Migrate` schaltet auf `expanding`, dann `backfill` (jede Region migriert ihre Daten mit ihren Workern), dann `dual-write`.
2. Alte Instanzen abbauen (Instanzregister leert sich).
3. `FinalizeMigration` — von einem Betriebs-Job oder manuell.

### 9.3 Verhalten im Fehlerfall

- Worker-Ausfall: Lease läuft ab, andere Instanz derselben Region übernimmt am Checkpoint.
- `Append`/CRUD während Migration: jederzeit erlaubt (Online-Migration); Dual-Write übernimmt die Engine.
- Netzsplit zur DB: Operationen liefern Fehler des Pools; Worker pausieren und nehmen nach Reconnect am Checkpoint wieder auf.

---

## 10. Observability

Observability-APIs zeigen dem **Betreiber** die physische Wahrheit — sie sind die einzige Stelle, an der Backends sich unterscheiden dürfen (auf SQLite erscheint ehrlich eine Region `local` mit einem Worker).

```go
st, err := db.MigrationStatus(ctx)
// st.Phase                        "backfill"
// st.CurrentVersion / TargetVersion
// st.Geo["eu-central"].Percent    87.3
// st.Geo["eu-central"].Workers    4

h, err := db.Health(ctx)
// h.Instances        lebende Instanzen (Geo, Rolle, App-/Schema-Version, Heartbeat)
// h.Projections      Lag je Projektion/Region (Events hinter dem Log)
// h.Regions          Topologie-Status (active/bootstrapping/draining)
```

Beide liefern reine Datenstrukturen — Anbindung an Logging/Metrics/Health-Endpoints ist Sache der App.

---

## 11. Fehler

Alle Fehler sind mit `errors.Is` prüfbare Sentinel-Werte:

| Fehler | Bedeutung |
|---|---|
| `orm.ErrNotFound` | Datensatz/Aggregat existiert nicht (im Tenant-/Geo-Scope) |
| `orm.ErrNoTenant` | Context ohne Tenant (fail-closed) |
| `orm.ErrNoGeo` | Mehr-Regionen-Topologie, aber kein Daten-Geo im Context |
| `orm.ErrRegionNotActive` | Daten-Geo zeigt auf `bootstrapping`/`draining`/unbekannte Region |
| `orm.ErrVersionConflict` | Optimistisches Locking: CRUD-`version` oder Aggregat-Version veraltet |
| `orm.ErrWaitTimeout` | `WaitFor`-Frist abgelaufen, Projektion hing hinterher |
| `orm.ErrSchemaDrift` | Modelle geändert ohne `SchemaVersion`-Erhöhung |
| `orm.ErrMigrationPending` | Operation erfordert abgeschlossene Migration (z. B. `FinalizeMigration` zu früh) |
| `orm.ErrReadOnlyReplica` | Schreibzugriff auf Replikat bei `WriteHomeOnly` |

---

## Anhang: Minimal-Beispiel Ende-zu-Ende

```go
package main

import (
    "context"
    orm "gitlab.techeve.de/orm-plus-plus/orm-plus-plus"
)

type Todo struct {
    ID    orm.ID `orm:"pk"`
    Title string `orm:"index"`
    Done  bool
}

func main() {
    ctx := context.Background()

    db, err := orm.Open(orm.SQLite("./todo.db"))
    if err != nil { panic(err) }
    defer db.Close()

    orm.Register[Todo](db, orm.CRUD())
    if err := db.Migrate(ctx); err != nil { panic(err) }
    db.StartWorkers(ctx)

    ctx = orm.WithTenant(ctx, orm.SingleTenant) // Konstante für Single-Tenant-Apps

    todos := orm.Repo[Todo](db)
    t := &Todo{Title: "ORM++ bauen"}
    _ = todos.Insert(ctx, t)

    open, _ := todos.Query(ctx).Where(orm.Eq("Done", false)).All()
    _ = open
}
```

Dieselbe Datei läuft unverändert gegen `orm.Yugabyte(dsn)` in einem geo-partitionierten Cluster — das ist das Versprechen von ORM++.
