# ORM++ — API-Referenz (Design)

Verbindliche Spezifikation der öffentlichen Go-API von ORM++. Dieses Dokument ist der Vertrag, gegen den implementiert wird; Abweichungen in der Implementierung sind Fehler oder müssen hier nachgezogen werden. Architektur-Hintergründe und physisches Schema: siehe [ROADMAP.md](ROADMAP.md).

**Status:** Phasen 1–4 implementiert — CRUD, Event Sourcing und Migrations-Engine laufen verhaltensgleich auf SQLite, PostgreSQL und YugabyteDB (identische Testsuite). Offen: native Partitionierung/Archivierung (4b), v1.0-Härtung (Phase 5). Import-Pfad:

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

### 4.1 Tenant-Registry (eingebaut)

Tenants kommen nicht aus der App — ORM++ bringt sie als System-Model mit (`ormpp_tenants`, beim Bootstrap angelegt, `GeoGlobal`, damit jede Region lokal validieren kann):

```go
tenants := db.Tenants()

t, err := tenants.Create(ctx, orm.TenantInfo{Name: "ACME GmbH"})   // ID: UUIDv7
t, err  = tenants.Get(ctx, id)
list, err := tenants.List(ctx)
err     = tenants.Archive(ctx, id)   // kein Hard-Delete: blockiert neue Schreib-
                                     // vorgänge, Bestandsdaten bleiben lesbar

// DSGVO: vollständiger Datenauszug eines Tenants (JSON Lines, alle Modelle
// inkl. Events, Snapshots und Archiv):
err = tenants.Export(ctx, id, w)

// Recht auf Vergessenwerden: physisches Löschen ALLER Daten des Tenants über
// alle Tabellen, Events, Snapshots und Archive hinweg. Nur die Engine kennt
// alle Orte. Zweistufig: Tenant muss archiviert sein (sonst
// ErrTenantNotArchived); Vorgang wird in ormpp_schema_history auditiert.
err = tenants.Purge(ctx, id)
```

`orm.SingleTenant` ist ein beim Bootstrap automatisch angelegter Tenant für Single-Tenant-Apps.

**Tenant-Regeln (nicht abschaltbar):**

1. **Insert-Verifikation:** Jeder `Insert`/`Append` prüft die Context-Tenant-ID gegen das Register — unbekannt oder archiviert ⇒ `orm.ErrUnknownTenant`. Durchsetzung engine-seitig (In-Memory-Cache des Registers, Invalidierung über die Event-Kette) plus FK-Constraint, wo die DB das nativ kann; das Verhalten ist auf allen Backends identisch.
2. **Write-once:** `tenant_id` wird beim Insert gesetzt und ist danach unveränderlich. Die Engine nimmt das Feld in kein `UPDATE` auf; es existiert keine API, einen Datensatz einem anderen Tenant zuzuordnen (Mandanten-Fusion wäre eine explizite, auditierte Verwaltungsoperation, Stufe 2).
3. **Scope in jeder Operation — auch per ID:** Alle Queries, Updates und Deletes filtern automatisch auf den Context-Tenant; auch `Get`/`Update`/`Delete` per Primärschlüssel. Ein per ID angesprochener Datensatz eines fremden Tenants verhält sich exakt wie nicht existent (`ErrNotFound`). Nicht-ID-Filter ohne Tenant sind konstruktionsbedingt unmöglich (fail-closed Context).

---

## 5. Modelle deklarieren

### 5.1 Registrierung

```go
func Register[T any](db *DB, mode ModelMode, opts ...ModelOption)

orm.CRUD()            // klassische Persistenz
orm.EventSourced()    // Event-Sourcing (Struct muss orm.Aggregate einbetten)
```

Registrierung passiert beim Start, vor `Migrate`. Die Registry validiert das Struct (Tags, PK, Einbettung) und kompiliert den Mapping-Plan einmalig (keine Reflection im Hot Path).

**Scope-Optionen:**

| Option | Bedeutung |
|---|---|
| `orm.TenantFree()` | Model ohne Tenant-Spalte und ohne Tenant-Filter — für **technische Tabellen ohne Nutzerdaten** (Konfiguration, Lookup-Werte, Job-Zustände). Operationen funktionieren auch mit Context ohne Tenant. Tenant-gebundene Modelle bleiben der Default; die Option ist die dokumentierte Ausnahme. |

**Zusammengesetzte Indizes & Constraints** (Model-Optionen; Feldnamen sind Go-Feldnamen):

```go
orm.Register[Record](db, orm.CRUD(),
    orm.Unique("ProjectID", "Name"),       // Unique-Constraint über mehrere Spalten
    orm.Index("Status", "CreatedAt"),      // zusammengesetzter Sekundärindex
)
```

Tenant- und Geo-Spalten werden automatisch in Unique-Constraints einbezogen (Eindeutigkeit gilt pro Tenant). Hinweis für YugabyteDB: Sekundärindizes sind dort verteilte Indizes mit entsprechenden Schreibkosten — die Registry übernimmt die Deklaration unverändert, das Verhalten ist identisch; die Kostenabwägung dokumentiert das Betriebs-Kapitel.

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
| `ref=Model[,ondelete=…]` | Referenz auf ein anderes Model (Abschnitt 5.4) |
| `enum=a\|b\|c` | Wertemenge für String-Felder: CHECK-Constraint wo nativ, engine-seitig geprüft überall — ungültiger Wert ⇒ `orm.ErrInvalidValue` |
| `default=…` | Default-Wert, wenn das Feld beim Insert den Zero-Value hat (nicht kombinierbar mit `required`) |
| `encrypted` | Feld wird verschlüsselt gespeichert (Abschnitt 5.5) |
| `immutable` | Write-once: wird beim Insert gesetzt, danach unveränderlich — die Engine nimmt das Feld in kein `UPDATE` auf (gleiches Verhalten wie `tenant_id`) |
| `required` | Muss beim Insert explizit gesetzt sein: Zero-Value ⇒ `orm.ErrRequiredField` |
| `deprecated` | Feld ist zur Entfernung markiert (Expand/Contract, Abschnitt 8) |
| `-` | Feld wird nicht persistiert |

**NULL-Fähigkeit** ergibt sich aus dem Go-Typ, nicht aus einem Tag: Nicht-Pointer-Felder sind `NOT NULL` (der Go-Zero-Value ist der Default), Pointer-Felder (`*string`, `*time.Time`) erlauben `NULL`. `required` verschärft das für Nicht-Pointer-Felder: Auch der Zero-Value ist beim Insert unzulässig — der Wert *muss* bewusst gesetzt werden.

`tenant_id` und die Geo-Spalten deklariert man **nicht** — sie sind implizit in jeder Tabelle vorhanden (außer bei `TenantFree`-Modellen) und werden ausschließlich über den Context gesteuert.

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

### 5.4 Referenzen zwischen Modellen

Beziehungen werden per `ref`-Tag deklariert und mit derselben Doppel-Durchsetzung abgesichert wie der Tenant: Engine-Prüfung auf allen Backends plus FK-Constraint, wo die DB das nativ kann — Verhalten überall identisch.

```go
type Document struct {
    ID        orm.ID `orm:"pk"`
    Title     string `orm:"required"`
    CreatedBy orm.ID `orm:"ref=User,immutable,required"`   // Ersteller: Pflicht, unveränderlich
    ProjectID orm.ID `orm:"ref=Project,ondelete=cascade"`  // Dokument stirbt mit dem Projekt
    ReviewerID *orm.ID `orm:"ref=User"`                    // optional (Pointer ⇒ NULL erlaubt)
}
```

**Regeln:**

1. **Insert/Update-Verifikation:** Der referenzierte Datensatz muss existieren — sonst `orm.ErrInvalidReference`. Geprüft im selben Schritt wie die Tenant-Verifikation.
2. **Tenant-Kopplung:** Referenzen dürfen nur auf Datensätze **desselben Tenants** zeigen (Ausnahme: das Ziel-Model ist `TenantFree` oder `GeoGlobal`-Stammdaten). Ein `TenantFree`-Model darf **nicht** auf ein tenant-gebundenes Model verweisen — das lehnt bereits die Registrierung ab, denn ohne Tenant-Scope wäre die Referenz nicht eindeutig prüfbar.
3. **Löschverhalten** (`ondelete`, Default `restrict`): `restrict` — Löschen des Ziels schlägt fehl, solange Verweise existieren (`orm.ErrReferenceInUse`); `cascade` — abhängige Datensätze werden mitgelöscht; `setnull` — Referenzfeld wird `NULL` (nur bei Pointer-Feldern zulässig, sonst Registrierungsfehler).
4. **Ziel-Typen:** Referenzen zeigen immer auf den Primärschlüssel. Ziel darf auch ein ES-Model sein — geprüft wird gegen dessen Read-Model; `ondelete`-Aktionen löst dort das Lösch-Event aus.
5. **Geo:** Referenzen über Regionsgrenzen sind erlaubt (z. B. auf `GeoGlobal`-Stammdaten immer lokal prüfbar); bei `GeoScoped`-Zielen in fremden Regionen prüft die Engine remote — mit Latenz, aber korrekt (Grundprinzip Verhaltensgleichheit).

Kein Eager-Loading in v1 — Referenzen sind Integritätswerkzeug; geladen wird explizit (`orm.Repo[User](db).Get(ctx, doc.CreatedBy)`). Komfort-Loading (Joins/Preload) ist Stufe 2.

### 5.5 Feld-Verschlüsselung

Felder mit dem Tag `encrypted` werden von der Engine vor dem Schreiben verschlüsselt (AES-256-GCM) und beim Lesen transparent entschlüsselt — auf allen Backends identisch, die DB sieht nur Ciphertext (`BYTEA`/`BLOB`).

```go
type ProviderAccount struct {
    ID     orm.ID `orm:"pk"`
    Name   string `orm:"index"`
    APIKey string `orm:"encrypted,required"`
}

db, err := orm.Open(orm.Postgres(dsn),
    orm.Encryption(orm.StaticKey(keyFromKMS)),   // Pflicht, sobald ein Model `encrypted` nutzt
)
```

**Regeln:**

- `orm.Encryption(provider)` ist eine `Open`-Option; ohne sie schlägt die Registrierung eines Models mit `encrypted`-Feldern fehl. `orm.StaticKey([]byte)` ist der einfachste Provider; das `orm.KeyProvider`-Interface (aktueller Schlüssel + Lookup per Key-ID) ist von Tag 1 rotationsfähig — jeder Ciphertext trägt die ID des benutzten Schlüssels, Rotation erfolgt lazy beim nächsten Schreiben.
- Verschlüsselte Felder sind **nicht indizierbar und nicht filterbar** (`Where` auf ein `encrypted`-Feld ⇒ Registrierungs-/Query-Fehler) — die DB kann Ciphertext nicht sinnvoll vergleichen.
- Das Tag wirkt auch in Event-Payloads und Snapshots von ES-Modellen: markierte Felder liegen dort ebenfalls nur verschlüsselt.

---

## 6. CRUD-Modelle

### 6.1 Repository

```go
func Repo[T any](h Handle) Repository[T]     // Handle: *DB oder Tx
```

```go
type Repository[T any] interface {
    Insert(ctx context.Context, entity *T) error          // füllt pk/autocreate
    InsertMany(ctx context.Context, entities []*T, opts ...BatchOption) error
    Get(ctx context.Context, id orm.ID) (*T, error)       // ErrNotFound
    GetForUpdate(ctx context.Context, id orm.ID) (*T, error)  // nur in Tx (sonst ErrRequiresTx)
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
    Iter() iter.Seq2[*T, error]                        // Streaming: Cursor statt Speicher
    First() (*T, error)                                 // ErrNotFound
    Count() (int64, error)
    Exists() (bool, error)
    UpdateSet(sets ...Set) (int64, error)              // mengenbasiertes Update (orm.Set(field, v))
    Delete() (int64, error)                            // mengenbasiertes Löschen
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

### 6.3 Batch & Bulk

```go
// Atomar (Default): alle oder keiner — eine Transaktion.
err := accounts.InsertMany(ctx, accs)

// Große Volumina: in Chunks, jeder Chunk eine eigene Transaktion.
// Rückgabefehler nennt die Zahl der bereits eingefügten Zeilen.
err = accounts.InsertMany(ctx, million, orm.Chunked(10_000))

// Mengenbasiert ändern/löschen — ein Statement, kein N×Roundtrip:
n, err := accounts.Query(ctx).
    Where(orm.Eq("Status", "trial")).
    UpdateSet(orm.Set("Status", "expired"))

n, err = accounts.Query(ctx).Where(orm.Lt("CreatedAt", cutoff)).Delete()
```

**Die Einfüge-Strategie wählt der Dialekt-Adapter, nicht der Aufrufer** (Grundprinzip): Postgres nutzt Multi-Row-`INSERT ... VALUES` und schaltet ab einer Schwelle auf `COPY`; Yugabyte batcht passend zur Tablet-Verteilung; SQLite fährt Prepared Statements in einer Transaktion. Tenant-, Referenz-, `enum`- und `required`-Prüfungen gelten in jedem Pfad — auch unter `COPY` (engine-seitige Validierung vor dem Schreiben). Mengenbasierte `UpdateSet`/`Delete` respektieren selbstverständlich Tenant-/Geo-Scope und lösen `ondelete`-Regeln aus.

### 6.4 Pessimistisches Sperren

Für Read-Modify-Write-Muster, bei denen optimistisches Locking (Retry) nicht passt:

```go
err := db.Tx(ctx, func(tx orm.Tx) error {
    acc, err := orm.Repo[Account](tx).GetForUpdate(ctx, id)   // Zeile gesperrt
    if err != nil { return err }
    acc.Balance -= amount
    return orm.Repo[Account](tx).Update(ctx, acc)
})
```

`SELECT ... FOR UPDATE` auf Postgres/Yugabyte; SQLite emuliert über die serialisierte Schreib-Connection — verhaltensgleich. Außerhalb einer Transaktion ⇒ `orm.ErrRequiresTx`.

### 6.5 Transaktionen

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
- **Geo-Pinning:** Das Daten-Geo klebt ab dem ersten Event am Aggregat. `WithGeo` bestimmt die Heimatregion bei der **Entstehung**; Folge-Appends schreiben immer in die Heimat-Partition, unabhängig vom Context-Geo. Umzug ist eine explizite Operation (`SetGeo`, GeoFlexible).
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

// AtVersion/AtTime liefern ein neues Objekt des Model-Typs (statisch `any`,
// da Go keine generischen Methoden kennt — bei Bedarf auf *DNSZone casten):
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
    orm.Named("zone-dashboard"),   // stabiler Name für Checkpoint & RebuildView
)
```

Handler müssen **idempotent** sein (at-least-once). Muster: Event-ID in der Ziel-View mitschreiben und Duplikate ignorieren. `orm.Named` gibt dem Konsumenten einen stabilen Namen (Checkpoint-Schlüssel, `RebuildView`-Referenz); ohne Angabe wird Model-Name + Pattern verwendet.

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

**Archivierung:** Events unterhalb des zweitjüngsten Snapshots wandern automatisch in Archiv-Tabellen (Worker, lease-koordiniert, nie über den Projektionsstand hinaus). Für die API ist das unsichtbar: `Load` faltet Snapshot + Hot-Log; `History`, `AtVersion`/`AtTime`, `Stream` und Rebuilds lesen transparent Hot **und** Archiv.

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
- **Entfallende Felder** werden mit `deprecated` markiert, nie automatisch gelöscht. Die Spalte fällt bei `FinalizeMigration` der Version, in der das Feld auch aus dem Struct entfernt wurde.
- **Drift-Schutz:** Modelle geändert ohne Versions-Erhöhung ⇒ Startfehler (Checksum-Vergleich).
- **`ReplaceModel`-Konventionen:** Der Go-Name des Alt-Structs ist der frühere Model-Name mit Versions-Suffix — `ZoneV2` liest die Tabelle des früheren Models `Zone` (Suffix `V<n>` wird für die Tabellen-Ableitung gestrichen). Tenant, Geo und — sofern die Transformation keine setzt — die ID bleiben über den Umbau erhalten. `required`/`enum`-Constraints des Ziel-Models gelten auch im Backfill. Ziel muss ein CRUD-Model sein (ES-Umbauten laufen über Events/Upcaster).
- **`BatchScript`-Checkpoint:** Das Skript arbeitet mit den normalen ORM-APIs und sichert seinen Fortschritt über `b.Checkpoint(ctx)` / `b.SaveCheckpoint(ctx, key, rowsDone)` — bei Wiederaufnahme (Absturz, Neustart) liest es den Schlüssel zurück und setzt dort fort. Erfolgreiche Rückkehr markiert den Schritt als erledigt.

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
- **dual-write:** alte Instanzen laufen unverändert weiter und schreiben in die alte Struktur; ihre Änderungen (Insert/Update/Delete) werden per Trigger-Nachlauf laufend in die neue Struktur nachgezogen (Worker verarbeitet die Queue, at-least-once). Beide App-Generationen koexistieren. Die Rückrichtung — neue Instanzen schreiben zusätzlich in die alte Struktur — erfordert eine Rück-Transformation und ist als optionale `ReplaceModel`-Erweiterung geplant.
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

Startet die Hintergrund-Verarbeitung dieser Instanz: Projektionen, `OnEvent`-Reaktoren, Snapshot- und Archiv-Worker, ggf. Migrations-Shards. Koordination über **Leases mit Fencing** in der DB — pro Aufgabe (Projektion je Model, View, Dual-Write-Nachlauf) arbeitet clusterweit genau eine Instanz, sticky; fällt sie aus (Lease-TTL bzw. `Close`), übernimmt eine andere. `ctx`-Abbruch oder `db.Close()` stoppt die Worker sauber.

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
| `orm.ErrUnknownTenant` | Tenant-ID existiert nicht im Register oder ist archiviert |
| `orm.ErrRequiredField` | `required`-Feld beim Insert nicht gesetzt (Zero-Value) |
| `orm.ErrInvalidReference` | `ref`-Ziel existiert nicht oder gehört zu einem anderen Tenant |
| `orm.ErrReferenceInUse` | Löschen verweigert: Datensatz wird noch referenziert (`ondelete=restrict`) |
| `orm.ErrInvalidValue` | Wert außerhalb der `enum`-Wertemenge |
| `orm.ErrRequiresTx` | Operation (z. B. `GetForUpdate`) außerhalb einer Transaktion |
| `orm.ErrTenantNotArchived` | `Purge` auf einen nicht archivierten Tenant |
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

    ctx = orm.WithTenant(ctx, orm.SingleTenant) // beim Bootstrap angelegter Default-Tenant

    todos := orm.Repo[Todo](db)
    t := &Todo{Title: "ORM++ bauen"}
    _ = todos.Insert(ctx, t)

    open, _ := todos.Query(ctx).Where(orm.Eq("Done", false)).All()
    _ = open
}
```

Dieselbe Datei läuft unverändert gegen `orm.Yugabyte(dsn)` in einem geo-partitionierten Cluster — das ist das Versprechen von ORM++.
