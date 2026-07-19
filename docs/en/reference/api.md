---
title: API reference
description: Complete specification of ORM++'s public Go API.
sidebar:
  order: 4
---

This page is the complete implementation contract of the public Go API —
every signature documented here is implemented and runs behaviourally
identical on SQLite, PostgreSQL and YugabyteDB (identical test suite, CI
matrix). For a hands-on start with examples, see the
[guides](/en/guides/models/); for architecture background and the physical
schema, see [Architecture](/en/reference/architecture/).

```go
import orm "gitlab.techeve.de/orm-plus-plus/orm-plus-plus"
```

## 1. Core principle

**Behavioural equality across all backends.** For the consuming application it is irrelevant which database sits underneath (SQLite, PostgreSQL, YugabyteDB). Every declaration is accepted and semantically fulfilled on every backend — natively where the DB can, emulated or collapsed where it cannot. App code **never** branches on the backend; the same application runs byte-for-byte identically on all three.

Consequences for the API:

- There is no function that reveals the backend (no `db.Kind()`). The one exception: the observability APIs (Section 10) show the *operator* the physical truth.
- A topology with five regions on SQLite is valid — SQLite implicitly has the single region `local`, and all declared regions map onto it. Responses from the behavioural API stay semantically correct.
- The application writes **no SQL** and knows no table, driver or dialect details.

---

## 2. Instantiation & connection

### 2.1 Open

```go
func Open(driver Driver, opts ...OpenOption) (*DB, error)
```

Builds the connection pool, validates the instance geo against the topology, registers the instance in the instance registry (with heartbeat) and loads the type dictionary. `Open` performs **no** schema changes — that is `Migrate`'s job (Section 8).

**Drivers:**

```go
orm.SQLite(path string)      // embedded; demo/desktop/tests. WAL mode, one write connection.
orm.Postgres(dsn string)     // server, on-prem (pgx pool)
orm.Yugabyte(dsn string)     // distributed; DSN should include the regionally close endpoints
```

**Options:**

| Option | Meaning | Default |
|---|---|---|
| `orm.InstanceGeo(geo string)` | Region this process runs in. Determines which background work (projection/migration shards) the instance may take on. | `"local"` |
| `orm.MigrationRole(role)` | `orm.MigrationNone` \| `orm.MigrationWorker` — may this instance take on backfill shards? | `MigrationNone` |
| `orm.AppVersion(v string)` | Application version, recorded in the instance registry (ops view). | `""` |
| `orm.DefaultSnapshotEvery(n int)` | Global snapshot default for all ES models. | `100` |
| `orm.EventTypePrefix(p string)` | Prefix for CloudEvents types, e.g. `"de.techeve.dns"`. | module-path based |

**Example — smallest sensible app (SQLite, no geo, no multi-tenant operation):**

```go
db, err := orm.Open(orm.SQLite("./app.db"))
if err != nil { ... }
defer db.Close()
```

**Example — cluster instance (Yugabyte, US-East region, allowed to migrate):**

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

Deregisters the instance (leases released, workers stopped) and closes the pool.

---

## 3. Topology (geo regions)

The topology describes **which regions the cluster has**. It is cluster state (table `ormpp_geo_regions`), not instance configuration: every instance declares the same topology (same binary); whoever wins the bootstrap lease during `Migrate` applies the diff. There is no special "first instance".

```go
func Topology(db *DB, regions ...RegionDecl)
func Region(name string, opts ...RegionOption) RegionDecl

orm.Placement(cloudPlacement string)   // YB: tablespace/placement mapping
```

**Example:**

```go
orm.Topology(db,
    orm.Region("eu-central", orm.Placement("cloud1.eu-central-1")),
    orm.Region("us-east",    orm.Placement("cloud1.us-east-1")),
    orm.Region("ap-south",   orm.Placement("cloud1.ap-south-1")),
)
```

**Rules:**

- No `Topology` declaration ⇒ implicit region `local`. The simplest case needs zero geo code.
- **Add a region** = one more `orm.Region(...)` line + rollout. Additive, no special mode. The new region runs through the lifecycle `bootstrapping → active`; while `bootstrapping`, `GeoGlobal` models and `ReplicateAll` records get replicated in; writes into the region are fail-closed blocked until `active`.
- **Remove a region** = set status `draining` (an ops action, also available via API in stage 2): no new data; records *homed* in the region move out via geo-distributed backfill; replicas are discarded; only then `removed`.
- On SQLite/single-region Postgres all regions collapse onto one — the declaration stays valid (core principle).

---

## 4. Context: tenant & geo

Tenant and geo are the **mandatory scope of every data operation** and hang off the `context.Context` — never off function signatures.

```go
func WithTenant(ctx context.Context, tenant uuid.UUID) context.Context
func WithGeo(ctx context.Context, home string, opts ...GeoOption) context.Context

orm.ReplicateTo(regions ...string)   // copies in further regions (GeoFlexible only)
orm.ReplicateAll()                   // copies in all active regions, follows the topology
```

**Rules:**

- Missing tenant ⇒ `orm.ErrNoTenant` (fail-closed). Nothing runs tenant-less by accident. Single-tenant apps set a constant tenant once at startup.
- Missing geo ⇒ the topology's default region (`local` if no topology is declared); with a multi-region topology and no default ⇒ `orm.ErrNoGeo`.
- `WithGeo` sets the **data geo** (where a record belongs) — independent of the instance geo. An EU instance may write a US record; it lands correctly in the US partition (with latency).
- Geo pointing at a non-active region ⇒ `orm.ErrRegionNotActive`.

**Example — typical request handler:**

```go
func (s *Server) handle(w http.ResponseWriter, r *http.Request) {
    ctx := orm.WithTenant(r.Context(), tenantFromAuth(r))
    ctx = orm.WithGeo(ctx, regionFromUser(r))
    // ... all ORM calls with this ctx
}
```

### 4.1 Tenant registry (built in)

Tenants do not come from the app — ORM++ ships them as a system model (`ormpp_tenants`, created at bootstrap, `GeoGlobal` so every region can validate locally):

```go
tenants := db.Tenants()

t, err := tenants.Create(ctx, orm.TenantInfo{Name: "ACME GmbH"})   // ID: UUIDv7
t, err  = tenants.Get(ctx, id)
list, err := tenants.List(ctx)
err     = tenants.Archive(ctx, id)   // no hard delete: blocks new writes,
                                     // existing data stays readable

// GDPR: full data export of a tenant (JSON Lines, all models
// incl. events, snapshots and archive):
err = tenants.Export(ctx, id, w)

// Right to be forgotten: physical deletion of ALL of the tenant's data across
// every table, event, snapshot and archive. Only the engine knows all the
// locations. Two-stage: tenant must be archived (else
// ErrTenantNotArchived); the operation is audited in ormpp_schema_history.
err = tenants.Purge(ctx, id)
```

`orm.SingleTenant` is a tenant created automatically at bootstrap for single-tenant apps.

**Tenant rules (not switchable off):**

1. **Insert verification:** every `Insert`/`Append` checks the context tenant ID against the registry — unknown or archived ⇒ `orm.ErrUnknownTenant`. Enforced engine-side (an in-memory cache of the registry, invalidated via the event chain) plus an FK constraint where the DB supports it natively; behaviour is identical across all backends.
2. **Write-once:** `tenant_id` is set at insert and immutable afterwards. The engine never includes the field in an `UPDATE`; there is no API to reassign a record to another tenant (tenant merging would be an explicit, audited admin operation, stage 2).
3. **Scope in every operation — including by ID:** all queries, updates and deletes automatically filter on the context tenant; including `Get`/`Update`/`Delete` by primary key. A record of a foreign tenant addressed by ID behaves exactly as if it didn't exist (`ErrNotFound`). Non-ID filters without a tenant are impossible by construction (fail-closed context).

---

## 5. Declaring models

### 5.1 Registration

```go
func Register[T any](db *DB, mode ModelMode, opts ...ModelOption)

orm.CRUD()            // classic persistence
orm.EventSourced()    // event sourcing (struct must embed orm.Aggregate)
```

Registration happens at startup, before `Migrate`. The registry validates the struct (tags, PK, embedding) and compiles the mapping plan once (no reflection in the hot path).

**Scope options:**

| Option | Meaning |
|---|---|
| `orm.TenantFree()` | Model without a tenant column and without a tenant filter — for **technical tables holding no user data** (configuration, lookup values, job state). Operations work even with a context that has no tenant. Tenant-scoped models remain the default; this option is the documented exception. |

**Composite indexes & constraints** (model options; field names are Go field names):

```go
orm.Register[Record](db, orm.CRUD(),
    orm.Unique("ProjectID", "Name"),       // unique constraint across several columns
    orm.Index("Status", "CreatedAt"),      // composite secondary index
)
```

Tenant and geo columns are automatically included in unique constraints (uniqueness applies per tenant). Note for YugabyteDB: secondary indexes are distributed indexes there with corresponding write costs — the registry accepts the declaration unchanged, behaviour is identical; the cost trade-off is documented in the operations chapter.

**Geo modes** (model option, default `GeoScoped`):

| Option | Meaning |
|---|---|
| `orm.GeoScoped()` | Each record lives in exactly one region (the normal case). |
| `orm.GeoGlobal()` | Model present in **all** regions (master data: tenants, users, plans). YB: native replica placement + follower reads. Writes pay cross-region consensus — unsuitable for write-heavy models (the registry warns). |
| `orm.GeoFlexible(opts...)` | **Per record**: home region + read replicas. Sub-options: `orm.WriteForwarding()` (an update on a replica is forwarded to home) or `orm.WriteHomeOnly()` (error). |

**Snapshot options** (ES models only, override the global default):

```go
orm.SnapshotEvery(n int)             // every n events per aggregate
orm.SnapshotMaxAge(d time.Duration)  // OR condition: at the latest after d
orm.SnapshotKeepLast(n int)          // retention, default 2
orm.SnapshotDisabled()               // pure logs, never loaded folded
```

### 5.2 Struct tags (CRUD models and read models)

```go
type ProviderAccount struct {
    ID        orm.ID    `orm:"pk"`
    Name      string    `orm:"index"`
    Email     string    `orm:"unique"`
    Labels    []string  `orm:"json"`
    Version   int64     `orm:"version"`      // optimistic locking
    CreatedAt time.Time `orm:"autocreate"`
    UpdatedAt time.Time `orm:"autoupdate"`
    Notes     string    `orm:"deprecated"`   // marked, dropped at finalisation
}
```

| Tag | Meaning |
|---|---|
| `pk` | Primary key (exactly one; `orm.ID` is UUIDv7) |
| `index`, `unique` | Secondary/unique index |
| `json` | Nested values as a JSON(B) column (v1; sub-tables in stage 2) |
| `version` | Column for optimistic locking on `Update` |
| `autocreate`, `autoupdate` | Timestamp maintenance by the engine |
| `ref=Model[,ondelete=…]` | Reference to another model (Section 5.4) |
| `enum=a\|b\|c` | Value set for string fields: native CHECK constraint where supported, engine-checked everywhere — invalid value ⇒ `orm.ErrInvalidValue` |
| `default=…` | Default value when the field holds the zero value at insert (not combinable with `required`) |
| `encrypted` | Field is stored encrypted (Section 5.5) |
| `immutable` | Write-once: set at insert, immutable afterwards — the engine never includes the field in an `UPDATE` (same behaviour as `tenant_id`) |
| `required` | Must be explicitly set at insert: zero value ⇒ `orm.ErrRequiredField` |
| `deprecated` | Field is marked for removal (expand/contract, Section 8) |
| `-` | Field is not persisted |

**Nullability** follows the Go type, not a tag: non-pointer fields are `NOT NULL` (the Go zero value is the default), pointer fields (`*string`, `*time.Time`) allow `NULL`. `required` tightens this for non-pointer fields: even the zero value is disallowed at insert — the value *must* be deliberately set.

You do **not** declare `tenant_id` or the geo columns — they are implicitly present in every table (except `TenantFree` models) and are controlled exclusively via the context.

### 5.3 Full declaration example

```go
// CRUD, tenant-scoped, one region per record:
orm.Register[ProviderAccount](db, orm.CRUD())

// CRUD master data, present everywhere:
orm.Register[Tenant](db, orm.CRUD(), orm.GeoGlobal())

// CRUD, region selectable per record:
orm.Register[SyncGroup](db, orm.CRUD(),
    orm.GeoFlexible(orm.WriteForwarding()),
)

// Event-sourced (details in Section 7):
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

### 5.4 References between models

Relationships are declared via the `ref` tag and secured with the same double enforcement as tenant: engine check on all backends plus an FK constraint where the DB supports it natively — behaviour identical everywhere.

```go
type Document struct {
    ID        orm.ID `orm:"pk"`
    Title     string `orm:"required"`
    CreatedBy orm.ID `orm:"ref=User,immutable,required"`   // creator: required, immutable
    ProjectID orm.ID `orm:"ref=Project,ondelete=cascade"`  // document dies with the project
    ReviewerID *orm.ID `orm:"ref=User"`                    // optional (pointer ⇒ NULL allowed)
}
```

**Rules:**

1. **Insert/update verification:** the referenced record must exist — otherwise `orm.ErrInvalidReference`. Checked in the same step as tenant verification.
2. **Tenant coupling:** references may only point to records of the **same tenant** (exception: the target model is `TenantFree` or `GeoGlobal` master data). A `TenantFree` model may **not** reference a tenant-scoped model — registration already rejects that, because without a tenant scope the reference couldn't be checked unambiguously.
3. **Delete behaviour** (`ondelete`, default `restrict`): `restrict` — deleting the target fails while references exist (`orm.ErrReferenceInUse`); `cascade` — dependent records are deleted too; `setnull` — the reference field becomes `NULL` (allowed only on pointer fields, otherwise a registration error).
4. **Target types:** references always point at the primary key. The target may also be an ES model — checked against its read model; `ondelete` actions there are triggered by the delete event.
5. **Geo:** references across region boundaries are allowed (e.g. `GeoGlobal` master data is always checkable locally); for `GeoScoped` targets in foreign regions the engine checks remotely — with latency, but correctly (the behavioural-equality core principle).

No eager loading in v1 — references are an integrity tool; loading is explicit (`orm.Repo[User](db).Get(ctx, doc.CreatedBy)`). Convenience loading (joins/preload) is stage 2.

### 5.5 Field encryption

Fields with the `encrypted` tag are encrypted by the engine before writing (AES-256-GCM) and transparently decrypted on read — identical on all backends, the DB only ever sees ciphertext (`BYTEA`/`BLOB`).

```go
type ProviderAccount struct {
    ID     orm.ID `orm:"pk"`
    Name   string `orm:"index"`
    APIKey string `orm:"encrypted,required"`
}

db, err := orm.Open(orm.Postgres(dsn),
    orm.Encryption(orm.StaticKey(keyFromKMS)),   // required as soon as a model uses `encrypted`
)
```

**Rules:**

- `orm.Encryption(provider)` is an `Open` option; without it `Migrate` fails for models with `encrypted` fields. `orm.StaticKey([]byte)` (32 bytes) is the simplest provider; the `orm.KeyProvider` interface (current key + lookup by key ID) is rotation-ready from day one — every ciphertext carries the ID of the key used, rotation happens lazily on the next write.
- `encrypted` applies to `string` and `[]byte` fields (also pointers) and is not combinable with `pk`/`index`/`unique`/`json`/`version`/`default`/`enum`/`ref`.
- Encrypted fields are **not indexable, filterable or sortable** (`Where`/`OrderBy` on an `encrypted` field ⇒ query error) — the DB cannot meaningfully compare ciphertext. `UpdateSet` encrypts engine-side.
- **v1 scope:** `encrypted` works on CRUD models. On event-sourced models (event payloads, snapshots) it is currently rejected at `Migrate` and follows in a later version.

---

## 6. CRUD models

### 6.1 Repository

```go
func Repo[T any](h Handle) Repository[T]     // Handle: *DB or Tx
```

```go
type Repository[T any] interface {
    Insert(ctx context.Context, entity *T) error          // fills pk/autocreate
    InsertMany(ctx context.Context, entities []*T, opts ...BatchOption) error
    Get(ctx context.Context, id orm.ID) (*T, error)       // ErrNotFound
    GetForUpdate(ctx context.Context, id orm.ID) (*T, error)  // Tx only (else ErrRequiresTx)
    Update(ctx context.Context, entity *T) error          // ErrVersionConflict with `version` tag
    Upsert(ctx context.Context, entity *T) error
    Delete(ctx context.Context, id orm.ID) error
    SetGeo(ctx context.Context, id orm.ID, home string, opts ...GeoOption) error  // GeoFlexible only
    Query(ctx context.Context) QueryBuilder[T]
}
```

**Example:**

```go
accounts := orm.Repo[ProviderAccount](db)

acc := &ProviderAccount{Name: "Cloudflare Prod", Email: "ops@example.org"}
err := accounts.Insert(ctx, acc)          // acc.ID, acc.CreatedAt filled

acc, err = accounts.Get(ctx, acc.ID)
acc.Name = "Cloudflare Production"
err = accounts.Update(ctx, acc)           // version conflict ⇒ ErrVersionConflict
err = accounts.Delete(ctx, acc.ID)
```

**Example — GeoFlexible record with replicas:**

```go
groups := orm.Repo[SyncGroup](db)

ctx := orm.WithTenant(ctx, tenant)
ctx = orm.WithGeo(ctx, "eu-central", orm.ReplicateTo("us-east", "ap-south"))
err := groups.Insert(ctx, g)              // home EU, read copies US + AP

// Move later / change replicas (engine-managed):
err = groups.SetGeo(ctx, g.ID, "us-east", orm.ReplicateTo("eu-central"))
```

Reads are **locality-preferred**: if a copy exists in the reading instance's region, it comes from there; otherwise the home region answers.

### 6.2 Query builder

```go
type QueryBuilder[T any] interface {
    Where(cond Cond) QueryBuilder[T]
    OrderBy(field string, dir Dir) QueryBuilder[T]     // orm.Asc | orm.Desc
    Limit(n int) QueryBuilder[T]
    Offset(n int) QueryBuilder[T]
    All() ([]*T, error)
    Iter() iter.Seq2[*T, error]                        // streaming: cursor instead of memory
    First() (*T, error)                                 // ErrNotFound
    Count() (int64, error)
    Exists() (bool, error)
    UpdateSet(sets ...Set) (int64, error)              // set-based update (orm.Set(field, v))
    Delete() (int64, error)                            // set-based delete
}
```

**Conditions:**

```go
orm.Eq(field, v)   orm.Ne(field, v)
orm.Gt(field, v)   orm.Gte(field, v)   orm.Lt(field, v)   orm.Lte(field, v)
orm.Like(field, pattern)               orm.In(field, vs...)
orm.IsNull(field)  orm.NotNull(field)
orm.And(conds...)  orm.Or(conds...)    orm.Not(cond)
```

Field names are the struct's Go field names (the engine maps them to columns). Unknown fields ⇒ error at build time, not at runtime in the DB.

**Example:**

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

The tenant filter (and geo scope) is **always** injected automatically — it cannot be switched off and never appears in any query.

### 6.3 Batch & bulk

```go
// Atomic (default): all or nothing — one transaction.
err := accounts.InsertMany(ctx, accs)

// Large volumes: in chunks, each chunk its own transaction.
// The returned error names the number of rows already inserted.
err = accounts.InsertMany(ctx, million, orm.Chunked(10_000))

// Set-based change/delete — one statement, no N×round-trip:
n, err := accounts.Query(ctx).
    Where(orm.Eq("Status", "trial")).
    UpdateSet(orm.Set("Status", "expired"))

n, err = accounts.Query(ctx).Where(orm.Lt("CreatedAt", cutoff)).Delete()
```

**The insert strategy is chosen by the dialect adapter, not the caller** (core principle): Postgres uses multi-row `INSERT ... VALUES` and switches to `COPY` past a threshold; Yugabyte batches to match tablet distribution; SQLite runs prepared statements in one transaction. Tenant, reference, `enum` and `required` checks apply in every path — even under `COPY` (engine-side validation before writing). Set-based `UpdateSet`/`Delete` naturally respect tenant/geo scope and trigger `ondelete` rules.

### 6.4 Pessimistic locking

For read-modify-write patterns where optimistic locking (retry) doesn't fit:

```go
err := db.Tx(ctx, func(tx orm.Tx) error {
    acc, err := orm.Repo[Account](tx).GetForUpdate(ctx, id)   // row locked
    if err != nil { return err }
    acc.Balance -= amount
    return orm.Repo[Account](tx).Update(ctx, acc)
})
```

`SELECT ... FOR UPDATE` on Postgres/Yugabyte; SQLite emulates via the serialised write connection — behaviourally identical. Outside a transaction ⇒ `orm.ErrRequiresTx`.

### 6.5 Transactions

```go
func (db *DB) Tx(ctx context.Context, fn func(tx orm.Tx) error) error
```

```go
err := db.Tx(ctx, func(tx orm.Tx) error {
    if err := orm.Repo[ProviderAccount](tx).Insert(ctx, acc); err != nil { return err }
    return orm.Repo[AuditNote](tx).Insert(ctx, note)
})
```

Rollback on error return or panic. Nested `Tx` use savepoints. Event appends (Section 7) may sit in the same transaction as CRUD writes.

---

## 7. Event-sourcing models

### 7.1 Declaration

An ES model consists of: the state (a struct embedding `orm.Aggregate`), the event payloads, and exactly one mandatory `Apply` function.

```go
type DNSZone struct {
    orm.Aggregate                       // brings ID/Version/Load/History/… along

    Name    string   `orm:"index,unique"`
    Records []Record `orm:"json"`
    Status  string   `orm:"index"`
}

// Event payloads: pure data structs, only the delta.
type ZoneCreated struct{ Name string }
type RecordAdded struct{ Record Record }
type ZoneDeleted struct{}

// The developer's only duty — pure function, no DB access.
// It is projection, rebuild AND snapshot logic in one.
func (z *DNSZone) Apply(e orm.Event) error {
    switch ev := e.Payload.(type) {
    case ZoneCreated:  z.Name, z.Status = ev.Name, "active"
    case RecordAdded:  z.Records = append(z.Records, ev.Record)
    case ZoneDeleted:  z.Status = "deleted"
    }
    return nil
}
```

Events are named at registration (Section 5.3). The full CloudEvents type is `EventTypePrefix + Name + ".v" + Version`, e.g. `de.techeve.dns.zone.record_added.v1`.

### 7.2 Writing: append

```go
// New aggregate:
zone := orm.New[DNSZone](db)                       // creates ID (UUIDv7)
pos, err := zone.Append(ctx, ZoneCreated{Name: "example.org"})

// Existing aggregate: load, then append.
zone, err := orm.Load[DNSZone](ctx, db, zoneID)
pos, err = zone.Append(ctx, RecordAdded{Record: rec})
```

- `Append` appends one or more events **atomically** (one call = one transaction) and implicitly expects the loaded aggregate version — if someone came in between: `orm.ErrVersionConflict`. Then: `Refresh` + decide + append again.
- **Geo pinning:** the data geo sticks to the aggregate from its first event. `WithGeo` sets the home region at **creation**; subsequent appends always write to the home partition, regardless of the context geo. Moving is an explicit operation (`SetGeo`, GeoFlexible).
- The return value `pos orm.Position` is the event position (consistency token for `WaitFor`).
- "Deletion" is an event (`ZoneDeleted{}`) — the history stays. Physical deletion (e.g. for GDPR) is an explicit admin operation (stage 2).
- Every `Append` automatically triggers the chain: built-in projection → `OnEvent` reactors → `Watch` streams.

### 7.3 Reading: current state

```go
// From the read model (fast; may lag milliseconds behind the last append):
zone, err := orm.Load[DNSZone](ctx, db, zoneID)

// Read-your-writes: wait until the projection has reached your own write position:
zone, err = orm.Load[DNSZone](ctx, db, zoneID, orm.WaitFor(pos, 2*time.Second))

// Query builder — identical to CRUD, runs against the read model:
active, err := orm.Query[DNSZone](db, ctx).
    Where(orm.Eq("Status", "active")).
    OrderBy("Name", orm.Asc).
    All()
```

`Load` internally loads: last snapshot + remaining events through `Apply` — transparent, the caller notices nothing of it.

### 7.4 Reading: history & time travel

All these functions exist out of the box through the `orm.Aggregate` embedding and transparently access snapshots and archive partitions:

```go
zone.ID()          zone.Version()          zone.UpdatedAt()

// AtVersion/AtTime return a new object of the model type (statically `any`,
// since Go has no generic methods — cast to *DNSZone if needed):
old, err := zone.AtVersion(ctx, 42)         // state after event 42
old, err  = zone.AtTime(ctx, timestamp)     // state at a point in time

// Event stream of an aggregate (audit, drift history) — as CloudEvents:
for ev, err := range zone.History(ctx) {
    fmt.Println(ev.Sequence, ev.Type, ev.Time, ev.Data)
}

err = zone.Refresh(ctx)                                  // reload
err = zone.Refresh(ctx, orm.WaitFor(pos, time.Second))   // with a consistency token
```

**Event stream across all aggregates of a model** (for custom consumers/integrations):

```go
for ev, err := range orm.Stream[DNSZone](ctx, db, orm.From(pos)) { ... }
```

Ordering guarantees: strict per aggregate; monotonic per region. No total order across regions (`orm.Position` is a cursor vector, one per geo).

### 7.5 Triggers: reactors & live streams

**`OnEvent` — the reliable path.** Persistent, at-least-once, checkpointed, lease-coordinated, rebuildable. For derived read views, search indexes, notification fan-out:

```go
orm.OnEvent[DNSZone](db, "zone.*",
    func(ctx context.Context, ce orm.CloudEvent, tx orm.Tx) error {
        return updateZoneDashboardView(ctx, tx, ce)   // runs transactionally
    },
    orm.Named("zone-dashboard"),   // stable name for checkpoint & RebuildView
)
```

Handlers must be **idempotent** (at-least-once). Pattern: write the event ID into the target view and ignore duplicates. `orm.Named` gives the consumer a stable name (checkpoint key, `RebuildView` reference); without it, model name + pattern is used.

**`Watch` — the fast path.** Ephemeral live notification to connected clients (SSE/WebSocket); whoever isn't listening misses nothing permanent:

```go
for ce := range orm.Watch[DNSZone](ctx, db, orm.From(pos)) {
    hub.Broadcast(ce)
}
```

**Rebuild** of a projection or view from the event stream:

```go
err := orm.RebuildProjection[DNSZone](ctx, db)
err  = orm.RebuildView(ctx, db, "zone-dashboard")     // named OnEvent consumer
```

### 7.6 Event schema versions: upcasters

Events are immutable. When a format changes, a new version is registered and an upcaster transforms old events **on read**:

```go
// Registration: RecordAdded is now v2.
orm.Register[DNSZone](db, orm.EventSourced(),
    orm.Events(
        orm.E[ZoneCreated]("zone.created"),
        orm.E[RecordAdded]("zone.record_added", orm.V(2)),
    ),
)

// v1 → v2 (field renamed); chain v1→v2→v3 is walked automatically:
orm.Upcast(db, "zone.record_added", 1,
    func(old RecordAddedV1) (RecordAdded, error) {
        return RecordAdded{Record: old.Entry}, nil
    },
)
```

Missing an upcaster for an old version present in the DB ⇒ startup error (not only discovered at read time).

### 7.7 Snapshots

Snapshots are **not an API concern in the normal case** — they are created automatically per model policy (Section 5.1), asynchronously, never in the write path. A snapshot is the aggregate state folded via `Apply`, serialized; there is no separate snapshot computation function.

Opt-in for special cases (custom serialization — excluding caches, a special format):

```go
func (z *DNSZone) SnapshotMarshal() ([]byte, error)
func (z *DNSZone) SnapshotUnmarshal(b []byte) error
```

**Archiving:** events below the second-newest snapshot automatically move into archive tables (a worker, lease-coordinated, never ahead of the projection state). This is invisible to the API: `Load` folds snapshot + hot log; `History`, `AtVersion`/`AtTime`, `Stream` and rebuilds transparently read hot **and** archive.

---

## 8. Schema versions & migration

### 8.1 Declaration

```go
orm.SchemaVersion(db, 3)   // schema version this app build expects

orm.MigrationTo(db, 3,     // steps from 2 to 3; older MigrationTo stay in the code
    orm.ReplaceModel[ZoneV2, DNSZone](func(ctx context.Context, old ZoneV2) (DNSZone, error) {
        return DNSZone{ /* rebuild */ }, nil
    }),
    orm.BatchScript("normalize-records", func(ctx context.Context, b orm.Batch) error {
        // b yields rows in chunks; the engine manages the checkpoint
        return nil
    }),
)
```

- **Additive changes** (new column/index/model) need no step — auto-diff.
- **Dropped fields** are marked `deprecated`, never deleted automatically. The column is dropped by `FinalizeMigration` of the version in which the field was also removed from the struct.
- **Drift protection:** models changed without a version bump ⇒ startup error (checksum comparison).
- **`ReplaceModel` conventions:** the Go name of the old struct is the former model name plus a version suffix — `ZoneV2` reads the table of the former model `Zone` (the `V<n>` suffix is stripped for table derivation). Tenant, geo and — unless the transform sets its own — the ID survive the rebuild. `required`/`enum` constraints of the target model apply during backfill too. The target must be a CRUD model (ES rebuilds go via events/upcasters).
- **`BatchScript` checkpoint:** the script works with the normal ORM APIs and saves its progress via `b.Checkpoint(ctx)` / `b.SaveCheckpoint(ctx, key, rowsDone)` — on resumption (crash, restart) it reads the key back and continues there. A successful return marks the step done.

### 8.2 Execution

```go
err := db.Migrate(ctx)                          // normal case

err = db.Migrate(ctx, orm.MigrationPlan{        // cluster fine-tuning (optional)
    WorkersPerGeo: map[string]int{"eu-central": 4, "us-east": 2},
    BatchSize:     5000,
    Throttle:      orm.RowsPerSecond(20000),
})
```

`Migrate` is idempotent and a no-op at the current version (only instance registration). State machine: `idle → expanding → backfill → dual-write → finalizing → idle`.

- **expanding:** additive DDL, global, one leader (lease).
- **backfill:** geo-parallel; the work unit is the shard `(step, geo, key range)`, leased only to workers **in the same region** (`MigrationRole(MigrationWorker)`). Resumable, throttleable.
- **dual-write:** old instances keep running unchanged and write into the old structure; their changes (insert/update/delete) are continuously carried into the new structure via a trigger tail (a worker processes the queue, at-least-once). Both app generations coexist. The reverse direction — new instances also writing into the old structure — requires a reverse transform and is planned as an optional `ReplaceModel` extension.
- **finalizing:** explicit —

```go
err := db.FinalizeMigration(ctx, 3)
```

Precondition (checked by ORM++): no live instance with an older schema version in the instance registry, all regions finished backfilling. Then: end dual-write, remove `deprecated` fields and old tables.

---

## 9. Workers & cluster operation

### 9.1 Start workers

```go
err := db.StartWorkers(ctx)
```

Starts this instance's background processing: projections, `OnEvent` reactors, snapshot and archive workers, and migration shards where applicable. Coordination via **leases with fencing** in the DB — per task (projection per model, view, dual-write tail) exactly one instance works cluster-wide, sticky; if it fails (lease TTL or `Close`) another takes over. `ctx` cancellation or `db.Close()` stops the workers cleanly.

### 9.2 Deployment patterns

**One process (SQLite, desktop/demo):**

```go
db, _ := orm.Open(orm.SQLite("./app.db"))
registerModels(db)                 // shared function: Register + Topology + SchemaVersion
db.Migrate(ctx)
db.StartWorkers(ctx)               // trivial: one region, one process
```

**N identical app instances (Postgres/Yugabyte):** identical code to above — each instance calls `Migrate` (only the lease winner works) and `StartWorkers` (leases distribute the work). No special case.

**Dedicated worker processes:** same binary pattern, the instance only serves background work:

```go
db, _ := orm.Open(orm.Yugabyte(dsn),
    orm.InstanceGeo("eu-central"),
    orm.MigrationRole(orm.MigrationWorker),
)
registerModels(db)
db.Migrate(ctx)
db.StartWorkers(ctx)
<-ctx.Done()                        // no HTTP server — workers only
```

App instances then set `MigrationRole(MigrationNone)` and optionally skip `StartWorkers` if projections should run exclusively on the worker processes.

**Geo-distributed cluster (rollout order during migration):**

1. Roll out the new app version (schema version n+1) region by region — `Migrate` moves to `expanding`, then `backfill` (each region migrates its data with its own workers), then `dual-write`.
2. Retire old instances (the instance registry empties).
3. `FinalizeMigration` — from an ops job or manually.

### 9.3 Failure behaviour

- Worker failure: the lease expires, another instance in the same region takes over at the checkpoint.
- `Append`/CRUD during migration: allowed at any time (online migration); the engine handles dual-write.
- Network split to the DB: operations return pool errors; workers pause and resume at the checkpoint after reconnect.

---

## 10. Observability

Observability APIs show the **operator** the physical truth — they are the one place where backends may differ (on SQLite you honestly see a region `local` with one worker).

```go
st, err := db.MigrationStatus(ctx)
// st.Phase                        "backfill"
// st.CurrentVersion / TargetVersion
// st.Geo["eu-central"].Percent    87.3
// st.Geo["eu-central"].Workers    4

h, err := db.Health(ctx)
// h.Instances        live instances (geo, role, app/schema version, heartbeat)
// h.Projections      lag per projection/region (events behind the log)
// h.Regions          topology status (active/bootstrapping/draining)
```

Both return plain data structures — wiring them to logging/metrics/health endpoints is the app's job.

---

## 11. Errors

All errors are sentinel values checkable with `errors.Is`:

| Error | Meaning |
|---|---|
| `orm.ErrNotFound` | Record/aggregate does not exist (within tenant/geo scope) |
| `orm.ErrNoTenant` | Context without tenant (fail-closed) |
| `orm.ErrUnknownTenant` | Tenant ID does not exist in the registry or is archived |
| `orm.ErrRequiredField` | `required` field not set at insert (zero value) |
| `orm.ErrInvalidReference` | `ref` target does not exist or belongs to another tenant |
| `orm.ErrReferenceInUse` | Delete refused: record still referenced (`ondelete=restrict`) |
| `orm.ErrInvalidValue` | Value outside the `enum` value set |
| `orm.ErrRequiresTx` | Operation (e.g. `GetForUpdate`) outside a transaction |
| `orm.ErrTenantNotArchived` | `Purge` on a non-archived tenant |
| `orm.ErrNoGeo` | Multi-region topology, but no data geo in the context |
| `orm.ErrRegionNotActive` | Data geo points at a `bootstrapping`/`draining`/unknown region |
| `orm.ErrVersionConflict` | Optimistic locking: CRUD `version` or aggregate version stale |
| `orm.ErrWaitTimeout` | `WaitFor` deadline elapsed, projection lagged behind |
| `orm.ErrSchemaDrift` | Models changed without a `SchemaVersion` bump |
| `orm.ErrMigrationPending` | Operation requires a completed migration (e.g. `FinalizeMigration` called too early) |
| `orm.ErrReadOnlyReplica` | Write access to a replica under `WriteHomeOnly` |

---

## Appendix: minimal end-to-end example

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

    ctx = orm.WithTenant(ctx, orm.SingleTenant) // default tenant created at bootstrap

    todos := orm.Repo[Todo](db)
    t := &Todo{Title: "ORM++ bauen"}
    _ = todos.Insert(ctx, t)

    open, _ := todos.Query(ctx).Where(orm.Eq("Done", false)).All()
    _ = open
}
```

The same file runs unchanged against `orm.Yugabyte(dsn)` in a geo-partitioned cluster — that's ORM++'s promise.
