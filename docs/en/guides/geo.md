---
title: Geo-partitioning
description: Topology, geo modes, replicas, locality-preferred reads.
sidebar:
  order: 7
---

The topology describes **which regions the cluster has**. It is cluster state,
not instance configuration: every instance declares the same topology (same
binary).

```go
orm.Topology(db,
	orm.Region("eu-central",   orm.Placement("ts_eu_central")),
	orm.Region("eu-southwest", orm.Placement("ts_eu_southwest")),
	orm.Region("na",           orm.Placement("ts_na")),
)
```

**Rules:**

- No `Topology` declaration ⇒ implicit region `local`. The simplest case needs
  zero geo code.
- **Add a region** = one more `orm.Region(...)` line + rollout. Additive, no
  special mode. `Migrate` reconciles partitions against the topology on
  **every** run, even without a new schema version.
- **Remove a region** = drop it from the declaration, roll out, then call
  `db.RemoveRegion(ctx, name)`. If the region still holds data, the call fails
  with `ErrRegionHasData` and names the tables.
- On SQLite/single-region Postgres all regions collapse onto one — the
  declaration stays valid (behavioural-equality principle).

## Geo-partitioning is a core capability

As soon as a topology is declared, ORM++ partitions **every** table of a
non-GeoGlobal model by `geo` on PG/YB — CRUD tables, ES read models, event
logs, archives and snapshots. The API does not change: reads never filter on
geo, and `Get`/`Update`/`Delete` by ID find the record in any region. Existing
installations with unpartitioned tables are converted by `Migrate` itself
(resumable; schedule the first `Migrate` after the upgrade into a quiet
period).

Two things move into the engine because they have no native counterpart on
partitioned tables — identically on all backends, SQLite included:

- **References to partitioned targets**: existence and tenant checks on
  write, `restrict` before delete, `setnull`/`cascade` emulated inside the
  delete transaction.
- **Unique constraints**: physically, uniqueness holds per geo; the engine
  checks tenant-global uniqueness before every write and reports
  `orm.ErrUniqueConflict`.

## Placement: turning the label into physical residency

Without `Placement`, a region is just a column value — the rows sit wherever
the database puts them. `orm.Placement("ts_eu_central")` names an **existing**
tablespace that ORM++ binds the region's partitions to.

ORM++ never creates tablespaces: replica count and placement blocks are an
operations decision. If the named tablespace does not exist, `Migrate` aborts
with `ErrPlacementNotFound` before any DDL runs.

```sql
-- Operations task, once per region (YugabyteDB):
CREATE TABLESPACE ts_eu_central WITH (replica_placement='{
  "num_replicas": 3,
  "placement_blocks": [
    {"cloud":"cloud1","region":"eu-central","zone":"eu-central-1a","min_num_replicas":3}
  ]}');
```

Measured on a three-node cluster with one node per region — `yb_local_tablets`
per node, counted for the partitions of one CRUD table (event logs, archives
and snapshots follow the same pattern):

| Partition | eu-central | eu-southwest | na |
|---|---|---|---|
| `geo_firma_geo_eu-central` | 1 | 0 | 0 |
| `geo_firma_geo_eu-southwest` | 0 | 1 | 0 |
| `geo_firma_geo_na` | 0 | 0 | 1 |
| `geo_firma_geo_default` (unbound) | 3 | 0 | 0 |

:::caution[ALTER TABLE … SET TABLESPACE does not rebind]
The binding is only ever established by the partition's `CREATE TABLE`.
YugabyteDB accepts `ALTER TABLE … SET TABLESPACE`, reports "data movement
successfully initiated" and rewrites the catalog — but without
`ysql_beta_feature_tablespace_alteration` the tablets do not move. The catalog
would then claim a placement that does not exist. ORM++ therefore does not use
it: if an existing partition is not in the declared tablespace, `Migrate`
reports `ErrPlacementMismatch` instead of faking a rebind.
:::

## Moving to another region

```go
// An organisation relocates to North America — every model at once:
err := db.MoveTenant(ctx, tenant, "na")

// Or a single record / a whole aggregate:
err = orm.Repo[Zone](db).SetGeo(ctx, zoneID, "na")
```

- `SetGeo` applies to every geo mode except `GeoGlobal`. Replica options stay
  reserved for `GeoFlexible`.
- On event-sourced models `SetGeo` moves the whole aggregate — event log,
  archive and read model together, otherwise geo pinning would break.
- On partitioned backends the rows move **physically** into the target
  region's partition.
- Moved events get fresh `seq` values at the tail of the target region (the geo
  sequence is monotonic per region). Projections there re-apply them as
  latecomers — at-least-once, idempotent.
- `MoveTenant` runs in batches and is idempotent: after an abort, calling it
  again resumes.

## Geo modes per model

| Option | Meaning |
|---|---|
| `orm.GeoScoped()` | Each record lives in exactly one region (the normal case). |
| `orm.GeoGlobal()` | Model present in **all** regions (master data). Writes pay cross-region consensus. |
| `orm.GeoFlexible(opts...)` | **Per record**: home region + read replicas. |

## Geo in the context & replicas

```go
ctx = orm.WithGeo(ctx, "eu-central", orm.ReplicateTo("us-east", "ap-south"))
err := groups.Insert(ctx, g)   // home EU, read copies US + AP

// Move later / change replicas:
err = groups.SetGeo(ctx, g.ID, "us-east", orm.ReplicateTo("eu-central"))
```

`WithGeo` sets the **data geo** (where a record belongs) — independent of the
instance geo. An EU instance may write a US record; it lands correctly in the
US partition (with latency). Reads are **locality-preferred**: if a copy exists
in the reading instance's region, it comes from there; otherwise the home
region answers.

**Geo pinning (ES):** the data geo sticks to the aggregate from its first
event. `WithGeo` sets the home region at creation; subsequent appends always
write to the home partition.

## Example: configuration data with locality-preferred reads

A model that's read often but written rarely — tenant settings, say —
benefits from `GeoFlexible`: every region gets a read replica, while writes
route through to the home region:

```go
type TenantSettings struct {
	ID    orm.ID `orm:"pk"`
	Key   string `orm:"unique"`
	Value string
}

orm.Register[TenantSettings](db, orm.CRUD(),
	orm.GeoFlexible(orm.WriteForwarding()), // write on a replica -> forwarded to the home region
)

settings := orm.Repo[TenantSettings](db)

// Home EU, replicas in US and AP right when it's created:
ctx = orm.WithGeo(ctx, "eu-central", orm.ReplicateTo("us-east", "ap-south"))
_ = settings.Insert(ctx, &TenantSettings{Key: "theme", Value: "dark"})

// An instance in us-east then reads locally, no cross-region latency —
// without app code ever asking for that explicitly.
```

Without `orm.WriteForwarding()`, the same write attempt from a us-east
instance would be rejected with an error (`orm.WriteHomeOnly()`, the
default) — replicas are then strictly read-only.
