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
	orm.Region("eu-central", orm.Placement("cloud1.eu-central-1")),
	orm.Region("us-east",    orm.Placement("cloud1.us-east-1")),
	orm.Region("ap-south",   orm.Placement("cloud1.ap-south-1")),
)
```

**Rules:**

- No `Topology` declaration ⇒ implicit region `local`. The simplest case needs
  zero geo code.
- **Add a region** = one more `orm.Region(...)` line + rollout. Additive, no
  special mode.
- On SQLite/single-region Postgres all regions collapse onto one — the
  declaration stays valid (behavioural-equality principle).

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
