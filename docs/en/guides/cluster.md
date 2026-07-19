---
title: Workers & cluster operation
description: StartWorkers, deployment patterns, failure behaviour.
sidebar:
  order: 8
---

## Start workers

```go
err := db.StartWorkers(ctx)
```

Starts this instance's background processing: projections, `OnEvent` reactors,
snapshot and archive workers, and migration shards where applicable.
Coordination via **leases with fencing** in the DB — per task exactly one
instance works cluster-wide, sticky; if it fails (lease TTL or `Close`) another
takes over. `ctx` cancellation or `db.Close()` stops the workers cleanly.

## Deployment patterns

**One process (SQLite, desktop/demo):**

```go
db, _ := orm.Open(orm.SQLite("./app.db"))
registerModels(db)          // Register + Topology + SchemaVersion
db.Migrate(ctx)
db.StartWorkers(ctx)        // trivial: one region, one process
```

**N identical app instances (Postgres/Yugabyte):** identical code — each
instance calls `Migrate` (only the lease winner works) and `StartWorkers`
(leases distribute the work). No special case.

**Dedicated worker processes:** same binary, the instance only serves
background work:

```go
db, _ := orm.Open(orm.Yugabyte(dsn),
	orm.InstanceGeo("eu-central"),
	orm.MigrationRole(orm.MigrationWorker),
)
registerModels(db)
db.Migrate(ctx)
db.StartWorkers(ctx)
<-ctx.Done()                // no HTTP server — workers only
```

## Failure behaviour

- **Worker failure:** the lease expires, another instance in the same region
  takes over at the checkpoint.
- **`Append`/CRUD during migration:** allowed at any time (online migration);
  the engine handles dual-write.
- **Network split to the DB:** operations return pool errors; workers pause and
  resume at the checkpoint after reconnect.
