---
title: Architecture
description: Layer model, immutable principles and physical schema of ORM++.
sidebar:
  order: 0
---

This page shows the big picture: how ORM++ is layered internally, which rules
never change, and how the data is physically laid out on disk. For the
practical use of the individual building blocks, see the [guides](/en/guides/models/);
this page explains the why behind them.

## Layer model

```
┌──────────────────────────────────────────────────────────┐
│ Public API (Go)                                          │
│  Register[T] · Query[T] · Command/Append · WaitFor       │
│  Bootstrap · Migrate · Rebuild · Snapshot/Archive        │
├──────────────────────────────────────────────────────────┤
│ Core Engine (dialect-independent)                        │
│  Model registry (reflection)   Schema planner/diff       │
│  Event Store + Outbox          Projection runtime        │
│  Snapshot/archive manager      Lease coordinator         │
│  Migration orchestrator (expand/contract/dual-write)     │
├──────────────────────────────────────────────────────────┤
│ Dialect adapters (thin)                                  │
│  sqlite (modernc)  ·  postgres (pgx)  ·  yugabyte (pgx)  │
│  DDL gen · upsert · JSON(B) · partitioning ·             │
│  Coordination primitives (lease table, no                │
│  advisory-lock requirement → YB-compatible)              │
└──────────────────────────────────────────────────────────┘
```

The app layer talks exclusively to the public API. The core engine knows no
SQL dialect detail — it compiles models, plans schema diffs, and orchestrates
migrations and projections against a narrow `dialect` interface. Only the
dialect adapters translate that into SQLite-, PostgreSQL-, or
YugabyteDB-specific SQL. Adding a new backend means: write an adapter — core
and public API stay unchanged.

## Immutable principles

These five rules apply to every line of code in ORM++ and are never broken:

1. **Behavioural equality across all backends.** SQLite, PostgreSQL, and
   YugabyteDB behave identically for app code — same API, same errors, same
   semantics. Emulation where necessary (SQLite collapses geo regions to one,
   emulates snapshot intervals via side tables), native implementation where
   available. App code **never** branches by backend — a `db.Kind()`
   deliberately does not exist.
2. **Tenant fail-closed.** Any operation without a tenant in the
   `context.Context` fails with `ErrNoTenant` — no default, no empty ID, no
   silent fallback. The only exception is `TenantFree` models for technical
   tables without user data.
3. **No SQL in app code.** The library generates all SQL itself. The app
   declares models with Go struct tags and calls typed repository methods.
4. **No reflection in the hot path.** Struct tags are compiled once at
   `Register[T]()` (tag parsing → mapping plan). Only index lookups happen in
   the I/O path.
5. **Immutable events.** Event log rows are never changed or deleted — only
   archived (see [Event Sourcing](/en/guides/event-sourcing/)).

The reason for principle 1 is the actual purpose of the project: the same
application should run unchanged on SQLite (demo/desktop), PostgreSQL
(on-prem), and YugabyteDB (geo-distributed cloud). The behavioural test suite
therefore runs **unchanged** against all three backends — there are no
backend-specific assertions in the test code.

## System tables

All tables managed by ORM++ itself carry the `ormpp_` prefix and — apart from
the tenant and topology registers — are themselves tenant-scoped:

| Table | Purpose |
|---|---|
| `ormpp_schema_state` | Global migration state (exactly one row) |
| `ormpp_schema_history` | Audit of all version changes |
| `ormpp_instances` | Instance registry: which app instance runs where, with which version |
| `ormpp_migration_progress` | Checkpoints of the backfill shards, per region |
| `ormpp_deprecated` | Marked but not yet removed fields/tables |
| `ormpp_leases` | Coordination (migration leader, projection workers), with fencing |
| `ormpp_tenants` | Built-in tenant registry (see [Multi-Tenancy](/en/guides/multi-tenancy/)) |
| `ormpp_geo_regions` | Topology register with lifecycle (see [Geo-Partitioning](/en/guides/geo/)) |
| `ormpp_outbox` / `ormpp_checkpoints` | Event trigger chain and projection positions |

Details on the migration state machine and geo modes live in the respective
guides — this table is the map, not the manual.

## Physical schema: event-sourced models

Every event-sourced model produces three tables (example `zones`):

| Table | Content |
|---|---|
| `zones` | Read model: one row per aggregate, columns from the struct plus `aggregate_seq`. The query builder reads against this — reads cost the same as with CRUD. |
| `zones_events` | Append-only log. Columns: `geo`, `tenant_id`, `aggregate_id`, `aggregate_seq`, `seq` (monotonic per geo), `event_id` (UUIDv7), `occurred_at`, `type_id`, `data` (JSON/JSONB). |
| `zones_snapshots` | `aggregate_id`, `aggregate_seq`, `taken_at`, `state`. **Not** append-only: older snapshots are deleted per policy (default `KeepLast(2)`). |

A snapshot is not a separate computation but the serialized aggregate state
after event N — the same `Apply` code that also folds on load. This avoids a
second source of divergence between snapshot and event replay.

`seq` is deliberately **monotonic per geo, not global** — a global sequence
would be a hotspot on a geo-distributed cluster. What's guaranteed is strict
ordering per aggregate (`aggregate_seq`) and per region (`seq`); there is no
total order across regions.

## Package layout

ORM++ lives entirely in the root package `orm` — no `src` directory, no
sub-packages for the public API. The most important files:

| File | Responsibility |
|---|---|
| `registry.go` | Tag parsing, validation, reference resolution, topo sort |
| `driver.go` / `sqlite.go` / `postgres.go` | Driver interface and the three dialect implementations |
| `db.go` | `Open`, `Register[T]`, `Migrate`, worker lifecycle |
| `schema.go` | DDL generation, additive diff |
| `repo.go` | `Repository[T]` — CRUD on classic models |
| `aggregate.go` / `event.go` | `orm.Aggregate`, event fold core |
| `projection.go` | Worker loop, checkpoints, `OnEvent`/`Watch`, snapshots |
| `migrator.go` | State machine expanding → backfill → dual-write → finalizing |
| `instances.go` | Instance registry and leases with fencing |

This map is for anyone reading the code itself — for using the library, the
guides and the [API reference](/en/reference/api/) are enough.
