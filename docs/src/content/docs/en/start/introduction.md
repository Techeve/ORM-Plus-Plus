---
title: Introduction
description: The core principle of ORM++ — a perfect abstraction layer over SQLite, PostgreSQL and YugabyteDB.
sidebar:
  order: 1
---

**ORM++** is a model-first persistence module for Go: classic ORM mapping
**plus** event sourcing, projections, snapshots/archiving and expand/contract
migrations — optimised for a few, fully exploited databases:

- **SQLite** — embedded, desktop/demo use
- **PostgreSQL** — server, on-prem
- **YugabyteDB** — distributed, multi-tenant, geo-partitioned

The consuming application declares models (tagged Go structs) and works with
commands, events and typed queries — it writes **no SQL** and knows no
database details.

## Core principle: behavioural equality

For the consuming application it is irrelevant which database sits underneath.
Every declaration is accepted and semantically fulfilled on every backend —
natively where the DB can, emulated or collapsed where it cannot. App code
**never** branches on the backend; the same application runs byte-for-byte
identically on all three.

Consequences for the API:

- No function reveals the backend (no `db.Kind()`). The only exception: the
  observability APIs show the *operator* the physical truth.
- A topology with five regions on SQLite is valid — SQLite implicitly has the
  single region `local`, and all declared regions map onto it.
- The application writes **no SQL** and knows no table, driver or dialect
  details.

## What ORM++ provides

- **CRUD models** with a typed repository and query builder.
- **Event-sourced aggregates** (CloudEvents 1.0), snapshots and archiving.
- **Projections/read models** materialised by workers from the event stream.
- **Multi-tenancy & geo-partitioning** baked into the data model from v1.
- **Field encryption** (AES-256-GCM) via an `encrypted` tag.
- **Expand/contract migrations** with dual-write for zero-downtime operation.

## Next

- [Installation](/en/start/installation/)
- [Quickstart](/en/start/quickstart/)
