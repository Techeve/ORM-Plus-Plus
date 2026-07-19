---
title: Overview
description: The bundled example applications.
sidebar:
  order: 1
---

The repository contains two runnable examples under `examples/`.

## `examples/demo` — every capability once

The fully commented example application shows every capability once:

```sh
go run ./examples/demo
```

CRUD, event sourcing, tenants, geo, encryption, migration with dual-write,
upcasters, GDPR and observability — all against SQLite, no external service.

## `examples/bench` — performance benchmark

The same scenario against SQLite/PostgreSQL/YugabyteDB; latencies (p50/p95/p99)
+ throughput, report as JSON and in Go benchmark format (benchstat-compatible):

```sh
docker compose up -d   # PostgreSQL (5433) and YugabyteDB (5434)
go run ./examples/bench -yugabyte "$ORMPP_BENCH_YUGABYTE"
```

## Testing against all three backends

The **identical** behaviour suite runs against all three backends:

```sh
docker compose up -d

ORMPP_TEST_BACKEND=postgres ORMPP_TEST_DSN="postgres://orm:orm@localhost:5433/orm" go test -race ./...
ORMPP_TEST_BACKEND=yugabyte ORMPP_TEST_DSN="postgres://yugabyte@localhost:5434/yugabyte" go test -race ./...
```

A concrete end-to-end example is on the [Todo app](/en/examples/todo/) page.
