---
title: Überblick
description: Die mitgelieferten Beispiel-Anwendungen.
sidebar:
  order: 1
---

Das Repository enthält zwei lauffähige Beispiele unter `examples/`.

## `examples/demo` — jede Fähigkeit einmal

Die durchkommentierte Beispielanwendung zeigt jede Fähigkeit einmal:

```sh
go run ./examples/demo
```

CRUD, Event Sourcing, Tenants, Geo, Verschlüsselung, Migration mit Dual-Write,
Upcaster, DSGVO und Observability — alles gegen SQLite, ohne externen Dienst.

## `examples/bench` — Performance-Benchmark

Dasselbe Szenario gegen SQLite/PostgreSQL/YugabyteDB; Latenzen (p50/p95/p99) +
Durchsatz, Bericht als JSON und im Go-Benchmark-Format (benchstat-kompatibel):

```sh
docker compose up -d   # PostgreSQL (5433) und YugabyteDB (5434)
go run ./examples/bench -yugabyte "$ORMPP_BENCH_YUGABYTE"
```

## Tests gegen alle drei Backends

Die **identische** Verhaltenssuite läuft gegen alle drei Backends:

```sh
docker compose up -d

ORMPP_TEST_BACKEND=postgres ORMPP_TEST_DSN="postgres://orm:orm@localhost:5433/orm" go test -race ./...
ORMPP_TEST_BACKEND=yugabyte ORMPP_TEST_DSN="postgres://yugabyte@localhost:5434/yugabyte" go test -race ./...
```

Ein konkretes Ende-zu-Ende-Beispiel steht unter
[Todo-App](/examples/todo/).
