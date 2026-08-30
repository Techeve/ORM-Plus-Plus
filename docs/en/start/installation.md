---
title: Installation
description: Add ORM++ as a Go module.
sidebar:
  order: 2
---

## Requirements

- **Go 1.26** or newer.
- For server operation: **PostgreSQL** or **YugabyteDB**. For desktop/demo and
  tests, **SQLite** is enough (embedded, no service required).

## Add the module

```go
import orm "gitlab.techeve.de/techeve/orm-plus-plus"
```

ORM++ lives on the internal GitLab server. For private fetches:

```sh
export GOPRIVATE=gitlab.techeve.de
go get gitlab.techeve.de/techeve/orm-plus-plus@latest
```

## Drivers

The drivers are part of the module — no separate import needed:

```go
orm.SQLite(path string)   // embedded; demo/desktop/tests. WAL mode.
orm.Postgres(dsn string)  // server, on-prem (pgx pool)
orm.Yugabyte(dsn string)  // distributed; DSN should contain regionally near endpoints
```

Continue to the [Quickstart](/en/start/quickstart/).
