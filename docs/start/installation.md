---
title: Installation
description: ORM++ als Go-Modul einbinden.
sidebar:
  order: 2
---

## Voraussetzungen

- **Go 1.26** oder neuer.
- Für Server-Betrieb: **PostgreSQL** oder **YugabyteDB**. Für Desktop/Demo und
  Tests genügt **SQLite** (eingebettet, kein Dienst nötig).

## Modul einbinden

```go
import orm "gitlab.techeve.de/orm-plus-plus/orm-plus-plus"
```

ORM++ liegt auf dem internen GitLab-Server. Für private Bezüge:

```sh
export GOPRIVATE=gitlab.techeve.de
go get gitlab.techeve.de/orm-plus-plus/orm-plus-plus@latest
```

## Treiber

Die Treiber sind Teil des Moduls — kein separater Import nötig:

```go
orm.SQLite(path string)   // eingebettet; Demo/Desktop/Tests. WAL-Modus.
orm.Postgres(dsn string)  // Server, On-Prem (pgx-Pool)
orm.Yugabyte(dsn string)  // verteilt; DSN sollte regional nahe Endpunkte enthalten
```

Weiter zum [Schnellstart](/start/quickstart/).
