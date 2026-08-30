---
title: Schnellstart
description: In fünf Minuten von null zum ersten Modell — CRUD auf SQLite.
sidebar:
  order: 3
---

Dieses Ende-zu-Ende-Beispiel deklariert ein Modell, bootstrapt eine
SQLite-Datei und schreibt/liest tenant-gescoped. **Dieselbe Datei läuft
unverändert gegen `orm.Yugabyte(dsn)`** in einem geo-partitionierten Cluster —
das ist das Versprechen von ORM++.

```go
package main

import (
	"context"
	orm "gitlab.techeve.de/techeve/orm-plus-plus"
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

	ctx = orm.WithTenant(ctx, orm.SingleTenant) // beim Bootstrap angelegter Default-Tenant

	todos := orm.Repo[Todo](db)
	t := &Todo{Title: "ORM++ bauen"}
	_ = todos.Insert(ctx, t)

	open, _ := todos.Query(ctx).Where(orm.Eq("Done", false)).All()
	_ = open
}
```

## Die drei Schritte

1. **Modell registrieren** (`Register`) — deklariert das Struct als CRUD- oder
   Event-Sourced-Modell. Passiert einmalig beim Start, vor `Migrate`.
2. **Schema anlegen** (`Migrate`) — legt Tabellen und Systemtabellen an. Idempotent.
3. **Worker starten** (`StartWorkers`) — Projektionen, Snapshots, Migrations-Shards.

## Tenant im Context

Tenant (und Geo) hängen **am `context.Context`**, nie an Funktionssignaturen.
Ein fehlender Tenant ist ein Fehler (fail-closed) — nichts läuft versehentlich
tenant-los. Single-Tenant-Apps setzen einmal beim Start `orm.SingleTenant`.

## Weiter

- [Modelle deklarieren](/guides/models/)
- [CRUD-Modelle](/guides/crud/)
- [Event Sourcing](/guides/event-sourcing/)
