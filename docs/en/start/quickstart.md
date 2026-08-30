---
title: Quickstart
description: From zero to your first model in five minutes — CRUD on SQLite.
sidebar:
  order: 3
---

This end-to-end example declares a model, bootstraps a SQLite file and
writes/reads tenant-scoped. **The same file runs unchanged against
`orm.Yugabyte(dsn)`** in a geo-partitioned cluster — that is the promise of
ORM++.

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

	ctx = orm.WithTenant(ctx, orm.SingleTenant) // default tenant created at bootstrap

	todos := orm.Repo[Todo](db)
	t := &Todo{Title: "build ORM++"}
	_ = todos.Insert(ctx, t)

	open, _ := todos.Query(ctx).Where(orm.Eq("Done", false)).All()
	_ = open
}
```

## The three steps

1. **Register a model** (`Register`) — declares the struct as a CRUD or
   event-sourced model. Done once at startup, before `Migrate`.
2. **Create the schema** (`Migrate`) — creates tables and system tables. Idempotent.
3. **Start workers** (`StartWorkers`) — projections, snapshots, migration shards.

## Tenant in the context

Tenant (and geo) hang off the **`context.Context`**, never off function
signatures. A missing tenant is an error (fail-closed) — nothing runs
tenant-less by accident. Single-tenant apps set `orm.SingleTenant` once at
startup.

## Next

- [Declaring models](/en/guides/models/)
- [CRUD models](/en/guides/crud/)
- [Event sourcing](/en/guides/event-sourcing/)
