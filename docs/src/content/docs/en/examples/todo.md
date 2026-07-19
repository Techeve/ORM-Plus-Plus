---
title: Todo app (end-to-end)
description: From model to query — the same file on every backend.
sidebar:
  order: 2
---

This minimal example declares a model, bootstraps SQLite and reads/writes
tenant-scoped. **The same file runs unchanged against `orm.Yugabyte(dsn)`** in
a geo-partitioned cluster.

```go
package main

import (
	"context"
	orm "gitlab.techeve.de/orm-plus-plus/orm-plus-plus"
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

	ctx = orm.WithTenant(ctx, orm.SingleTenant)

	todos := orm.Repo[Todo](db)
	t := &Todo{Title: "build ORM++"}
	_ = todos.Insert(ctx, t)

	open, _ := todos.Query(ctx).Where(orm.Eq("Done", false)).All()
	_ = open
}
```

## From CRUD to event sourcing

If `Todo` becomes an aggregate with history, only the declaration changes — the
operational logic (`Migrate`/`StartWorkers`) stays the same. See
[Event sourcing](/en/guides/event-sourcing/).
