---
title: Todo-App (Ende-zu-Ende)
description: Vom Modell bis zur Query — dieselbe Datei auf allen Backends.
sidebar:
  order: 2
---

Dieses Minimalbeispiel deklariert ein Modell, bootstrapt SQLite und
liest/schreibt tenant-gescoped. **Dieselbe Datei läuft unverändert gegen
`orm.Yugabyte(dsn)`** in einem geo-partitionierten Cluster.

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
	t := &Todo{Title: "ORM++ bauen"}
	_ = todos.Insert(ctx, t)

	open, _ := todos.Query(ctx).Where(orm.Eq("Done", false)).All()
	_ = open
}
```

## Von CRUD zu Event Sourcing

Wird aus `Todo` ein Aggregat mit Historie, ändert sich nur die Deklaration —
die Betriebslogik (`Migrate`/`StartWorkers`) bleibt gleich. Siehe
[Event Sourcing](/de/guides/event-sourcing/).
