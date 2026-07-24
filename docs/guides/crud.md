---
title: CRUD-Modelle
description: Repository, Query-Builder, Batch/Bulk, Transaktionen und Sperren.
sidebar:
  order: 2
---

## Repository

```go
func Repo[T any](h Handle) Repository[T]   // Handle: *DB oder Tx
```

```go
type Repository[T any] interface {
	Insert(ctx, entity *T) error              // füllt pk/autocreate
	InsertMany(ctx, entities []*T, opts ...BatchOption) error
	Get(ctx, id orm.ID) (*T, error)           // ErrNotFound
	GetForUpdate(ctx, id orm.ID) (*T, error)  // nur in Tx (sonst ErrRequiresTx)
	Update(ctx, entity *T) error              // ErrVersionConflict bei `version`
	Upsert(ctx, entity *T) error
	Delete(ctx, id orm.ID) error
	Query(ctx) QueryBuilder[T]
}
```

```go
accounts := orm.Repo[ProviderAccount](db)

acc := &ProviderAccount{Name: "Cloudflare Prod", Email: "ops@example.org"}
err := accounts.Insert(ctx, acc)   // acc.ID, acc.CreatedAt gefüllt

acc, err = accounts.Get(ctx, acc.ID)
acc.Name = "Cloudflare Production"
err = accounts.Update(ctx, acc)    // Versionskonflikt ⇒ ErrVersionConflict
err = accounts.Delete(ctx, acc.ID)
```

## Query-Builder

```go
list, err := accounts.Query(ctx).
	Where(orm.And(
		orm.Like("Name", "Cloud%"),
		orm.Gte("CreatedAt", since),
	)).
	OrderBy("CreatedAt", orm.Desc).
	Limit(20).
	All()
```

Bedingungen: `Eq`, `Ne`, `Gt`, `Gte`, `Lt`, `Lte`, `Like`, `In`, `IsNull`,
`NotNull`, `And`, `Or`, `Not`. Feldnamen sind die **Go-Feldnamen** des Structs;
unbekannte Felder ⇒ Fehler beim Bauen, nicht zur Laufzeit. Der Tenant-Filter
(und Geo-Scope) wird **immer** automatisch injiziert.

Weitere Terminatoren: `All()`, `Iter()` (Cursor-Streaming statt Speicher),
`First()`, `Count()`, `Exists()`, `UpdateSet(...)`, `Delete()`.

## Batch & Bulk

```go
err := accounts.InsertMany(ctx, accs)                   // atomar (Default)
err = accounts.InsertMany(ctx, million, orm.Chunked(10_000)) // Chunks je eine Tx

n, err := accounts.Query(ctx).
	Where(orm.Eq("Status", "trial")).
	UpdateSet(orm.Set("Status", "expired"))             // ein Statement

n, err = accounts.Query(ctx).Where(orm.Lt("CreatedAt", cutoff)).Delete()
```

Die **Einfüge-Strategie wählt der Dialekt-Adapter**, nicht der Aufrufer:
Postgres nutzt Multi-Row-`INSERT` und schaltet ab einer Schwelle auf `COPY`,
Yugabyte batcht tablet-gerecht, SQLite fährt Prepared Statements in einer Tx.
Tenant-, Referenz-, `enum`- und `required`-Prüfungen gelten in jedem Pfad.

## Pessimistisches Sperren & Transaktionen

```go
err := db.Tx(ctx, func(tx orm.Tx) error {
	acc, err := orm.Repo[Account](tx).GetForUpdate(ctx, id)  // Zeile gesperrt
	if err != nil { return err }
	acc.Balance -= amount
	return orm.Repo[Account](tx).Update(ctx, acc)
})
```

`SELECT ... FOR UPDATE` auf Postgres/Yugabyte; SQLite emuliert über die
serialisierte Schreib-Connection — verhaltensgleich. Rollback bei
Fehler-Rückgabe oder Panic; verschachtelte `Tx` nutzen Savepoints.
Event-Appends dürfen in derselben Transaktion mit CRUD-Schreibvorgängen stehen.
