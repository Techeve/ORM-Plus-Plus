---
title: CRUD models
description: Repository, query builder, batch/bulk, transactions and locking.
sidebar:
  order: 2
---

## Repository

```go
func Repo[T any](h Handle) Repository[T]   // Handle: *DB or Tx
```

```go
type Repository[T any] interface {
	Insert(ctx, entity *T) error              // fills pk/autocreate
	InsertMany(ctx, entities []*T, opts ...BatchOption) error
	Get(ctx, id orm.ID) (*T, error)           // ErrNotFound
	GetForUpdate(ctx, id orm.ID) (*T, error)  // Tx only (else ErrRequiresTx)
	Update(ctx, entity *T) error              // ErrVersionConflict with `version`
	Upsert(ctx, entity *T) error
	Delete(ctx, id orm.ID) error
	Query(ctx) QueryBuilder[T]
}
```

```go
accounts := orm.Repo[ProviderAccount](db)

acc := &ProviderAccount{Name: "Cloudflare Prod", Email: "ops@example.org"}
err := accounts.Insert(ctx, acc)   // acc.ID, acc.CreatedAt filled

acc, err = accounts.Get(ctx, acc.ID)
acc.Name = "Cloudflare Production"
err = accounts.Update(ctx, acc)    // version conflict ⇒ ErrVersionConflict
err = accounts.Delete(ctx, acc.ID)
```

## Query builder

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

Conditions: `Eq`, `Ne`, `Gt`, `Gte`, `Lt`, `Lte`, `Like`, `In`, `IsNull`,
`NotNull`, `And`, `Or`, `Not`. Field names are the struct's **Go field
names**; unknown fields ⇒ error at build time, not at runtime. The tenant
filter (and geo scope) is **always** injected automatically.

More terminators: `All()`, `Iter()` (cursor streaming instead of memory),
`First()`, `Count()`, `Exists()`, `UpdateSet(...)`, `Delete()`.

## Batch & bulk

```go
err := accounts.InsertMany(ctx, accs)                   // atomic (default)
err = accounts.InsertMany(ctx, million, orm.Chunked(10_000)) // chunks, one Tx each

n, err := accounts.Query(ctx).
	Where(orm.Eq("Status", "trial")).
	UpdateSet(orm.Set("Status", "expired"))             // one statement

n, err = accounts.Query(ctx).Where(orm.Lt("CreatedAt", cutoff)).Delete()
```

The **insert strategy is chosen by the dialect adapter**, not the caller:
Postgres uses multi-row `INSERT` and switches to `COPY` past a threshold,
Yugabyte batches per tablet, SQLite runs prepared statements in one Tx.
Tenant, reference, `enum` and `required` checks apply in every path.

## Pessimistic locking & transactions

```go
err := db.Tx(ctx, func(tx orm.Tx) error {
	acc, err := orm.Repo[Account](tx).GetForUpdate(ctx, id)  // row locked
	if err != nil { return err }
	acc.Balance -= amount
	return orm.Repo[Account](tx).Update(ctx, acc)
})
```

`SELECT ... FOR UPDATE` on Postgres/Yugabyte; SQLite emulates via the
serialised write connection — behaviourally identical. Rollback on error
return or panic; nested `Tx` use savepoints. Event appends may sit in the same
transaction as CRUD writes.
