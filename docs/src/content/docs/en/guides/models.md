---
title: Declaring models
description: Register structs, tags, scope and geo options, references.
sidebar:
  order: 1
---

A model is a Go struct with tags. Registration happens at startup, before
`Migrate`. The registry validates the struct and compiles the mapping plan
once (no reflection in the hot path).

```go
func Register[T any](db *DB, mode ModelMode, opts ...ModelOption)

orm.CRUD()          // classic persistence
orm.EventSourced()  // event sourcing (struct embeds orm.Aggregate)
```

## Struct tags

```go
type ProviderAccount struct {
	ID        orm.ID    `orm:"pk"`
	Name      string    `orm:"index"`
	Email     string    `orm:"unique"`
	Labels    []string  `orm:"json"`
	Version   int64     `orm:"version"`      // optimistic locking
	CreatedAt time.Time `orm:"autocreate"`
	UpdatedAt time.Time `orm:"autoupdate"`
	Notes     string    `orm:"deprecated"`   // marked, dropped at finalisation
}
```

| Tag | Meaning |
|---|---|
| `pk` | Primary key (exactly one; `orm.ID` is UUIDv7) |
| `index`, `unique` | Secondary / unique index |
| `json` | Nested values as a JSON(B) column |
| `version` | Column for optimistic locking on `Update` |
| `autocreate`, `autoupdate` | Timestamp maintenance by the engine |
| `ref=Model[,ondelete=…]` | Reference to another model |
| `enum=a\|b\|c` | Value set for string fields (native CHECK, engine-checked everywhere) |
| `default=…` | Default when the field holds the zero value at insert |
| `encrypted` | Field is stored encrypted |
| `immutable` | Write-once: unchangeable after insert |
| `required` | Must be set at insert (zero value ⇒ `ErrRequiredField`) |
| `deprecated` | Field is marked for removal (expand/contract) |
| `-` | Field is not persisted |

**Nullability** follows the Go type: non-pointer fields are `NOT NULL`,
pointer fields (`*string`, `*time.Time`) allow `NULL`. You do **not** declare
`tenant_id` or the geo columns — they are implicitly present in every table
and controlled via the context.

## Scope and geo options

```go
orm.TenantFree()   // model without tenant column/filter (technical tables)

orm.GeoScoped()    // each record in exactly one region (default)
orm.GeoGlobal()    // model present in all regions (master data: tenants, users, plans)
orm.GeoFlexible()  // per record: home region + read replicas
```

## Composite constraints

```go
orm.Register[Record](db, orm.CRUD(),
	orm.Unique("ProjectID", "Name"),   // unique across several columns
	orm.Index("Status", "CreatedAt"),  // composite secondary index
)
```

Tenant and geo columns are automatically included in unique constraints —
uniqueness applies per tenant.

## References

```go
type Document struct {
	ID        orm.ID  `orm:"pk"`
	Title     string  `orm:"required"`
	CreatedBy orm.ID  `orm:"ref=User,immutable,required"`  // required, immutable
	ProjectID orm.ID  `orm:"ref=Project,ondelete=cascade"` // dies with the project
	ReviewerID *orm.ID `orm:"ref=User"`                    // optional (pointer ⇒ NULL)
}
```

References may only point to records of the **same tenant** (exception: the
target is `TenantFree` or `GeoGlobal` master data). `ondelete`:
`restrict` (default) · `cascade` · `setnull` (pointer fields only).

## Field encryption

See [Encryption](/en/guides/encryption/).
