---
title: Multi-tenancy
description: Tenant in the context, the built-in tenant registry, GDPR export/purge.
sidebar:
  order: 5
---

Tenant and geo are the **mandatory scope of every data operation** and hang off
the `context.Context` — never off function signatures.

```go
ctx = orm.WithTenant(ctx, tenantID)
ctx = orm.WithGeo(ctx, "eu-central")
```

- Missing tenant ⇒ `orm.ErrNoTenant` (fail-closed). Nothing runs tenant-less by
  accident. Single-tenant apps set a constant tenant once at startup
  (`orm.SingleTenant`).
- Every operation — including `Get`/`Update`/`Delete` by ID — filters on the
  context tenant. A foreign tenant by ID behaves like `ErrNotFound`.

## Tenant registry (built in)

Tenants do not come from the app — ORM++ ships them as a system model
(`GeoGlobal`, so every region can validate locally):

```go
tenants := db.Tenants()

t, err := tenants.Create(ctx, orm.TenantInfo{Name: "ACME GmbH"})   // ID: UUIDv7
t, err  = tenants.Get(ctx, id)
list, err := tenants.List(ctx)
err     = tenants.Archive(ctx, id)   // blocks new writes, existing data stays readable
```

**Tenant rules (not switchable off):**

1. **Insert verification:** every `Insert`/`Append` checks the context tenant ID
   against the registry — unknown or archived ⇒ `orm.ErrUnknownTenant`.
2. **Write-once:** `tenant_id` is set at insert and immutable afterwards. There
   is no API to reassign a record.
3. **Scope in every operation** — including by primary key.

## GDPR: export & purge

```go
// Full data export of a tenant (JSON Lines, all models incl. events,
// snapshots and archive):
err = tenants.Export(ctx, id, w)

// Right to be forgotten: physical deletion of ALL of the tenant's data.
// Two-stage: the tenant must be archived (else ErrTenantNotArchived);
// the operation is audited.
err = tenants.Purge(ctx, id)
```

## Backup: import

`Export` on its own is a data subject access request. Together with `Import`
it is a backup: last night's stream turns tenant X back into tenant X.

```go
// Replace, don't merge: the target must be empty or archived.
err = tenants.Archive(ctx, id)
err = tenants.Import(orm.WithGeo(ctx, "eu-central"), id, r)
```

What you can rely on:

- **No silent half state.** During the import the tenant sits at
  `importing` and every write fails with `ErrImportIncomplete`. If the
  import aborts, that status stays — the tenant is visibly incomplete
  rather than quietly half filled. Running the import again discards the
  remains and reaches the correct end state.
- **Truncated streams are caught.** The export ends with a terminator
  line; without it the import refuses.
- **Secrets are re-encrypted** with the target database's current key,
  which makes an import the path for a key rotation as a side effect.
- **The present wins for geo.** The target's home region applies, not the
  export's; with several regions `orm.WithGeo` is mandatory. Otherwise
  every restore would scatter the data across regions again.
- **Writing continues seamlessly.** Events land at the end of the target
  region's geo sequence; read models are re-projected from them.
- **Foreign schema states are rejected**, never silently applied
  (`ErrExportSchemaMismatch`) — allow deliberately with
  `orm.AllowSchemaDrift()`.

See also [Geo-partitioning](/en/guides/geo/).
