---
title: Errors
description: All sentinel errors, checkable with errors.Is.
sidebar:
  order: 3
---

All errors are sentinel values checkable with `errors.Is`.

| Error | Meaning |
|---|---|
| `orm.ErrNotFound` | Record/aggregate does not exist (within tenant/geo scope) |
| `orm.ErrNoTenant` | Context without tenant (fail-closed) |
| `orm.ErrUnknownTenant` | Tenant ID not in the registry or archived |
| `orm.ErrRequiredField` | required field not set at insert (zero value) |
| `orm.ErrInvalidReference` | ref target does not exist or belongs to another tenant |
| `orm.ErrReferenceInUse` | Delete refused: record still referenced |
| `orm.ErrInvalidValue` | Value outside the enum set |
| `orm.ErrRequiresTx` | Operation (e.g. GetForUpdate) outside a transaction |
| `orm.ErrTenantNotArchived` | Purge on a non-archived tenant |
| `orm.ErrNoGeo` | Multi-region topology but no data geo in context |
| `orm.ErrRegionNotActive` | Data geo points at a bootstrapping/draining/unknown region |
| `orm.ErrVersionConflict` | Optimistic locking: CRUD version or aggregate version stale |
| `orm.ErrWaitTimeout` | WaitFor deadline elapsed, projection lagged behind |
| `orm.ErrSchemaDrift` | Models changed without a SchemaVersion bump |
| `orm.ErrMigrationPending` | Operation requires a completed migration |
| `orm.ErrReadOnlyReplica` | Write access to a replica under WriteHomeOnly |
