---
title: Schema versions & migration
description: Expand/contract with dual-write — zero-downtime migration.
sidebar:
  order: 4
---

## Declaration

```go
orm.SchemaVersion(db, 3)   // schema version this app build expects

orm.MigrationTo(db, 3,     // steps from 2 to 3; older MigrationTo stay in the code
	orm.ReplaceModel[ZoneV2, DNSZone](func(ctx, old ZoneV2) (DNSZone, error) {
		return DNSZone{ /* rebuild */ }, nil
	}),
	orm.BatchScript("normalize-records", func(ctx, b orm.Batch) error {
		// b yields rows in chunks; the engine manages the checkpoint
		return nil
	}),
)
```

- **Additive changes** (new column/index/model) need no step (auto-diff).
- **Dropped fields** are marked `deprecated`, never deleted automatically.
- **Drift protection:** models changed without a version bump ⇒ startup error (checksum).

## Execution

```go
err := db.Migrate(ctx)   // normal case, idempotent
```

`Migrate` runs the state machine
`idle → expanding → backfill → dual-write → finalizing → idle`:

- **expanding:** additive DDL, global, one leader (lease).
- **backfill:** geo-parallel; the work unit is the shard `(step, geo, key
  range)`, leased only to workers **in the same region**. Resumable, throttleable.
- **dual-write:** old instances keep running unchanged; their changes are
  continuously carried into the new structure. Both app generations coexist.
- **finalizing:** explicit — ends dual-write, removes `deprecated` fields and
  old tables.

```go
err := db.FinalizeMigration(ctx, 3)
```

Precondition (checked): no live instance with an older schema version in the
register, all regions finished backfilling.

## Cluster rollout

1. Roll out the new app version (schema version n+1) region by region —
   `Migrate` moves to `expanding`, then `backfill`, then `dual-write`.
2. Retire old instances (the register empties).
3. `FinalizeMigration` — from an ops job or manually.

## Example: batch migration script with its own checkpoint

`BatchScript` itself doesn't prescribe an iteration strategy — the script
uses the normal query and update APIs and manages its own progress. Here's a
normalization that resumes exactly where it left off after a restart:

```go
orm.MigrationTo(db, 4,
	orm.BatchScript("normalize-email", func(ctx context.Context, b orm.Batch) error {
		last, err := b.Checkpoint(ctx) // "" on the very first run
		if err != nil {
			return err
		}

		var processed int64
		for account, err := range orm.Query[ProviderAccount](db, ctx).
			Where(orm.Gt("ID", last)).
			OrderBy("ID", orm.Asc).
			Iter() {
			if err != nil {
				return err
			}
			normalized := strings.ToLower(strings.TrimSpace(account.Email))
			if normalized == account.Email {
				continue
			}
			if _, err := orm.Query[ProviderAccount](db, ctx).
				Where(orm.Eq("ID", account.ID)).
				UpdateSet(orm.Set("Email", normalized)); err != nil {
				return err
			}
			processed++
			if processed%500 == 0 { // checkpoint every 500 rows, not every single one
				if err := b.SaveCheckpoint(ctx, account.ID.String(), processed); err != nil {
					return err
				}
			}
		}
		return nil
	}),
)
```

If the process dies mid-run, the next attempt finds the last saved ID via
`b.Checkpoint(ctx)` and resumes exactly there through the
`Where(orm.Gt("ID", last))` condition — the script only needs to be
idempotent to set up, not recompute the whole backfill.
