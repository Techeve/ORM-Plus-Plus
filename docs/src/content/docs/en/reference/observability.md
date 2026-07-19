---
title: Observability
description: MigrationStatus and Health — the physical truth for the operator.
sidebar:
  order: 2
---

Observability APIs show the **operator** the physical truth — they are the one
place backends may differ (on SQLite you honestly see a region `local` with one
worker).

```go
st, err := db.MigrationStatus(ctx)
// st.Phase                        "backfill"
// st.CurrentVersion / TargetVersion
// st.Geo["eu-central"].Percent    87.3
// st.Geo["eu-central"].Workers    4

h, err := db.Health(ctx)
// h.Instances    live instances (geo, role, app/schema version, heartbeat)
// h.Projections  lag per projection/region (events behind the log)
// h.Regions      topology status (active/bootstrapping/draining)
```

Both return plain data structures — wiring them to logging/metrics/health
endpoints is the app's job.
