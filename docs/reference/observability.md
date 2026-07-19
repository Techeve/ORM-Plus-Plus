---
title: Observability
description: MigrationStatus und Health — die physische Wahrheit für den Betreiber.
sidebar:
  order: 2
---

Observability-APIs zeigen dem **Betreiber** die physische Wahrheit — sie sind
die einzige Stelle, an der Backends sich unterscheiden dürfen (auf SQLite
erscheint ehrlich eine Region `local` mit einem Worker).

```go
st, err := db.MigrationStatus(ctx)
// st.Phase                        "backfill"
// st.CurrentVersion / TargetVersion
// st.Geo["eu-central"].Percent    87.3
// st.Geo["eu-central"].Workers    4

h, err := db.Health(ctx)
// h.Instances    lebende Instanzen (Geo, Rolle, App-/Schema-Version, Heartbeat)
// h.Projections  Lag je Projektion/Region (Events hinter dem Log)
// h.Regions      Topologie-Status (active/bootstrapping/draining)
```

Beide liefern reine Datenstrukturen — die Anbindung an Logging/Metrics/Health-
Endpoints ist Sache der App.
