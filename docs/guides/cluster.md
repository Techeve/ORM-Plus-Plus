---
title: Worker & Clusterbetrieb
description: StartWorkers, Deployment-Muster, Verhalten im Fehlerfall.
sidebar:
  order: 8
---

## Worker starten

```go
err := db.StartWorkers(ctx)
```

Startet die Hintergrund-Verarbeitung dieser Instanz: Projektionen,
`OnEvent`-Reaktoren, Snapshot- und Archiv-Worker, ggf. Migrations-Shards.
Koordination über **Leases mit Fencing** in der DB — pro Aufgabe arbeitet
clusterweit genau eine Instanz, sticky; fällt sie aus (Lease-TTL bzw. `Close`),
übernimmt eine andere. `ctx`-Abbruch oder `db.Close()` stoppt die Worker sauber.

## Deployment-Muster

**Ein Prozess (SQLite, Desktop/Demo):**

```go
db, _ := orm.Open(orm.SQLite("./app.db"))
registerModels(db)          // Register + Topology + SchemaVersion
db.Migrate(ctx)
db.StartWorkers(ctx)        // trivial: eine Region, ein Prozess
```

**N gleichartige App-Instanzen (Postgres/Yugabyte):** identischer Code — jede
Instanz ruft `Migrate` (nur der Lease-Gewinner arbeitet) und `StartWorkers`
(Leases verteilen die Arbeit). Kein Sonderfall.

**Dedizierte Worker-Prozesse:** gleiches Binary, die Instanz bedient nur
Hintergrund-Arbeit:

```go
db, _ := orm.Open(orm.Yugabyte(dsn),
	orm.InstanceGeo("eu-central"),
	orm.MigrationRole(orm.MigrationWorker),
)
registerModels(db)
db.Migrate(ctx)
db.StartWorkers(ctx)
<-ctx.Done()                // kein HTTP-Server — nur Worker
```

## Verhalten im Fehlerfall

- **Worker-Ausfall:** Lease läuft ab, andere Instanz derselben Region übernimmt
  am Checkpoint.
- **`Append`/CRUD während Migration:** jederzeit erlaubt (Online-Migration);
  Dual-Write übernimmt die Engine.
- **Netzsplit zur DB:** Operationen liefern Fehler des Pools; Worker pausieren
  und nehmen nach Reconnect am Checkpoint wieder auf.
