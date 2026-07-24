---
title: Architektur
description: Schichtenmodell, unveränderliche Prinzipien und physisches Schema von ORM++.
sidebar:
  order: 0
---

Diese Seite zeigt das große Bild: wie ORM++ intern geschichtet ist, welche
Regeln sich nie ändern, und wie die Daten physisch auf der Platte liegen. Für
die praktische Anwendung der einzelnen Bausteine siehe die [Anleitungen](/guides/models/);
diese Seite erklärt das Warum dahinter.

## Schichtenmodell

```
┌──────────────────────────────────────────────────────┐
│ Public API (Go)                                       │
│  Register[T] · Query[T] · Command/Append · WaitFor     │
│  Bootstrap · Migrate · Rebuild · Snapshot/Archive       │
├──────────────────────────────────────────────────────┤
│ Core Engine (dialektunabhängig)                        │
│  Model-Registry (Reflection)   Schema-Planner/Diff      │
│  Event Store + Outbox          Projektions-Runtime      │
│  Snapshot/Archiv-Manager       Lease-Koordinator        │
│  Migrations-Orchestrator (Expand/Contract/DualWrite)    │
├──────────────────────────────────────────────────────┤
│ Dialekt-Adapter (dünn)                                  │
│  sqlite (modernc)  ·  postgres (pgx)  ·  yugabyte (pgx) │
│  DDL-Gen · Upsert · JSON(B) · Partitionierung ·          │
│  Koordinations-Primitiva (Lease-Tabelle, kein            │
│  Advisory-Lock-Zwang → YB-kompatibel)                    │
└──────────────────────────────────────────────────────┘
```

Die App-Schicht spricht ausschließlich die Public API. Die Core Engine kennt
kein SQL-Dialekt-Detail — sie kompiliert Modelle, plant Schema-Diffs und
orchestriert Migrationen und Projektionen gegen ein schmales `dialect`-Interface.
Erst die Dialekt-Adapter übersetzen das in SQLite-, PostgreSQL- oder
YugabyteDB-spezifisches SQL. Ein neues Backend heißt: einen Adapter schreiben,
Core und Public API bleiben unverändert.

## Unveränderliche Prinzipien

Diese fünf Regeln gelten für jede Zeile Code in ORM++ und werden nie gebrochen:

1. **Verhaltensgleichheit über allen Backends.** SQLite, PostgreSQL und
   YugabyteDB verhalten sich für App-Code identisch — gleiche API, gleiche
   Fehler, gleiche Semantik. Emulation, wo nötig (SQLite kollabiert Geo-Regionen
   auf eine, emuliert Snapshot-Intervalle über Nebentabellen), native
   Implementierung, wo vorhanden. App-Code verzweigt **nie** nach Backend — ein
   `db.Kind()` existiert bewusst nicht.
2. **Tenant fail-closed.** Jede Operation ohne Tenant im `context.Context`
   schlägt mit `ErrNoTenant` fehl — kein Default, keine leere ID, kein
   Silent-Fallback. Einzige Ausnahme: `TenantFree`-Modelle für technische
   Tabellen ohne Nutzerdaten.
3. **Kein SQL im App-Code.** Die Library generiert alles SQL selbst. Die App
   deklariert Modelle mit Go-Struct-Tags und ruft typisierte
   Repository-Methoden auf.
4. **Keine Reflection im Hot Path.** Struct-Tags werden einmalig bei
   `Register[T]()` kompiliert (Tag-Parsing → Mapping-Plan). Im I/O-Pfad
   passieren nur noch Index-Lookups.
5. **Immutable Events.** Event-Log-Zeilen werden nie geändert oder gelöscht —
   nur archiviert (siehe [Event Sourcing](/guides/event-sourcing/)).

Der Grund für Prinzip 1 ist der eigentliche Zweck des Projekts: dieselbe
Anwendung soll unverändert auf SQLite (Demo/Desktop), PostgreSQL (On-Prem) und
YugabyteDB (geo-verteilte Cloud) laufen. Die Verhaltens-Testsuite läuft deshalb
**unverändert** gegen alle drei Backends — es gibt keine backend-spezifischen
Assertions im Testcode.

## Systemtabellen

Alle von ORM++ selbst verwalteten Tabellen tragen das Präfix `ormpp_` und sind
— bis auf das Tenant- und das Topologie-Register — selbst wieder
mandantenfähig:

| Tabelle | Zweck |
|---|---|
| `ormpp_schema_state` | Globaler Migrationszustand (genau eine Zeile) |
| `ormpp_schema_history` | Audit aller Versionswechsel |
| `ormpp_instances` | Instanzregister: welche App-Instanz läuft wo, mit welcher Version |
| `ormpp_migration_progress` | Checkpoints der Backfill-Shards, pro Region |
| `ormpp_deprecated` | Markierte, noch nicht entfernte Felder/Tabellen |
| `ormpp_leases` | Koordination (Migrations-Leader, Projektions-Worker), mit Fencing |
| `ormpp_tenants` | Eingebautes Tenant-Register (siehe [Multi-Tenancy](/guides/multi-tenancy/)) |
| `ormpp_geo_regions` | Topologie-Register mit Lebenszyklus (siehe [Geo-Partitionierung](/guides/geo/)) |
| `ormpp_outbox` / `ormpp_checkpoints` | Event-Trigger-Kette und Projektions-Stände |

Details zu Migrations-Zustandsmaschine und Geo-Modi stehen in den jeweiligen
Anleitungen — diese Tabelle ist die Landkarte, nicht die Bedienungsanleitung.

## Physisches Schema: Event-Sourcing-Modelle

Pro Event-Sourced-Model entstehen drei Tabellen (Beispiel `zones`):

| Tabelle | Inhalt |
|---|---|
| `zones` | Read-Model: eine Zeile pro Aggregat, Spalten aus dem Struct plus `aggregate_seq`. Der Query-Builder liest hiergegen — Lesen kostet wie bei CRUD. |
| `zones_events` | Append-only-Log. Spalten: `geo`, `tenant_id`, `aggregate_id`, `aggregate_seq`, `seq` (je Geo monoton), `event_id` (UUIDv7), `occurred_at`, `type_id`, `data` (JSON/JSONB). |
| `zones_snapshots` | `aggregate_id`, `aggregate_seq`, `taken_at`, `state`. **Nicht** append-only: ältere Snapshots werden nach Policy gelöscht (Default `KeepLast(2)`). |

Ein Snapshot ist dabei keine separate Berechnung, sondern der serialisierte
Aggregat-Zustand nach Event N — derselbe `Apply`-Code, der auch beim Laden
faltet. Das vermeidet eine zweite Divergenzquelle zwischen Snapshot und
Event-Replay.

`seq` ist bewusst **je Geo monoton, nicht global** — eine globale Sequenz wäre
auf einem geo-verteilten Cluster ein Hotspot. Garantiert ist strikte Ordnung
pro Aggregat (`aggregate_seq`) und pro Region (`seq`); es gibt keine
Totalordnung über Regionen hinweg.

## Paketaufbau

ORM++ lebt komplett im Root-Package `orm` — kein `src`-Verzeichnis, keine
Unterpakete für die öffentliche API. Die wichtigsten Dateien:

| Datei | Verantwortung |
|---|---|
| `registry.go` | Tag-Parsing, Validierung, Referenzauflösung, Topo-Sort |
| `driver.go` / `sqlite.go` / `postgres.go` | Treiber-Interface und die drei Dialekt-Implementierungen |
| `db.go` | `Open`, `Register[T]`, `Migrate`, Worker-Lebenszyklus |
| `schema.go` | DDL-Generierung, additiver Diff |
| `repo.go` | `Repository[T]` — CRUD auf klassischen Modellen |
| `aggregate.go` / `event.go` | `orm.Aggregate`, Event-Fold-Kern |
| `projection.go` | Worker-Loop, Checkpoints, `OnEvent`/`Watch`, Snapshots |
| `migrator.go` | Zustandsmaschine expanding → backfill → dual-write → finalizing |
| `instances.go` | Instanzregister und Leases mit Fencing |

Diese Landkarte richtet sich an alle, die den Code selbst lesen — für die
Verwendung der Library reichen die Anleitungen und die [API-Referenz](/reference/api/).
