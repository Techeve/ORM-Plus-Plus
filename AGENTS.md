# AGENTS.md — Kontextdokument für AI-Agenten

Dieses Dokument gibt jedem AI-Agenten alle notwendigen Informationen, um sofort und
korrekt auf diesem Projekt arbeiten zu können. Vor dem ersten Tool-Call hier lesen.

## 1. Projekt auf einen Blick

**Name:** ORM++
**Import-Pfad:** `gitlab.techeve.de/orm-plus-plus/orm-plus-plus`
**Go-Version:** 1.26
**Status:** Phase 1 (CRUD) + Phase 2 (Event Sourcing) auf SQLite abgeschlossen ✅ — Phase 3 (Migrations-Engine) als Nächstes.

ORM++ ist eine Go-Library für model-first Persistenz: klassisches ORM-Mapping **plus**
Event Sourcing, Projektionen, Snapshots und Expand/Contract-Migrationen — optimiert für
SQLite (Demo/Test), PostgreSQL (On-Prem) und YugabyteDB (geo-distributed).
Die konsumierende Anwendung deklariert Go-Structs, schreibt **kein SQL** und kennt keine
DB-Details.

## 2. Spezifikationsdokumente

| Dokument | Zweck |
|---|---|
| [doc/API.md](doc/API.md) | Vollständige öffentliche API-Spezifikation — **Implementierungsvertrag**. Abweichungen in der Implementierung sind Fehler oder müssen hier nachgezogen werden. |
| [doc/ROADMAP.md](doc/ROADMAP.md) | Architektur-Entscheidungen, physisches Schema, 6-Phasen-Plan, Risiken. |
| [doc/TASK.md](doc/TASK.md) | Aktueller Implementierungsstand, abgeschlossene Schritte, offene Punkte. **Hier zuerst lesen** bevor Code geschrieben wird. |

## 3. Unveränderliche Prinzipien (niemals brechen)

1. **Verhaltensgleichheit über allen Backends.** SQLite, PostgreSQL, YugabyteDB verhalten
   sich für App-Code identisch — gleiche API, gleiche Fehler, gleiche Semantik. Emulation
   wo nötig (SQLite kollabiert Geo, emuliert Snapshot-Intervalle), native Implementierung
   wo vorhanden. App-Code verzweigt **nie** nach Backend. `db.Kind()` o. Ä. existiert nicht.

2. **Tenant fail-closed.** Jede Operation ohne Tenant im Context schlägt mit `ErrNoTenant`
   fehl — kein Default, keine leere ID, kein Silent-Fallback. Ausnahme: `TenantFree`-Modelle.

3. **Kein SQL im App-Code.** Die Library generiert alles SQL. Die App deklariert Modelle
   mit Go-Struct-Tags und ruft typisierte Repository-Methoden auf.

4. **Keine Reflection im Hot Path.** Struct-Tags werden einmalig bei `Register[T]()`
   kompiliert (Tag-Parsing → `[]field`-Slice). Im I/O-Pfad nur Index-Lookups.

5. **Immutable Events (Phase 2+).** Event-Log-Zeilen werden nie geändert oder gelöscht —
   nur archiviert.

## 4. Implementierungsstand

### Phase 1 (CRUD) + Phase 2 (Event Sourcing) auf SQLite ✅ (30 Tests, CI grün)

| Datei | Inhalt |
|---|---|
| `orm.go` | Package-Kommentar, `const Version` |
| `id.go` | `orm.ID` (UUIDv7, `[16]byte`), `SingleTenant`, `NewID`, `ParseID` |
| `errors.go` | Alle Sentinel-Fehler (`ErrNotFound`, `ErrNoTenant`, …) |
| `context.go` | `WithTenant`, `WithGeo`, `ReplicateTo`, `ReplicateAll` |
| `options.go` | Alle Option-Typen: `OpenOption`, `ModelMode`, `ModelOption`, `BatchOption`, `Role` |
| `registry.go` | Tag-Parsing, Validierung (inkl. ES: Aggregate-Einbettung, Apply), Referenzauflösung, Topo-Sort, Checksum |
| `driver.go` | `Driver`-Interface, `dialect`-Interface, `Postgres`/`Yugabyte` Stubs (Phase 4) |
| `sqlite.go` | SQLite-Treiber: WAL, FK, txlock=immediate, MaxOpenConns=1 (modernc.org/sqlite) |
| `db.go` | `DB`-Struct, `Open`, `Register[T]`, `Migrate`, `Tx`, `Topology`, `Tenants`, `StartWorkers`/`Close` |
| `schema.go` | DDL-Generierung (CRUD + ES-Read-Model/Events/Snapshots), additiver Diff |
| `values.go` | Typ-Konversionen, `scanModelRows[T]` (inkl. Aggregat-Verdrahtung bei ES) |
| `tenants.go` | `TenantRegistry`: Create/Get/List/Archive + Cache + Insert-Verifikation |
| `repo.go` | `Repository[T]` (CRUD; auf ES-Modellen gesperrt) |
| `query.go` | `QueryBuilder[T]` + freies `orm.Query[T]` (läuft auch gegen ES-Read-Models) |
| `event.go` | `Event`, `CloudEvent`, `Position`, `Events`/`E`/`V`, Typ-Wörterbuch (`ormpp_event_types`) |
| `aggregate.go` | `orm.Aggregate`: `New`/`Load`/`Append`/`Refresh`/`AtVersion`/`AtTime`/`History`, Fold-Kern |
| `upcast.go` | `orm.Upcast`, Dekodierung mit Upcaster-Kette, Migrate-Validierung |
| `projection.go` | Worker-Loop, Checkpoints, eingebaute Projektion, `OnEvent`/`Named`, Snapshots, `RebuildProjection`/`RebuildView`, WaitFor |
| `stream.go` | `Stream`, `Watch`, In-Process-Live-Hub |
| `migration.go` | `ReplaceModel[Old,New]` Stub (Phase 3) |

Tests: `crud_test.go` (Phase 1), `es_test.go` (Phase 2) — laufen später unverändert gegen PG/YB.

### Absichtliche Stubs (existieren als API, noch nicht implementiert)

| API | Fehler / Verhalten | Geplant |
|---|---|---|
| `repo.SetGeo` | gibt Fehler zurück | Phase 3 |
| `Tenants().Export` | gibt Fehler zurück | Phase 5 |
| `Tenants().Purge` | gibt Fehler zurück | Phase 5 |
| `FinalizeMigration` | gibt Fehler zurück | Phase 3 |
| `Postgres`/`Yugabyte` Driver | gibt Fehler zurück | Phase 4 |
| `encrypted`-Tag | Registrierungsfehler | Phase 3/5 |
| Archiv-Tabellen, Snapshot-Kompression | Events bleiben im Hot-Log, Snapshots unkomprimiert | Phase 4 |
| Lease-Koordination der Worker | ein Prozess = ein Worker-Loop | Phase 3/4 |

## 5. Coding-Konventionen

- **Package:** Alles im Root-Package `orm`. Kein `src/`-Unterverzeichnis (idiomatic Go).
  Interna wandern später ggf. nach `internal/`, wenn der Umfang es erfordert.
- **Kommentare:** Nur wenn das WIE oder WARUM nicht offensichtlich ist. Kein "was der
  Code tut", keine mehrzeiligen Docstrings. Einzeiler max.
- **Tabellennamen:** `snake_case` des Struct-Namens (`ProviderAccount` → `provider_account`).
- **Spaltennamen:** `snake_case` mit Akronym-Behandlung (`APIKey` → `api_key`).
- **Implizite Spalten:** Jede Tabelle außer `TenantFree` hat `tenant_id TEXT NOT NULL`
  und `geo TEXT NOT NULL DEFAULT 'local'`. Reads filtern **immer** auf `tenant_id`,
  **nie** auf `geo` (Geo ist Platzierung, kein Security-Scope).
- **Typen-Speicherung:** IDs → TEXT (UUID-String), Zeit → TEXT (RFC3339Nano),
  bool → INTEGER, json-Tag → TEXT, `[]byte` → BLOB.
- **Unique-Constraints:** Beziehen `tenant_id` automatisch ein.
- **Fehler:** `fmt.Errorf("orm: …: %w", sentinelErr)` — immer Package-Präfix + Sentinel.
- **Neue Abhängigkeiten:** Nur nach Diskussion. Einzige externe Dep. ist `modernc.org/sqlite`.

## 6. Datenmodell-Invarianten

- `tenant_id` ist **write-once** — nie im UPDATE, nie änderbar nach Insert.
- `version`-Feld (optimistisches Locking) wird nur von der Engine inkrementiert, nie
  von App-Code gesetzt.
- `orm.ID` ist immer UUIDv7 (time-sortable, client-side generated) — keine DB-Auto-Increment.
- Das Tenant-Register (`ormpp_tenants`) ist ein GeoGlobal/TenantFree-Systemmodell
  ohne `tenant_id`-Spalte.
- `ormpp_schema_state` hat genau eine Zeile (id=1, CHECK-Constraint).

## 7. Tests & lokale Befehle

```sh
go test -race ./...                      # alle Tests mit Race-Detector
go test -race -run TestFoo ./...         # einzelner Test
FILES=$(git ls-files '*.go'); gofmt -l $FILES  # Format-Check (kein "." — CI-Workaround)
go vet ./...
```

Die Verhaltens-Testsuite (`crud_test.go`, Phase 1) läuft später **unverändert** gegen
PostgreSQL und YugabyteDB. Keine backend-spezifischen Assertions einbauen.

## 8. Git-Workflow

- **Default-Branch:** `develop`. `main` ist geschützt — nur MRs aus `develop` mit grüner Pipeline.
- **Commits:** [Conventional Commits](https://www.conventionalcommits.org):
  `feat:`, `fix:`, `refactor:`, `docs:`, `test:`, `ci:`, `chore:`.
- **Commit-Identität (lokal):** `user.name "Claude"`, `user.email "claude@techeve.de"`.
- **Releases:** via Git-Tag `vX.Y.Z` — CI generiert Changelog (git-cliff) und GitLab-Release.
- **Kein direkter Push auf `main`** — immer via MR.

## 9. Phasenplan

| Phase | Inhalt | Status |
|---|---|---|
| 0 | Repo, CI, Branch-Schutz, API.md, ROADMAP.md | ✅ |
| 1 | CRUD auf SQLite, Model-Registry, Schema, Tenants (17 Tests) | ✅ |
| 2 | Event Sourcing: `orm.Aggregate`, Append, Projektion, OnEvent/Watch, Upcaster, Snapshots, WaitFor (12 Tests) | ✅ |
| 3 | Migrations-Engine: Expand/Contract, Dual-Write, Backfill-Worker | ⏳ nächste |
| 4 | PostgreSQL- und YugabyteDB-Adapter | — |
| 5 | v1.0-Härtung: Encryption, Export/Purge, MigrationStatus/Health | — |

Vollständiger Phasenplan inkl. physischem Schema: [doc/ROADMAP.md](doc/ROADMAP.md).
