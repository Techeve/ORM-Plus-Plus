# AGENTS.md — Kontextdokument für AI-Agenten

Dieses Dokument gibt jedem AI-Agenten alle notwendigen Informationen, um sofort und
korrekt auf diesem Projekt arbeiten zu können. Vor dem ersten Tool-Call hier lesen.

## 1. Projekt auf einen Blick

**Name:** ORM++
**Import-Pfad:** `gitlab.techeve.de/orm-plus-plus/orm-plus-plus`
**Go-Version:** 1.26
**Status:** Phasen 1–3 (CRUD, Event Sourcing, Migrations-Engine) + Phase 4 Kern (PostgreSQL/YugabyteDB-Adapter, Suite läuft backend-identisch) abgeschlossen ✅ — offen: Partitionierung/Archivierung (4b), dann Phase 5 (v1.0-Härtung).

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

### Phase 1 (CRUD) + Phase 2 (Event Sourcing) + Phase 3 (Migration) auf SQLite ✅ (35 Tests, CI grün)

| Datei | Inhalt |
|---|---|
| `orm.go` | Package-Kommentar, `const Version` |
| `id.go` | `orm.ID` (UUIDv7, `[16]byte`), `SingleTenant`, `NewID`, `ParseID` |
| `errors.go` | Alle Sentinel-Fehler (`ErrNotFound`, `ErrNoTenant`, …) |
| `context.go` | `WithTenant`, `WithGeo`, `ReplicateTo`, `ReplicateAll` |
| `options.go` | Alle Option-Typen: `OpenOption`, `ModelMode`, `ModelOption`, `BatchOption`, `Role` |
| `registry.go` | Tag-Parsing, Validierung (inkl. ES: Aggregate-Einbettung, Apply), Referenzauflösung, Topo-Sort, Checksum |
| `driver.go` | `Driver`-Interface, `dialect`-Interface (rebind/columnType/autoPK/forUpdate/Trigger-DDL), `dialq`-Rebind-Wrapper |
| `sqlite.go` | SQLite-Treiber + Dialekt: WAL, FK, txlock=immediate, MaxOpenConns=1 (modernc.org/sqlite) |
| `postgres.go` | PostgreSQL-/YugabyteDB-Treiber + Dialekt (pgx stdlib): $n-Rebind, BIGINT/JSONB/BYTEA, FOR UPDATE, plpgsql-Trigger |
| `db.go` | `DB`-Struct, `Open`, `Register[T]`, `Migrate` (Erstinstallation/No-op/Upgrade), `Tx`, `Topology`, `Tenants`, `StartWorkers`/`Close` |
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
| `migration.go` | Deklarations-API: `MigrationPlan`/`RowsPerSecond`, `MigrationTo`, `ReplaceModel[Old,New]` (V-Suffix-Konvention), `BatchScript`/`Batch` (Checkpoints) |
| `migrator.go` | Zustandsmaschine idle→expanding→backfill→dual-write→finalizing, Backfill (checkpointed/drosselbar), Dual-Write-Trigger + Queue-Drain, `FinalizeMigration`, deprecated-Verwaltung |
| `instances.go` | Instanzregister (`ormpp_instances`, Heartbeat/TTL) + Leases mit Fencing (`ormpp_leases`) |

Tests: `crud_test.go` (Phase 1), `es_test.go` (Phase 2), `migration_test.go` (Phase 3) — laufen **unverändert** gegen alle drei Backends. Backend-Wahl über `ORMPP_TEST_BACKEND` (sqlite|postgres|yugabyte) + `ORMPP_TEST_DSN` (`backend_test.go`: Schema-pro-Test-Isolation). Lokal: `docker compose up -d` (PG auf 5433, YB-YSQL auf 5434), siehe README.

### Absichtliche Stubs / bewusste Grenzen

| API | Fehler / Verhalten | Geplant |
|---|---|---|
| `repo.SetGeo` | gibt Fehler zurück | Phase 4b/5 |
| `Tenants().Export` / `Purge` | gibt Fehler zurück | Phase 5 |
| `encrypted`-Tag | Registrierungsfehler | Phase 5 |
| `MigrationStatus`/`Health` | existiert noch nicht | Phase 5 |
| Native Geo-Partitionierung + Archiv-Tabellen, Snapshot-Kompression | Events bleiben im Hot-Log (eine Tabelle), Snapshots unkomprimiert | Phase 4b |
| Lease-Koordination der Projektions-Worker | ein Worker-Loop pro Instanz, unkoordiniert (Migrations-Leader nutzt Leases bereits) | Phase 4b |
| Dual-Write-Rückrichtung (neu→alt) | einseitig alt→neu via Trigger-Nachlauf; Rück-Transformation als `ReplaceModel`-Option geplant | Phase 5 |

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
- **Neue Abhängigkeiten:** Nur nach Diskussion. Externe Deps: `modernc.org/sqlite` (CGO-frei) und `github.com/jackc/pgx/v5` (PG/YB, laut ROADMAP Phase 4).
- **SQL-Platzhalter:** Engine-Code schreibt immer `?`; der `dialq`-Wrapper rebindet pro Dialekt. Nie `d.sql` direkt für Queries verwenden — immer `d.q()`.

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
go test -race ./...                      # alle Tests (SQLite, Default)
go test -race -run TestFoo ./...         # einzelner Test
FILES=$(git ls-files '*.go'); gofmt -l $FILES  # Format-Check (kein "." — CI-Workaround)
go vet ./...

# Gegen PostgreSQL/YugabyteDB (docker compose up -d):
ORMPP_TEST_BACKEND=postgres ORMPP_TEST_DSN="postgres://orm:orm@localhost:5433/orm" go test -race ./...
ORMPP_TEST_BACKEND=yugabyte ORMPP_TEST_DSN="postgres://yugabyte@localhost:5434/yugabyte" go test -race ./...
```

Die Verhaltens-Testsuite läuft **unverändert** gegen alle drei Backends —
keine backend-spezifischen Assertions einbauen.

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
| 3 | Migrations-Engine: Expand/Contract-Zustandsmaschine, Backfill, Dual-Write-Nachlauf, Finalize, Instanzregister/Leases (5 Tests) | ✅ |
| 4 | PostgreSQL- und YugabyteDB-Adapter | ⏳ nächste |
| 5 | v1.0-Härtung: Encryption, Export/Purge, MigrationStatus/Health | — |

Vollständiger Phasenplan inkl. physischem Schema: [doc/ROADMAP.md](doc/ROADMAP.md).
