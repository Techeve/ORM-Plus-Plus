# ORM++

Model-first Persistenz-Modul für Go: klassisches ORM-Mapping **plus** Event Sourcing, Projektionen, Snapshots/Archivierung und Expand/Contract-Migrationen — optimiert für wenige, dafür voll ausgereizte Datenbanken:

- **SQLite** — eingebettet, Desktop/Demo-Betrieb
- **PostgreSQL** — Server, On-Prem
- **YugabyteDB** — verteilt, multi-tenant, geo-partitioniert

Die konsumierende Anwendung deklariert Modelle (Go-Structs mit Tags) und arbeitet mit Commands, Events und typisierten Queries — sie schreibt **kein SQL** und kennt keine DB-Details.

- **[doc/API.md](doc/API.md)** — die vollständige API-Spezifikation (Implementierungsvertrag): Instanziierung, Modelle, Queries, Event Sourcing, Migration, Clusterbetrieb.
- **[doc/ROADMAP.md](doc/ROADMAP.md)** — Architektur-Entscheidungen, physisches Schema und Phasenplan.

## Status

In Entwicklung (Phase 0/1) — noch keine stabile API.

## Entwicklung

```sh
go test -race ./...   # Tests (SQLite, Default)
gofmt -l .            # Format-Check
```

### Tests gegen PostgreSQL und YugabyteDB

Die **identische** Verhaltenssuite läuft gegen alle drei Backends. Lokal via Docker:

```sh
docker compose up -d   # startet PostgreSQL (Port 5433) und YugabyteDB (YSQL, Port 5434)

ORMPP_TEST_BACKEND=postgres ORMPP_TEST_DSN="postgres://orm:orm@localhost:5433/orm" go test -race ./...
ORMPP_TEST_BACKEND=yugabyte ORMPP_TEST_DSN="postgres://yugabyte@localhost:5434/yugabyte" go test -race ./...
```

Jeder Test bekommt dabei ein eigenes Schema (search_path) und räumt es am Ende weg. Die CI fährt dieselbe Matrix (SQLite, PostgreSQL, YugabyteDB als Services).

### Workflow

- Default-Branch: `develop`. `main` ist geschützt — nur Merge Requests aus `develop` mit grüner Pipeline.
- Commits folgen [Conventional Commits](https://www.conventionalcommits.org) (`feat:`, `fix:`, `refactor:`, …). Daraus wird der [CHANGELOG.md](CHANGELOG.md) generiert.

### Releases & Changelog

Releases werden über Git-Tags `vX.Y.Z` ausgelöst. Die CI-Pipeline generiert dann mit [git-cliff](https://git-cliff.org) den Changelog aus den Conventional Commits und erstellt ein GitLab-Release. Lokal:

```sh
git cliff -o CHANGELOG.md        # kompletten Changelog neu generieren
git cliff --unreleased           # Vorschau der nächsten Release-Notes
```
