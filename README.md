# ORM++

Model-first Persistenz-Modul für Go: klassisches ORM-Mapping **plus** Event Sourcing, Projektionen, Snapshots/Archivierung und Expand/Contract-Migrationen — optimiert für wenige, dafür voll ausgereizte Datenbanken:

- **SQLite** — eingebettet, Desktop/Demo-Betrieb
- **PostgreSQL** — Server, On-Prem
- **YugabyteDB** — verteilt, multi-tenant, geo-partitioniert

Die konsumierende Anwendung deklariert Modelle (Go-Structs mit Tags) und arbeitet mit Commands, Events und typisierten Queries — sie schreibt **kein SQL** und kennt keine DB-Details.

- **[API.md](API.md)** — die vollständige API-Spezifikation (Implementierungsvertrag): Instanziierung, Modelle, Queries, Event Sourcing, Migration, Clusterbetrieb.
- **[ROADMAP.md](ROADMAP.md)** — Architektur-Entscheidungen, physisches Schema und Phasenplan.

## Status

In Entwicklung (Phase 0/1) — noch keine stabile API.

## Entwicklung

```sh
go test -race ./...   # Tests
gofmt -l .            # Format-Check
```

### Workflow

- Default-Branch: `develop`. `main` ist geschützt — nur Merge Requests aus `develop` mit grüner Pipeline.
- Commits folgen [Conventional Commits](https://www.conventionalcommits.org) (`feat:`, `fix:`, `refactor:`, …). Daraus wird der [CHANGELOG.md](CHANGELOG.md) generiert.

### Releases & Changelog

Releases werden über Git-Tags `vX.Y.Z` ausgelöst. Die CI-Pipeline generiert dann mit [git-cliff](https://git-cliff.org) den Changelog aus den Conventional Commits und erstellt ein GitLab-Release. Lokal:

```sh
git cliff -o CHANGELOG.md        # kompletten Changelog neu generieren
git cliff --unreleased           # Vorschau der nächsten Release-Notes
```
