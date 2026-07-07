# ORM++ — Entwicklungs-Fahrplan

Stand: 2026-07-07. Basiert auf dem Hand-off-Brief plus Interview-Entscheidungen.

## 1. Festgezurrte Entscheidungen

| Thema | Entscheidung |
|---|---|
| Modell-Deklaration | Go-Structs mit Struct-Tags, Registrierung zur Laufzeit (Reflection + Generics an der API-Oberfläche) |
| Query-API | Typisierter Query-Builder (`Query[T]`, `Where`, `OrderBy`, …), Engine übersetzt pro Dialekt |
| Migration | Expand/Contract mit Versionserkennung in der DB: Schema-Änderungen automatisch, entfallende Felder werden nur **markiert** (nie still gelöscht). Für Datenumbauten schreibt der Entwickler Migrationsprozesse; bei komplexen Änderungen: neues Model + Migrationsprozess. Während der Migration **Dual-Write** in alte und neue Tabelle, sodass alte und neue App-Versionen parallel laufen können. Abschluss der Migration entfernt die alte Tabelle. |
| Konsistenzmodell | Async: Event-Append atomar (Outbox), Projektionen werden von Workern nachgezogen |
| Read-your-writes | Command liefert Event-Position; Lesepfad kann optional darauf warten (WaitFor / Konsistenz-Token, Timeout konfigurierbar) |
| Worker-Topologie | Beides: In-Process-Worker mit Lease-Koordination über die DB **und** optional als dedizierter Worker-Prozess startbar |
| Event-Payloads | JSON(B) (SQLite: JSON-Text), jedes Event trägt Typ + Schema-Version; Upcaster-Funktionen (v1→v2) beim Lesen/Rebuild; Events selbst bleiben unveränderlich |
| Snapshots & Archivierung | Beides ab v1: automatische Snapshots alle N Events pro Aggregat + Auslagerung alter Events in Archiv-/Partitionstabellen (SQLite: Emulation über Nebentabellen) |
| Multi-Tenancy & Geo | Elementar ab v1 im Datenmodell: Tenant-UUID in jedem Datensatz jeder Tabelle; zusätzlich Geo-Felder mit **mehreren Ebenen von Geolokalität** im Design — v1 implementiert zunächst nur eine Ebene, das Schema ist aber für mehrere ausgelegt |
| Backend-Reihenfolge | SQLite zuerst (schnelle Tests, einfache CI-Pipelines); PostgreSQL und YugabyteDB werden aber **noch vor v1.0** integriert |
| Tooling | v1 nur Library-API (auch Migration-Finalisierung ist Go-API); CLI später |
| Repo | gitlab.techeve.de, Gruppe `orm-plus-plus`, Projekt `orm-plus-plus`; zunächst intern, später Open Source (Lizenz noch offen, permissiv angedacht) |

## 2. Architektur-Schichten

```
┌──────────────────────────────────────────────────────┐
│ Public API (Go)                                      │
│  Register[T] · Query[T] · Command/Append · WaitFor   │
│  Bootstrap · Migrate · Rebuild · Snapshot/Archive    │
├──────────────────────────────────────────────────────┤
│ Core Engine (dialektunabhängig)                      │
│  Model-Registry (Reflection)   Schema-Planner/Diff   │
│  Event Store + Outbox          Projektions-Runtime   │
│  Snapshot/Archiv-Manager       Lease-Koordinator     │
│  Migrations-Orchestrator (Expand/Contract/DualWrite) │
├──────────────────────────────────────────────────────┤
│ Dialekt-Adapter (dünn)                               │
│  sqlite (modernc/mattn)  ·  postgres (pgx)  ·  yb    │
│  DDL-Gen · Upsert · JSON(B) · Partitionierung ·      │
│  Koordinations-Primitiva (Lease-Tabelle, kein        │
│  Advisory-Lock-Zwang → YB-kompatibel)                │
└──────────────────────────────────────────────────────┘
```

Grundregeln:
- Jede systemseitige Tabelle (Events, Snapshots, Projektionen, Outbox, Leases, Schema-Versionen, Migrationszustand) trägt von Anfang an `tenant_id UUID` und die Geo-Spalte(n).
- Koordination (Migrations-Leader, Projektions-Leases) läuft über **Lease-Tabellen mit Fencing**, nicht über Postgres-Advisory-Locks — die sind auf YugabyteDB nicht verlässlich verfügbar und auf SQLite ohnehin nicht.
- Kein Feature landet im Core, das nicht auf allen drei Dialekten (nativ oder emuliert) darstellbar ist.

## 3. Phasen

### Phase 0 — Fundament (kurz)
- Repo auf gitlab.techeve.de unter Gruppe `orm-plus-plus` anlegen (develop default, main geschützt, Conventional Commits, Commit-Identität Claude/claude@techeve.de).
- Go-Modul, Linting, CI-Pipeline (zunächst nur SQLite-Tests — läuft ohne Dienste).
- ADR-Verzeichnis: die Entscheidungen aus Abschnitt 1 als ADRs 001–01x festhalten.

### Phase 1 — Core auf SQLite: Modelle, Schema, CRUD, Query
- Model-Registry: Struct-Tags parsen (Spaltentypen, PK, Indizes, `es`/`crud`-Modus, tenant/geo-Scoping), Validierung beim Registrieren.
- Schema-Planner: deklarierte Modelle → DDL; Bootstrap legt Tabellen inkl. Systemtabellen an; Schema-Versionsstand in der DB.
- CRUD-Pfad: Insert/Update/Delete/Get für klassische Modelle, Tenant-Filter erzwungen via `context`.
- Typisierter Query-Builder inkl. Übersetzungsschicht (Dialekt-Interface von Tag 1, auch wenn es erst eine Implementierung gibt).
- Meilenstein: Demo-App legt Modelle an, bootstrapt SQLite-Datei, liest/schreibt tenant-gescoped.

### Phase 2 — Event Sourcing: Append, Outbox, Projektionen, Snapshots
- Event-Store-Tabellen (Events append-only, Typ + Schema-Version, JSON-Payload, globale + Aggregat-Sequenz, tenant/geo).
- Command-Pfad: Command → Event(s) atomar anhängen + Outbox-Eintrag; Rückgabe der Event-Position.
- Projektions-Runtime: Worker konsumiert Outbox, materialisiert Read-Models; Checkpoints pro Projektion; Rebuild aus dem Event-Strom.
- Lease-Koordinator (auf SQLite trivial: ein Prozess) + WaitFor-Mechanik für Read-your-writes.
- Upcaster-Registry für Event-Schema-Versionen.
- Snapshots: automatisch alle N Events, Laden = Snapshot + Restevents.
- Meilenstein: event-sourced Modell mit Projektion, Rebuild und WaitFor auf SQLite, alles unter Test.

### Phase 3 — Migrations-Engine (Expand/Contract, Dual-Write)
Das Herzstück und der schwierigste Teil — bewusst nach Phase 1/2, weil sie auf Schema-Planner und Versionsstand aufsetzt:
- Diff-Erkennung: deklarierte Modelle vs. Ist-Schema; additive Änderungen automatisch; entfallende Felder → Markierung (`deprecated`), kein Drop.
- Migrationsprozesse: Entwickler registriert Migrations-Hooks (alt→neu-Transformation) am Model; Engine orchestriert.
- Dual-Write-Phase: Schreibpfad schreibt in alte + neue Tabelle; Lesepfad pro App-Version auf „ihrer" Tabelle; Migrationszustand (expand → backfill → dual-write → finalize) in der DB versioniert.
- Backfill-Worker: Bestandsdaten alt→neu kopieren/transformieren, wiederaufnehmbar, drosselbar.
- Finalisierung als explizite API: Dual-Write beenden, alte Tabelle entfernen — erst wenn der Betreiber bestätigt, dass alle Instanzen migriert sind.
- Meilenstein: Zwei App-Versionen laufen gleichzeitig gegen dieselbe DB durch eine komplette Expand/Contract-Migration, ohne Datenverlust (Testszenario in CI).

### Phase 4 — PostgreSQL- und YugabyteDB-Adapter
- pgx-Adapter: JSONB, Upsert (`ON CONFLICT`), echte Nebenläufigkeit für Leases/Outbox (`FOR UPDATE SKIP LOCKED`), Connection-Pooling.
- CI erweitern: identische Test-Suite läuft gegen SQLite, Postgres **und** YugabyteDB (Container); Dialekt-Unterschiede (Sequenzen, Locking, Timeouts) hier ausbügeln.
- Partitionierung der Event-/Archivtabellen auf PG/YB nativ; SQLite emuliert über Nebentabellen.
- Archivierung ab hier vollwertig: alte Events (vor Snapshot) in Archivtabellen/-partitionen auslagern, historische Reads greifen transparent auf Archiv zu.
- Meilenstein: gesamte Suite grün auf allen drei Backends.

### Phase 5 — v1.0-Härtung
- Standalone-Worker-Modus (gleiche Library, eigenes Binary-Muster).
- Geo-Ebene 1 produktiv nutzbar (Spalten + Filter); Mehr-Ebenen-Design dokumentiert für Stufe 2.
- Lasttests (Append-Durchsatz, Projektions-Lag, Backfill unter Last), Fehlerinjektion (Worker-Ausfall mitten in Migration/Projektion).
- Doku + Beispielprojekt (Mini-Version des DNS-Tool-Musters: ES-Kern + CRUD-Rest).
- Lizenzentscheidung, dann Veröffentlichung.

### Stufe 2 (nach v1.0, nur notiert)
- YB-Geo-Partitionierung mit mehreren Geolokalitäts-Ebenen, Row-Level Security nativ (PG/YB) + SQLite-Emulation, CLI-Tool, Admin-HTTP-Endpoint, Point-in-time-Reads als First-Class-API.

## 4. Größte Risiken

1. **Dual-Write-Migration** ist die anspruchsvollste Komponente (Konfliktfälle: Schreiben in alt während Backfill läuft; Reihenfolge-Garantien). Früh ein präzises Zustandsmodell (State machine) definieren und als ADR festhalten, bevor Code entsteht.
2. **SQLite-Nebenläufigkeit**: eine Schreib-Connection, WAL-Modus Pflicht; Outbox/Worker-Design darf nicht stillschweigend Postgres-Semantik (SKIP LOCKED) voraussetzen.
3. **YugabyteDB-Abweichungen** trotz PG-Wire: Advisory Locks, Serialisierungsverhalten, Sequenz-Caching. Deshalb ab Phase 4 sofort in CI, nicht erst am Ende.
4. **Reflection-Performance** im heißen Pfad: Mapping-Pläne pro Typ einmalig beim Registrieren kompilieren und cachen, nicht pro Query reflektieren.
