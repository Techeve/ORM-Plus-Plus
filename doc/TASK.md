# TASK.md — Arbeitsstand der Implementierung

Fortschritts-Log für die Umsetzung nach [ROADMAP.md](ROADMAP.md) (Phasen) und [API.md](API.md) (Vertrag).
Nach jedem abgeschlossenen Schritt wird diese Datei aktualisiert und committet — bei Abbruch hier weiterlesen statt das Projekt neu zu scannen.

## Aktueller Schritt

**Phase 2 abgeschlossen** (Event Sourcing auf SQLite, alle Tests + Lint grün). Nächster Schritt: Phase 3 — Migrations-Engine (Expand/Contract-Zustandsmaschine, Dual-Write, Backfill-Worker, `ReplaceModel`/`BatchScript`-Ausführung, `FinalizeMigration`).

## Phase-2-Arbeitsplan (Reihenfolge)

| # | Schritt | Status |
|---|---|---|
| 2.1 | Event-Deklaration: `orm.Events`/`orm.E`/`orm.V`, ES-Validierung in der Registry (Aggregate-Einbettung, Apply, keine pk/id-Kollision) | ✅ |
| 2.2 | Physisches Schema: Read-Model (implizite `id` + `aggregate_seq`), `<t>_events` (PK aggregate_id+seq, Unique geo+seq), `<t>_snapshots`; Systemtabellen `ormpp_event_types`, `ormpp_checkpoints` | ✅ |
| 2.3 | `orm.Aggregate`: `New`/`Load`/`Append` (atomar, optimistisch, auch in Tx), `Refresh`, `AtVersion`/`AtTime`, `History` | ✅ |
| 2.4 | Typ-Wörterbuch + Upcaster: `orm.Upcast`, Ketten-Validierung bei `Migrate` (Startfehler statt Lesefehler) | ✅ |
| 2.5 | Projektions-Runtime: Worker (`StartWorkers`), Checkpoints je (Consumer, Geo), Read-Model-Upsert, `RebuildProjection` | ✅ |
| 2.6 | Trigger-Kette: `OnEvent` (persistent, at-least-once, transaktionaler Checkpoint, `orm.Named`), `RebuildView`, `Watch` (flüchtiger Live-Hub), `Stream` | ✅ |
| 2.7 | Read-your-writes: `orm.Position` (Cursor-Vektor je Geo), `WaitFor` an `Load`/`Refresh` (`ErrWaitTimeout`) | ✅ |
| 2.8 | Snapshots: asynchron im Worker, `SnapshotEvery`/`SnapshotMaxAge`/`SnapshotKeepLast`/`SnapshotDisabled`, `SnapshotMarshal`/`SnapshotUnmarshal`-Opt-in, Laden = Snapshot + Restevents | ✅ |
| 2.9 | Verhaltens-Testsuite Phase 2 (`es_test.go`, 12 Tests) | ✅ |

## Phase-2-Notizen (Entscheidungen & bewusste Grenzen)

- **Neue Dateien:** `event.go` (Event/CloudEvent/Position/Deklarationen/Typ-Wörterbuch), `aggregate.go` (Aggregate, New/Load/Append/Refresh/AtVersion/AtTime/History), `upcast.go`, `projection.go` (Worker/Checkpoints/OnEvent/Snapshots/Rebuild/WaitFor), `stream.go` (Stream/Watch/Live-Hub), Tests in `es_test.go`.
- **Load faltet immer autoritativ** (Snapshot + Restevents durch `Apply`) — es hängt nie hinter dem Log. `WaitFor` betrifft die eingebaute **Projektion** (Read-Model für den Query-Builder).
- **Append-Konfliktprüfung:** Konflikt, wenn die Log-Spitze **über** der geladenen Version liegt; kleiner ist erlaubt (Events vor dem Snapshot archiviert). Duplikate verhindert der PK `(aggregate_id, aggregate_seq)`.
- **Apply läuft innerhalb der Append-Transaktion**: Apply-Fehler rollen die Events zurück; der In-Memory-Zustand ist danach stale → `Refresh`.
- **Query-Ergebnisse auf ES-Modellen** sind Aggregat-verdrahtet (id/aggregate_seq werden mitselektiert): `Append` auf einem Query-Treffer funktioniert, kann aber bei nachhängender Projektion `ErrVersionConflict` liefern → `Refresh` oder `Load`.
- **Kein FK von CRUD-Tabellen auf ES-Read-Models** — deren Zeilen sind rebuildbare Artefakte (`RebuildProjection` löscht sie temporär); Referenz-Prüfung läuft engine-seitig gegen das Read-Model.
- **Event-Deklarationen sind nicht Teil der Schema-Checksum** — Event-Evolution läuft über Upcaster, nicht über `SchemaVersion` (kein Drift-Fehler bei neuer Event-Version).
- **CloudEvents-Typ-Präfix:** Default ist der Package-Pfad des Models mit `/`→`.` (überschreibbar via `orm.EventTypePrefix`).
- **Worker:** ein Goroutine-Loop pro Instanz (200-ms-Tick + Wake-Signal nach Append), verarbeitet Projektionen → Reaktoren → Snapshots; Fehler werden geschluckt und im nächsten Durchlauf erneut versucht (Checkpoints bleiben stehen — at-least-once). Lease-Koordination für Mehrinstanz-Betrieb kommt mit Phase 3/4.
- **Bewusste Auslassungen:** keine zstd-Kompression der Snapshots (keine neue Dependency; Feld ist BLOB, Kompression kann später dazu), keine Archiv-Nebentabellen (Phase 4), `ondelete`-Aktionen auf ES-Ziele lösen noch kein Lösch-Event aus (Phase 3), Worker-Fehler sind nicht beobachtbar (Observability-API kommt in Phase 5), `Watch` ohne Tenant im Context liefert einen sofort geschlossenen Kanal (Signatur hat keinen Fehlerkanal — fail-closed).

## Phase-1-Arbeitsplan (Reihenfolge)

| # | Schritt | Status |
|---|---|---|
| 1.1 | Grundgerüst: `orm.ID` (UUIDv7), Sentinel-Fehler, `WithTenant`/`WithGeo`, Options-Typen | ✅ |
| 1.2 | Model-Registry: Tag-Parsing, Validierung, kompilierter Mapping-Plan | ✅ |
| 1.3 | SQLite-Treiber (modernc.org/sqlite, WAL, FK an, txlock=immediate) + Dialekt-Interface | ✅ |
| 1.4 | Schema-Planner: DDL-Generierung, Systemtabellen, Bootstrap + additiver Diff, Checksum/Drift | ✅ |
| 1.5 | Tenant-Register: `db.Tenants()` (Create/Get/List/Archive), SingleTenant-Seed, Insert-Verifikations-Cache | ✅ |
| 1.6 | CRUD-Repository: Insert/InsertMany/Get/GetForUpdate/Update/Upsert/Delete + Feld-Constraints (required/enum/default/immutable/ref) | ✅ |
| 1.7 | Query-Builder: Cond-Baum, All/Iter/First/Count/Exists/UpdateSet/Delete | ✅ |
| 1.8 | Transaktionen (`db.Tx`), Topologie-Registrierung (Namensvalidierung, physisch kollabiert) | ✅ |
| 1.9 | Verhaltens-Testsuite Phase 1 (läuft später unverändert gegen PG/YB) | ✅ (17 Tests) |

## Erledigt

- Phase 0 komplett: Repo, CI (Test/Lint/Changelog-Release), Branch-Schutz, API.md, ROADMAP.md.
- **Phase 1 komplett** (Dateien: `id.go`, `errors.go`, `context.go`, `options.go`, `registry.go`, `driver.go`, `sqlite.go`, `db.go`, `schema.go`, `values.go`, `tenants.go`, `repo.go`, `query.go`, `migration.go`; Tests: `crud_test.go`):
  - Registry kompiliert Mapping-Pläne beim Registrieren (keine Reflection-Iteration im Hot Path über Tags), Referenzauflösung + Topo-Sortierung für FK-DDL bei `Migrate`.
  - `Migrate`: Systemtabellen (`ormpp_schema_state`, `ormpp_tenants`), SingleTenant-Seed, Checksum-Drift-Prüfung (`ErrSchemaDrift`), additiver Diff (neue Tabellen + `ALTER TABLE ADD COLUMN`).
  - Tenant-Regeln umgesetzt: Insert-Verifikation via Cache (`ErrUnknownTenant`, archiviert blockiert), write-once (nie im UPDATE), Scope in jeder Operation inkl. per-ID (fremder Tenant ⇒ `ErrNotFound`), `TenantFree`-Modelle.
  - Constraints: required/enum/default/immutable, `ref=` mit Engine-Prüfung + FK (`ondelete` restrict/cascade/setnull), Composite `Unique`/`Index` (Unique bezieht tenant_id ein).
  - Optimistisches Locking (`version`-Tag, `ErrVersionConflict` mit Existenz-Nachprüfung), `GetForUpdate` (`ErrRequiresTx`), `InsertMany` atomar/`Chunked`, `UpdateSet`/`Delete` mengenbasiert, `Iter()`-Streaming.
  - Topologie: `Topology`/`Region`/`Placement` registriert Regionen; Daten-Geo-Validierung (`ErrRegionNotActive`, `ErrNoGeo` bei Mehr-Regionen ohne Geo); physisch kollabiert alles auf die eine SQLite-Datei (Geo-Spalte speichert das deklarierte Daten-Geo; Reads filtern NIE auf Geo).

## Bewusste Phase-1-Auslassungen (kommen laut Roadmap später)

- Event Sourcing komplett (Phase 2): `orm.EventSourced()` wird registriert, aber `Migrate` lehnt mit klarer Fehlermeldung ab.
- Migrations-Zustandsmaschine/Dual-Write/Backfill (Phase 3): v1-`Migrate` kann Bootstrap + additiven Diff; `MigrationTo`-Schritte werden gespeichert, aber noch nicht ausgeführt; `FinalizeMigration` ⇒ Fehler „Phase 3".
- Postgres/Yugabyte-Treiber (Phase 4): `orm.Postgres`/`orm.Yugabyte` existieren, `Open` liefert klaren Fehler.
- `encrypted`-Tag, `Tenants().Export/Purge`, `SetGeo`, GeoGlobal/GeoFlexible-Mechanik, `MigrationStatus`/`Health`, Worker/Leases (`StartWorkers` = No-op): Stubs mit klarem Fehler bzw. dokumentiertem No-op.
- `ondelete=cascade/setnull`: DDL-seitig (FK) umgesetzt; engine-seitige Vor-Prüfung nur für `restrict`.

## Implementierungs-Notizen (Konventionen)

- Paket: alles im Root-Package `orm` (Import-Pfad laut API.md); Interna klein gehalten, später ggf. `internal/`.
- Tabellennamen: snake_case des Struct-Namens (`ProviderAccount` → `provider_account`); Spalten: snake_case des Feldnamens mit Akronym-Behandlung (`APIKey` → `api_key`).
- Implizite Spalten: `tenant_id TEXT NOT NULL` (außer TenantFree), `geo TEXT NOT NULL DEFAULT 'local'`. Reads filtern auf Tenant, **nie** auf Geo (Geo ist Platzierung, kein Sicherheits-Scope).
- Unique-Constraints beziehen `tenant_id` automatisch ein (Eindeutigkeit pro Tenant).
- Zeit als TEXT (RFC3339Nano), IDs als TEXT (UUID-String), bool als INTEGER, `json`-Tag als TEXT.
- UUIDv7 selbst implementiert (crypto/rand) — keine Abhängigkeit auf uuid-Lib; einzige externe Dependency: modernc.org/sqlite (CGO-frei).
- SQLite-Pool: MaxOpenConns=1 (Serialisierung; getrennter Lese-Pool ist eine spätere Optimierung).
- `Register` sammelt Fehler; sie schlagen gesammelt bei `db.Migrate` fehl (Register hat laut API keinen Fehler-Rückgabewert).
- Update mit 0 betroffenen Zeilen: Existenz-Nachprüfung entscheidet `ErrVersionConflict` vs. `ErrNotFound`.

## Probleme / offene Fragen

- (leer)
