# TASK.md — Arbeitsstand der Implementierung

Fortschritts-Log für die Umsetzung nach [ROADMAP.md](ROADMAP.md) (Phasen) und [API.md](API.md) (Vertrag).
Nach jedem abgeschlossenen Schritt wird diese Datei aktualisiert und committet — bei Abbruch hier weiterlesen statt das Projekt neu zu scannen.

## Aktueller Schritt

**Phase 5 in Arbeit** (v1.0-Härtung, ein Commit je Baustein):

| # | Baustein | Status |
|---|---|---|
| 5.1 | Feld-Verschlüsselung: `encrypted`-Tag (AES-256-GCM, BLOB), `orm.Encryption`/`KeyProvider`/`StaticKey`, Key-ID im Ciphertext (lazy Rotation), nicht filter-/sortierbar, `UpdateSet` engine-seitig; v1: CRUD-Modelle (ES abgelehnt, folgt) | ✅ |
| 5.2 | `Tenants().Export` — DSGVO-Auszug als JSON Lines (alle Modelle, Events, Snapshots, Archiv; entschlüsselt) | ⏳ |
| 5.3 | `Tenants().Purge` — physisches Löschen über alle Tabellen, auditiert | — |
| 5.4 | `MigrationStatus`/`Health` — Observability | — |
| 5.5 | `SetGeo` (GeoFlexible-Metadaten) + Abschluss-Doku | — |

## Phase-4b-Arbeitsplan (Reihenfolge)

| # | Schritt | Status |
|---|---|---|
| 4b.1 | Native `PARTITION BY LIST (geo)` der Event-Tabellen auf PG/YB: Partition je deklarierter Region + DEFAULT-Partition, additiv bei neuen Regionen; PK enthält dort `geo` (Partitionierungs-Anforderung); SQLite kollabiert unverändert | ✅ |
| 4b.2 | Aggregat-Geo-Pinning: Daten-Geo klebt ab dem ersten Event am Aggregat — Folge-Appends schreiben in die Heimat-Partition, unabhängig vom Context-Geo | ✅ |
| 4b.3 | Archivierung: `<t>_events_archive`-Nebentabelle, Archiv-Worker (Grenze: zweitjüngster Snapshot UND Projektions-Checkpoint, batchweise, idempotent) | ✅ |
| 4b.4 | Transparente Union-Reads (Hot + Archiv) für History, AtVersion/AtTime unterhalb der Snapshots, Stream/Watch-Replay, Reaktoren/RebuildView, eventGeos | ✅ |
| 4b.5 | Worker-Leases: Projektion+Snapshot+Archiv je Model, je View, Dual-Write-Drain — clusterweit eine Instanz, sticky, TTL-Failover, sofortige Freigabe bei Close | ✅ |
| 4b.6 | `ormpp_geo_regions`: Topologie-Register (Name, Status, Placement) bei Migrate persistiert | ✅ |
| 4b.7 | Verhaltenstests: Archiv-Transparenz, Geo-Pinning, Lease-Koordination (backend-neutral) | ✅ |

## Phase-4b-Notizen (Entscheidungen & bewusste Grenzen)

- **Schema-Änderung (dokumentiert wie abgesprochen):** Auf PG/YB ist der Event-PK `(aggregate_id, aggregate_seq, geo)` — Partitionstabellen verlangen den Partition-Key im PK. Die globale Eindeutigkeit von `(aggregate_id, aggregate_seq)` sichert das Geo-Pinning (ein Aggregat lebt in genau einer Region); der theoretische Rest-Fall — zwei Prozesse erzeugen zeitgleich dasselbe frische Aggregat mit unterschiedlichem Geo — ist über Partitionsgrenzen nicht constraint-durchsetzbar (bekannte Partitionierungs-Grenze, ROADMAP-Risiko notiert).
- **API-Verhalten präzisiert (API.md §7.2):** `WithGeo` bestimmt die Heimatregion bei der Entstehung; danach klebt sie am Aggregat.
- **Archiv als Nebentabelle auf allen Backends** (verhaltensgleich, SQL-abfragbar). Die ROADMAP-Optimierung „Seq-Range-Partition abhängen statt kopieren" (PG/YB) setzt Sub-Partitionierung RANGE(seq) voraus — als spätere Optimierung notiert, Semantik ändert sich dadurch nicht.
- **Archiv-Grenze doppelt gesichert:** nur Events ≤ zweitjüngster Snapshot UND ≤ Projektions-Checkpoint des jeweiligen Geos. Read-Model und Normal-Load lesen dadurch nie ins Archiv; Reaktoren/Rebuilds/Zeitreisen lesen Union.
- **Worker-Leases sticky:** Halter erneuert pro Durchlauf (TTL 15 s); `Close` löscht die Leases der Instanz ⇒ sofortiger Failover. Regionale Zuordnung der Leases („Worker derselben Region") folgt mit der Geo-Replikation (Phase 5/Stufe 2).
- **YugabyteDB Smart Driver bewusst nicht verwendet:** `github.com/yugabyte/pgx` (Fork) bringt Cluster-aware Connection-Load-Balancing (`load_balance`, `topology_keys`), keine schnellere Query-Ausführung. Für v1 reicht jackc/pgx + Multi-Host-DSN/LB; der Fork hinkt Upstream hinterher. Da `orm.Yugabyte` den Treiber kapselt, ist ein späterer Wechsel ein interner Drop-in ohne API-Änderung. Entscheidung dokumentiert, Re-Evaluation bei Multi-Node-Betrieb (Phase 5/Stufe 2).
- **`ormpp_geo_regions`-Lebenszyklus** (bootstrapping → active → draining → removed) ist angelegt, aber noch ohne Zustandsmaschine — deklarierte Regionen sind sofort `active`; Nachreplikation/Draining kommt mit GeoGlobal/GeoFlexible (Phase 5/Stufe 2).

## Phase-4-Arbeitsplan (Reihenfolge)

| # | Schritt | Status |
|---|---|---|
| 4.1 | pgx-Treiber (`orm.Postgres`, `orm.Yugabyte` über PG-Wire), Ping bei Open, Pool-Defaults | ✅ |
| 4.2 | Dialekt-Erweiterung: `rebind` (?→$n), `columnType`/`zeroLiteral` (BIGINT/JSONB/BYTEA/DOUBLE), `autoPK`, `limitAll`, `forUpdate`, Trigger-DDL | ✅ |
| 4.3 | Alle Engine-Statements durch den Rebind-Wrapper (`dialq`) geroutet — ein Codepfad, zwei Platzhalter-Stile | ✅ |
| 4.4 | `GetForUpdate` nativ (`SELECT … FOR UPDATE` auf PG/YB; SQLite emuliert weiter) | ✅ |
| 4.5 | Append unter echter Nebenläufigkeit: PK-Verletzung → `ErrVersionConflict`, Geo-Seq-Kollision → transparenter Retry | ✅ |
| 4.6 | Dual-Write-Trigger auf PG/YB als plpgsql-Funktion + Zeilen-Trigger | ✅ |
| 4.7 | Test-Matrix: `ORMPP_TEST_BACKEND`/`ORMPP_TEST_DSN`, Schema-pro-Test-Isolation (search_path), docker-compose.yml, CI-Jobs test-postgres/test-yugabyte | ✅ |

## Phase-4-Notizen (Entscheidungen & bewusste Grenzen)

- **Neue Dependency `github.com/jackc/pgx/v5`** (stdlib-Modus) — laut ROADMAP Phase 4 vorgesehen.
- **Speicherformate bleiben backend-neutral**: IDs/Zeit als TEXT, bool als BIGINT (0/1), `json`-Tag als JSONB auf PG/YB (TEXT auf SQLite) — Verhalten identisch, decodeField versteht beides.
- **Platzhalter-Strategie:** Engine schreibt überall `?`; `dialq` rebindet pro Dialekt ($n auf PG/YB). String-Literale bleiben unangetastet.
- **Offen (Phase 4b):** native `PARTITION BY LIST (geo)` der Event-Tabellen (erfordert Partition-Key im PK — Schemaänderung), Archiv-Partitionen/-Nebentabellen, `ormpp_geo_regions`-Lebenszyklus, Leases für Projektions-Worker über Instanzen hinweg (`FOR UPDATE SKIP LOCKED`-Optimierung), Typ-Wörterbuch-Registrierung unter paralleler Migration mehrerer Instanzen.
- **Poison-Rows/Dead-Letter und `MigrationStatus`/`Health`** weiterhin Phase 5.

## Phase-3-Arbeitsplan (Reihenfolge)

| # | Schritt | Status |
|---|---|---|
| 3.1 | Instanzregister `ormpp_instances`: Registrierung bei Migrate, Heartbeat im Worker, Abmeldung bei Close, TTL-Lebendprüfung | ✅ |
| 3.2 | Leases mit Fencing-Token (`ormpp_leases`): acquire/renew/release, Ablauf-Übernahme; Migrations-Leader läuft darüber | ✅ |
| 3.3 | Zustandsmaschine in `ormpp_schema_state` (current/target/phase) + Audit `ormpp_schema_history`: idle → expanding → backfill → dual-write → finalizing → idle | ✅ |
| 3.4 | `MigrationTo`-Schritte: `ReplaceModel[Old,New]` (V-Suffix-Konvention, Scratch-Plan fürs Alt-Struct, Identitäts-/Scope-Erhalt) und `BatchScript` (Checkpoint-API auf `Batch`) | ✅ |
| 3.5 | Backfill: batchweise, checkpointed (`ormpp_migration_progress`), drosselbar (`MigrationPlan`: BatchSize, `RowsPerSecond`), wiederaufnehmbar nach Absturz | ✅ |
| 3.6 | Dual-Write-Nachlauf: Trigger auf Alt-Tabellen → `ormpp_dualwrite_queue`, Drain im Worker (at-least-once); Alt-Instanz-Schreibvorgänge landen laufend in der neuen Struktur | ✅ |
| 3.7 | `FinalizeMigration`: verweigert bei lebender Alt-Instanz (`ErrMigrationPending`), leert die Queue, droppt Alt-Tabellen und aus dem Struct entfernte deprecated-Spalten, Zustand → idle | ✅ |
| 3.8 | `ormpp_deprecated`: Markierung bei expanding; Spalten fallen erst, wenn das Feld aus dem Struct entfernt ist (nächste Contract-Runde) | ✅ |
| 3.9 | Verhaltens-Testsuite Phase 3 (`migration_test.go`, 5 Tests) — inkl. Meilenstein: zwei App-Generationen gleichzeitig gegen dieselbe DB durch eine komplette Expand/Contract-Migration | ✅ |

## Phase-3-Notizen (Entscheidungen & bewusste Grenzen)

- **Neue Dateien:** `instances.go` (Instanzregister + Leases), `migration.go` (Deklarations-API: `MigrationPlan`, `ReplaceModel`, `BatchScript`, Fortschritt), `migrator.go` (Zustandsmaschine, Backfill, Trigger, Drain, Finalize); Tests in `migration_test.go`.
- **`Migrate(ctx, plan ...MigrationPlan)`** — optionale Plan-Parameter laut API.md §8.2; `WorkersPerGeo` ist auf SQLite informativ (eine Region, ein Worker — gleiche Mechanik).
- **V-Suffix-Konvention:** `ReplaceModel[CustomerV1, Account]` liest die Tabelle des früheren Models `Customer` — Suffix `V<n>` wird bei der Tabellen-Ableitung gestrichen.
- **Dual-Write ist v1 einseitig:** Alt-Instanz → neue Struktur (Trigger + Queue + Drain). Die Rückrichtung (neue Instanz schreibt zusätzlich alt) bräuchte eine Rück-Transformation; als optionale `ReplaceModel`-Erweiterung notiert (sonst Echo-/Konfliktproblematik). API.md §8.2 entsprechend präzisiert.
- **Trigger-DDL ist SQLite-Syntax** (`strftime`); wandert in Phase 4 hinter das Dialekt-Interface (PG: Trigger-Funktion, YB: eingeschränkte Trigger → ggf. Outbox-Fallback).
- **Instanzregistrierung bei `Migrate`** (nicht bei `Open`): die deklarierte SchemaVersion steht erst nach `SchemaVersion(db, n)` fest. TTL 60 s, Heartbeat alle 5 s im Worker.
- **Kein `target_checksum`:** Ändert jemand Modelle erneut, während dual-write läuft (gleiches Ziel, andere Checksum), wird das nicht erkannt — erst wieder nach Finalize. Notiert für Phase 5-Härtung.
- **Backfill-Reihenfolge = Deklarationsreihenfolge** der Schritte; bei Referenzen zwischen ersetzten Modellen Schritte in Abhängigkeitsreihenfolge deklarieren.
- **Poison-Rows in der Nachlauf-Queue** blockieren den Drain (Fehler → Retry im nächsten Durchlauf); Dead-Letter-Behandlung kommt mit der Observability (Phase 5).
- **`ReplaceModel` setzt tenant-gebundene CRUD-Modelle voraus** (Alt- und Neu-Seite gleich gebunden); TenantFree-Umbauten laufen über `BatchScript`.

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
