# ORM++ Beispielanwendung

Eine Mini-DNS-Verwaltung, die **jede Fähigkeit von ORM++ einmal zeigt** — mit
ausführlichen Kommentaren, die erklären, *warum* etwas so gemacht wird.

```sh
go run ./examples/demo
```

Die Demo simuliert **zwei App-Generationen gegen dieselbe Datenbank-Datei** —
so läuft ORM++ im echten Leben (Rollout neuer App-Versionen):

## Generation 1 — Betrieb (SchemaVersion 1)

| Thema | Was gezeigt wird |
|---|---|
| Modelle & Tags | `pk`, `index`, `unique`, `enum`+`default`, `required`, `immutable`, `ref`+`ondelete`, `json`, `version`, `autocreate`/`autoupdate`, `deprecated`, `encrypted`, `-` |
| Model-Varianten | CRUD, `TenantFree` (Konfigtabelle ohne Tenant), `GeoFlexible` (Heimat + Replikate pro Datensatz), EventSourced |
| Tenants | eingebautes Register, `WithTenant` (fail-closed: ohne Tenant ⇒ `ErrNoTenant`), Isolation |
| CRUD | Insert/Get/Update, optimistisches Locking (`ErrVersionConflict`), Referenzprüfung, `InsertMany` mit `Chunked` |
| Query-Builder | `Like`/`In`/`And`, `OrderBy`/`Limit`, `Count`, `Iter` (Cursor-Streaming), mengenbasiertes `UpdateSet`/`Delete` |
| Transaktionen | `db.Tx` über mehrere Modelle, `GetForUpdate` (Zeilensperre) |
| Event Sourcing | `orm.New`/`Append` (atomar, optimistisch), `Load` mit `WaitFor` (Read-your-writes), Query aufs Read-Model, `History` (CloudEvents), Zeitreisen (`AtVersion`/`AtTime`), `Watch` (Live-Strom) |
| OnEvent-Reaktor | abgeleitete View transaktional bauen — inkl. der zwei Pflicht-Muster: Idempotenz (Event-ID-Merker) und Tenant/Geo **aus dem Event** übernehmen |
| Geo | `Topology`, `WithGeo` (Daten-Geo, validiert), `SetGeo`/`ReplicateTo`/`ReplicateAll` |
| Verschlüsselung | `encrypted`-Roundtrip, „nicht filterbar"-Garantie |
| DSGVO | `Tenants().Export` (JSON Lines), `Archive` + `Purge` (auditiert) |
| Observability | `db.Health` (Instanzen, Projektions-Lag, Regionen) |

## Generation 2 — Schema-Evolution (SchemaVersion 2→3)

| Thema | Was gezeigt wird |
|---|---|
| Drift-Schutz | Modelländerung erzwingt `SchemaVersion`-Erhöhung |
| `ReplaceModel` | neues Model ersetzt altes (V-Suffix-Konvention: `KontaktV1` liest Tabelle `kontakt`), Backfill transformiert Zeile für Zeile, Identität/Tenant/Geo bleiben |
| `BatchScript` | freies Migrationsskript mit Checkpoint (wiederaufnehmbar) |
| Zustandsmaschine | `Migrate` → expanding → backfill → **dual-write**; `MigrationStatus` zeigt Phase + Fortschritt |
| `FinalizeMigration` | expliziter Abschluss: Alt-Tabelle fällt (verweigert, solange alte Instanzen leben) |
| Event-Upcaster | `record_added` v1→v2: alte Events bleiben unveränderlich, der Upcaster hebt sie **beim Lesen** — fehlt er, schlägt `Migrate` fehl |

Die Demo läuft auf SQLite. **Derselbe Code läuft unverändert gegen
`orm.Postgres(dsn)` oder `orm.Yugabyte(dsn)`** — Verhaltensgleichheit ist das
Grundprinzip: App-Code verzweigt nie nach dem Backend und schreibt kein SQL.
