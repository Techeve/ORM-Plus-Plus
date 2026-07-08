# ORM++ Performance-Benchmark

Fährt **dasselbe Szenario gegen SQLite, PostgreSQL und YugabyteDB** und misst
Latenzen (p50/p95/p99/max) und Durchsatz der wichtigsten ORM++-Operationen.

```sh
# Nur SQLite (läuft immer, eingebettet):
go run ./examples/bench

# Alle drei Backends (Container: docker compose up -d im Repo-Root):
go run ./examples/bench \
    -postgres "postgres://orm:orm@localhost:5433/orm" \
    -yugabyte "postgres://yugabyte@localhost:5434/yugabyte" \
    -scale 300
```

Flags: `-scale` (Operationen pro Messreihe), `-out report.json`, `-bench bench.txt`,
`-roh=false` (Roh-SQL-Baseline abschalten).
DSNs alternativ über `ORMPP_BENCH_POSTGRES` / `ORMPP_BENCH_YUGABYTE`.

## Roh-SQL-Baseline: der Preis der Abstraktion

Zu jedem Backend läuft zusätzlich ein **`<backend>/roh`-Lauf**: dieselben
Statements, die ORM++ erzeugt — aber handgeschrieben, direkt über
`database/sql` + Treiber (gleiche Pragmas, gleiche Pool-Einstellungen,
frischer Speicher, Prepared Statements, vorserialisierte Werte). Das ist der
Idealfall ohne jede Abstraktion: kein Reflection, kein Query-Bau, keine
Validierung. Die Tabelle **„ORM++ vs. Roh-SQL"** weist den Overhead pro
Messreihe aus (auch in `report.json` unter `overhead`).

Erkenntnisse des Referenzlaufs (lokal, scale=200):

- **Auf PG/YB verschwindet der ORM-Overhead fast vollständig im
  Netz-/Konsens-Roundtrip** (±10–30 %, teils Messrauschen) — die Datenbank
  dominiert, nicht die Library.
- **Auf SQLite (in-process, nichts versteckt den CPU-Anteil)** zeigen sich
  die echten Reserven: `insert_many_chunked` ~+100 % (Query-String wird pro
  Zeile neu gebaut, kein Prepared-Statement-Reuse) und `es/append_1_event`
  ~+120 % (wobei die Engine hier auch echte Mehrarbeit leistet: Apply,
  Watch-Publish, Worker-Wake).
- Werte um ±20 % sind Rauschen (getrennte Dateien/Schemata, WAL-Varianz) —
  für belastbare Deltas mehrere Läufe mit `benchstat` mitteln.

## Das Szenario

12 Messreihen, die die Roadmap-Lasttest-Punkte abdecken (Append-Durchsatz,
Projektions-Lag, CRUD-Pfade):

| Messreihe | Was gemessen wird |
|---|---|
| `crud/insert_einzeln` | Grundlatenz eines Einzel-Inserts (inkl. Tenant-Verifikation, Constraints) |
| `crud/insert_many_chunked` | Bulk-Durchsatz — die Einfüge-Strategie wählt der Dialekt |
| `crud/get_per_id` | der heißeste Lesepfad (PK + Tenant-Filter) |
| `crud/query_index_limit20` | Query-Builder mit Index, Sortierung, Limit |
| `crud/update_optimistisch` | Read-Modify-Write mit `version`-Spalte |
| `crud/updateset_mengen` | 1 Statement über viele Zeilen (Zeilen/s) |
| `crud/tx_getforupdate` | Transaktion mit Zeilensperre |
| `es/append_1_event` | Event-Append-Durchsatz (atomar, optimistisch, viele Aggregate) |
| `es/append_5_events` | Batch-Append (5 Events atomar) |
| `es/projektions_aufholzeit` | wie schnell das Read-Model nach einem Burst aufholt |
| `es/load_aggregat` | Aggregat laden (Snapshot + Restevents falten) |
| `es/query_readmodel` | Query gegen das ES-Read-Model |

Gemessen wird bewusst **single-threaded**: So bleiben die Latenzen vergleichbar
und der architektonische Unterschied der Backends wird nicht durch
Parallelität verwischt — SQLite arbeitet in-process (µs), PostgreSQL kostet
einen Netz-Roundtrip (ms), YugabyteDB zahlt pro Schreibzugriff Raft-Konsens
(einige ms; im Single-Node-Docker ohne echten Cluster-Vorteil).

## Berichte

- **Konsole** — Tabelle pro Backend + Durchsatz-Vergleichsmatrix.
- **`report.json`** — strukturiert (Zeitstempel, Plattform, alle Kennzahlen);
  zum Archivieren und für eigene Auswertungen.
- **`bench.txt`** — das **Go-Benchmark-Format**, der etablierte Standard für
  Performance-Daten im Go-Umfeld. Läufe vergleichen (z. B. vor/nach einer
  Optimierung oder SQLite vs. PG):

```sh
go install golang.org/x/perf/cmd/benchstat@latest
go run ./examples/bench -bench alt.txt          # Basislinie
# … Änderung einbauen …
go run ./examples/bench -bench neu.txt
benchstat alt.txt neu.txt                        # Δ mit Signifikanz
```

## Interpretationshilfe

- Absolute Zahlen hängen an Hardware, Docker-Overhead und (bei YB) an der
  Cluster-Größe — **vergleichbar sind Läufe auf derselben Maschine**.
- `updateset_mengen` vs. `update_optimistisch` zeigt, warum mengenbasierte
  Operationen existieren: ein Statement statt N Roundtrips.
- `projektions_aufholzeit` ist der Lag-Kennwert für Read-your-writes-Budgets
  (`orm.WaitFor`).
- Auf YugabyteDB sind Einzel-Statement-Latenzen konsensbedingt hoch; der
  Ausgleich kommt aus horizontaler Skalierung und Geo-Verteilung — genau
  dafür ist das Backend gedacht.
