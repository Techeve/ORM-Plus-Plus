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

Flags: `-scale` (Operationen pro Messreihe), `-out report.json`, `-bench bench.txt`.
DSNs alternativ über `ORMPP_BENCH_POSTGRES` / `ORMPP_BENCH_YUGABYTE`.

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
