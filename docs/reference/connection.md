---
title: Instanziierung & Verbindung
description: Open, Optionen, Close.
sidebar:
  order: 1
---

## Open

```go
func Open(driver Driver, opts ...OpenOption) (*DB, error)
```

Baut den Verbindungspool auf, validiert das Instanz-Geo gegen die Topologie,
registriert die Instanz im Instanzregister (mit Heartbeat) und lädt das
Typ-Wörterbuch. `Open` führt **keine** Schema-Änderungen aus — das macht
`Migrate`.

**Treiber:** `orm.SQLite(path)` · `orm.Postgres(dsn)` · `orm.Yugabyte(dsn)`.

**Optionen:**

| Option | Bedeutung | Default |
|---|---|---|
| `orm.InstanceGeo(geo string)` | Region, in der dieser Prozess läuft. | `"local"` |
| `orm.MigrationRole(role)` | `MigrationNone` \| `MigrationWorker` | `MigrationNone` |
| `orm.AppVersion(v string)` | Version der Anwendung (Betriebs-Sicht). | `""` |
| `orm.DefaultSnapshotEvery(n int)` | Globaler Snapshot-Default für ES-Modelle. | `100` |
| `orm.EventTypePrefix(p string)` | Präfix für CloudEvents-Typen. | modulpfad-basiert |
| `orm.Encryption(p KeyProvider)` | Feld-Verschlüsselung aktivieren. | — |

```go
db, err := orm.Open(orm.Yugabyte(dsnUSEast),
	orm.InstanceGeo("us-east"),
	orm.MigrationRole(orm.MigrationWorker),
	orm.AppVersion("1.4.2"),
)
```

## Close

```go
func (db *DB) Close() error
```

Meldet die Instanz im Register ab (Leases werden freigegeben, Worker gestoppt)
und schließt den Pool.
