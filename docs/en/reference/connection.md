---
title: Instantiation & connection
description: Open, options, Close.
sidebar:
  order: 1
---

## Open

```go
func Open(driver Driver, opts ...OpenOption) (*DB, error)
```

Builds the connection pool, validates the instance geo against the topology,
registers the instance in the instance registry (with heartbeat) and loads the
type dictionary. `Open` performs **no** schema changes — that is `Migrate`'s
job.

**Drivers:** `orm.SQLite(path)` · `orm.Postgres(dsn)` · `orm.Yugabyte(dsn)`.

**Options:**

| Option | Meaning | Default |
|---|---|---|
| `orm.InstanceGeo(geo string)` | Region this process runs in. | `"local"` |
| `orm.MigrationRole(role)` | `MigrationNone` \| `MigrationWorker` | `MigrationNone` |
| `orm.AppVersion(v string)` | Application version (ops view). | `""` |
| `orm.DefaultSnapshotEvery(n int)` | Global snapshot default for ES models. | `100` |
| `orm.EventTypePrefix(p string)` | Prefix for CloudEvents types. | module-path based |
| `orm.Encryption(p KeyProvider)` | Enable field encryption. | — |

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

Deregisters the instance (leases released, workers stopped) and closes the pool.
