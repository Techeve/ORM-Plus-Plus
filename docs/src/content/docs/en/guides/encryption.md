---
title: Field encryption
description: The encrypted tag, key rotation, limits.
sidebar:
  order: 6
---

Fields with the `encrypted` tag are encrypted by the engine before writing
(**AES-256-GCM**) and transparently decrypted on read — identical on all
backends, the DB only ever sees ciphertext (`BYTEA`/`BLOB`).

```go
type ProviderAccount struct {
	ID     orm.ID `orm:"pk"`
	Name   string `orm:"index"`
	APIKey string `orm:"encrypted,required"`
}

db, err := orm.Open(orm.Postgres(dsn),
	orm.Encryption(orm.StaticKey(keyFromKMS)),   // required as soon as a model uses `encrypted`
)
```

**Rules:**

- `orm.Encryption(provider)` is an `Open` option; without it `Migrate` fails
  for models with `encrypted` fields. `orm.StaticKey([]byte)` (32 bytes) is the
  simplest provider; the `orm.KeyProvider` interface (current key + lookup by
  key ID) is **rotation-ready** from day one — every ciphertext carries the ID
  of the key used, rotation happens lazily on the next write.
- `encrypted` applies to `string` and `[]byte` fields (also pointers) and is
  not combinable with `pk`/`index`/`unique`/`json`/`version`/`default`/`enum`/`ref`.
- Encrypted fields are **not indexable, filterable or sortable** — the DB
  cannot meaningfully compare ciphertext.
- **v1 scope:** `encrypted` works on CRUD models. On event-sourced models it is
  currently rejected at `Migrate` and follows in a later version.
