---
title: Feld-Verschlüsselung
description: encrypted-Tag, Schlüsselrotation, Grenzen.
sidebar:
  order: 6
---

Felder mit dem Tag `encrypted` werden von der Engine vor dem Schreiben
verschlüsselt (**AES-256-GCM**) und beim Lesen transparent entschlüsselt — auf
allen Backends identisch, die DB sieht nur Ciphertext (`BYTEA`/`BLOB`).

```go
type ProviderAccount struct {
	ID     orm.ID `orm:"pk"`
	Name   string `orm:"index"`
	APIKey string `orm:"encrypted,required"`
}

db, err := orm.Open(orm.Postgres(dsn),
	orm.Encryption(orm.StaticKey(keyFromKMS)),   // Pflicht, sobald ein Model `encrypted` nutzt
)
```

**Regeln:**

- `orm.Encryption(provider)` ist eine `Open`-Option; ohne sie schlägt `Migrate`
  bei Modellen mit `encrypted`-Feldern fehl. `orm.StaticKey([]byte)` (32 Bytes)
  ist der einfachste Provider; das `orm.KeyProvider`-Interface (aktueller
  Schlüssel + Lookup per Key-ID) ist von Tag 1 **rotationsfähig** — jeder
  Ciphertext trägt die ID des benutzten Schlüssels, Rotation erfolgt lazy beim
  nächsten Schreiben.
- `encrypted` gilt für `string`- und `[]byte`-Felder (auch Pointer) und ist
  nicht kombinierbar mit `pk`/`index`/`unique`/`json`/`version`/`default`/`enum`/`ref`.
- Verschlüsselte Felder sind **nicht indizierbar, nicht filterbar und nicht
  sortierbar** — die DB kann Ciphertext nicht sinnvoll vergleichen.
- **v1-Umfang:** `encrypted` wirkt auf CRUD-Modellen. Auf EventSourced-Modellen
  wird es aktuell bei `Migrate` abgelehnt und folgt in einer späteren Version.
