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

## Beispiel: Schlüsselrotation

`StaticKey` reicht für den Einstieg, ist aber ein einzelner Schlüssel ohne
Rotation. Für den Produktivbetrieb implementiert man `orm.KeyProvider` selbst
— zum Beispiel gegen ein KMS, das mehrere Schlüsselversionen kennt:

```go
type kmsKeys struct{ kms *kms.Client }

// CurrentKey liefert den Schlüssel, mit dem NEU geschrieben wird.
func (k kmsKeys) CurrentKey() (id string, key []byte, err error) {
	return k.kms.CurrentKeyID(), k.kms.Fetch(k.kms.CurrentKeyID()), nil
}

// Key löst eine im Ciphertext gespeicherte Key-ID beim LESEN auf.
func (k kmsKeys) Key(id string) ([]byte, error) {
	return k.kms.Fetch(id), nil
}

db, err := orm.Open(orm.Postgres(dsn),
	orm.Encryption(kmsKeys{kms: kmsClient}),
)
```

Rotation braucht keinen Migrationsschritt: sobald das KMS eine neue
`CurrentKeyID()` liefert, verschlüsseln alle folgenden Schreibvorgänge damit.
Alte Zeilen bleiben lesbar, weil ihr Ciphertext die ursprüngliche Key-ID trägt
und `Key(id)` sie auflöst — Alt- und Neuschlüssel koexistieren, bis ein
Batch-Migrationsprozess (siehe [Migration](/guides/migration/)) die
Bestandszeilen bei Gelegenheit neu verschlüsselt.
