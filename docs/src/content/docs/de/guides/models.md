---
title: Modelle deklarieren
description: Structs registrieren, Tags, Scope- und Geo-Optionen, Referenzen.
sidebar:
  order: 1
---

Ein Modell ist ein Go-Struct mit Tags. Die Registrierung passiert beim Start,
vor `Migrate`. Die Registry validiert das Struct und kompiliert den
Mapping-Plan einmalig (keine Reflection im Hot Path).

```go
func Register[T any](db *DB, mode ModelMode, opts ...ModelOption)

orm.CRUD()          // klassische Persistenz
orm.EventSourced()  // Event Sourcing (Struct bettet orm.Aggregate ein)
```

## Struct-Tags

```go
type ProviderAccount struct {
	ID        orm.ID    `orm:"pk"`
	Name      string    `orm:"index"`
	Email     string    `orm:"unique"`
	Labels    []string  `orm:"json"`
	Version   int64     `orm:"version"`      // optimistisches Locking
	CreatedAt time.Time `orm:"autocreate"`
	UpdatedAt time.Time `orm:"autoupdate"`
	Notes     string    `orm:"deprecated"`   // markiert, fällt bei Finalisierung
}
```

| Tag | Bedeutung |
|---|---|
| `pk` | Primärschlüssel (genau einer; `orm.ID` ist UUIDv7) |
| `index`, `unique` | Sekundär-/Unique-Index |
| `json` | Verschachtelte Werte als JSON(B)-Spalte |
| `version` | Spalte für optimistisches Locking bei `Update` |
| `autocreate`, `autoupdate` | Zeitstempel-Pflege durch die Engine |
| `ref=Model[,ondelete=…]` | Referenz auf ein anderes Model |
| `enum=a\|b\|c` | Wertemenge für String-Felder (CHECK nativ, Engine-Prüfung überall) |
| `default=…` | Default, wenn das Feld beim Insert den Zero-Value hat |
| `encrypted` | Feld wird verschlüsselt gespeichert |
| `immutable` | Write-once: nach dem Insert unveränderlich |
| `required` | Muss beim Insert gesetzt sein (Zero-Value ⇒ `ErrRequiredField`) |
| `deprecated` | Feld ist zur Entfernung markiert (Expand/Contract) |
| `-` | Feld wird nicht persistiert |

**NULL-Fähigkeit** ergibt sich aus dem Go-Typ: Nicht-Pointer-Felder sind
`NOT NULL`, Pointer-Felder (`*string`, `*time.Time`) erlauben `NULL`.
`tenant_id` und die Geo-Spalten deklariert man **nicht** — sie sind implizit
in jeder Tabelle vorhanden und werden über den Context gesteuert.

## Scope- und Geo-Optionen

```go
orm.TenantFree()   // Model ohne Tenant-Spalte/-Filter (technische Tabellen)

orm.GeoScoped()    // jeder Datensatz in genau einer Region (Default)
orm.GeoGlobal()    // Model in allen Regionen (Stammdaten: Tenants, Nutzer, Pläne)
orm.GeoFlexible()  // pro Datensatz wählbar: Heimatregion + lesende Replikate
```

## Composite-Constraints

```go
orm.Register[Record](db, orm.CRUD(),
	orm.Unique("ProjectID", "Name"),   // Unique über mehrere Spalten
	orm.Index("Status", "CreatedAt"),  // zusammengesetzter Sekundärindex
)
```

Tenant- und Geo-Spalten werden automatisch in Unique-Constraints einbezogen —
Eindeutigkeit gilt pro Tenant.

## Referenzen

```go
type Document struct {
	ID        orm.ID  `orm:"pk"`
	Title     string  `orm:"required"`
	CreatedBy orm.ID  `orm:"ref=User,immutable,required"`  // Pflicht, unveränderlich
	ProjectID orm.ID  `orm:"ref=Project,ondelete=cascade"` // stirbt mit dem Projekt
	ReviewerID *orm.ID `orm:"ref=User"`                    // optional (Pointer ⇒ NULL)
}
```

Referenzen dürfen nur auf Datensätze **desselben Tenants** zeigen (Ausnahme:
Ziel ist `TenantFree` oder `GeoGlobal`-Stammdaten). `ondelete`:
`restrict` (Default) · `cascade` · `setnull` (nur Pointer-Felder).

## Feld-Verschlüsselung

Siehe [Verschlüsselung](/de/guides/encryption/).
