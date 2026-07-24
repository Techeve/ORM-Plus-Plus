---
title: Multi-Tenancy
description: Tenant im Context, das eingebaute Tenant-Register, DSGVO-Export/Purge.
sidebar:
  order: 5
---

Tenant und Geo sind **Pflicht-Scope jeder Datenoperation** und hängen am
`context.Context` — nie an Funktionssignaturen.

```go
ctx = orm.WithTenant(ctx, tenantID)
ctx = orm.WithGeo(ctx, "eu-central")
```

- Fehlender Tenant ⇒ `orm.ErrNoTenant` (fail-closed). Nichts läuft
  versehentlich tenant-los. Single-Tenant-Apps setzen einmal beim Start einen
  konstanten Tenant (`orm.SingleTenant`).
- Jede Operation — auch `Get`/`Update`/`Delete` per ID — filtert auf den
  Context-Tenant. Ein fremder Tenant per ID verhält sich wie `ErrNotFound`.

## Tenant-Register (eingebaut)

Tenants kommen nicht aus der App — ORM++ bringt sie als System-Model mit
(`GeoGlobal`, damit jede Region lokal validieren kann):

```go
tenants := db.Tenants()

t, err := tenants.Create(ctx, orm.TenantInfo{Name: "ACME GmbH"})   // ID: UUIDv7
t, err  = tenants.Get(ctx, id)
list, err := tenants.List(ctx)
err     = tenants.Archive(ctx, id)   // blockiert neue Schreibvorgänge, Bestand bleibt lesbar
```

**Tenant-Regeln (nicht abschaltbar):**

1. **Insert-Verifikation:** Jeder `Insert`/`Append` prüft die Context-Tenant-ID
   gegen das Register — unbekannt oder archiviert ⇒ `orm.ErrUnknownTenant`.
2. **Write-once:** `tenant_id` wird beim Insert gesetzt und ist danach
   unveränderlich. Es gibt keine API, einen Datensatz umzuhängen.
3. **Scope in jeder Operation** — auch per Primärschlüssel.

## DSGVO: Export & Purge

```go
// Vollständiger Datenauszug eines Tenants (JSON Lines, alle Modelle
// inkl. Events, Snapshots und Archiv):
err = tenants.Export(ctx, id, w)

// Recht auf Vergessenwerden: physisches Löschen ALLER Daten des Tenants.
// Zweistufig: Tenant muss archiviert sein (sonst ErrTenantNotArchived);
// der Vorgang wird auditiert.
err = tenants.Purge(ctx, id)
```

Siehe auch [Geo-Partitionierung](/guides/geo/).
