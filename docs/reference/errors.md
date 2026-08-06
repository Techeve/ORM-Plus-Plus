---
title: Fehler
description: Alle Sentinel-Fehler, mit errors.Is prüfbar.
sidebar:
  order: 3
---

Alle Fehler sind mit `errors.Is` prüfbare Sentinel-Werte.

| Fehler | Bedeutung |
|---|---|
| `orm.ErrNotFound` | Datensatz/Aggregat existiert nicht (im Tenant-/Geo-Scope) |
| `orm.ErrNoTenant` | Context ohne Tenant (fail-closed) |
| `orm.ErrUnknownTenant` | Tenant-ID existiert nicht im Register oder ist archiviert |
| `orm.ErrRequiredField` | required-Feld beim Insert nicht gesetzt (Zero-Value) |
| `orm.ErrInvalidReference` | ref-Ziel existiert nicht oder gehört zu einem anderen Tenant |
| `orm.ErrReferenceInUse` | Löschen verweigert: Datensatz wird noch referenziert |
| `orm.ErrInvalidValue` | Wert außerhalb der enum-Wertemenge |
| `orm.ErrRequiresTx` | Operation (z. B. GetForUpdate) außerhalb einer Transaktion |
| `orm.ErrTenantNotArchived` | Purge auf einen nicht archivierten Tenant |
| `orm.ErrTenantNotEmpty` | `Import` in einen aktiven Tenant, der noch Daten hält |
| `orm.ErrImportIncomplete` | Tenant aus einem abgebrochenen Import; Strom ohne Schlusszeile |
| `orm.ErrExportSchemaMismatch` | Export von einem anderen Schemastand (ohne `AllowSchemaDrift`) |
| `orm.ErrUnknownEventType` | Event im Strom, dessen Typ hier nicht deklariert ist |
| `orm.ErrNoGeo` | Mehr-Regionen-Topologie, aber kein Daten-Geo im Context |
| `orm.ErrRegionNotActive` | Daten-Geo zeigt auf bootstrapping/draining/unbekannte Region |
| `orm.ErrVersionConflict` | Optimistisches Locking: CRUD-version oder Aggregat-Version veraltet |
| `orm.ErrWaitTimeout` | WaitFor-Frist abgelaufen, Projektion hing hinterher |
| `orm.ErrSchemaDrift` | Modelle geändert ohne SchemaVersion-Erhöhung |
| `orm.ErrMigrationPending` | Operation erfordert abgeschlossene Migration |
| `orm.ErrReadOnlyReplica` | Schreibzugriff auf Replikat bei WriteHomeOnly |
