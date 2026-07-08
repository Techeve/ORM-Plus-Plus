package orm

import "errors"

// Sentinel-Fehler laut doc/API.md, Abschnitt 11. Alle mit errors.Is prüfbar.
var (
	ErrNotFound          = errors.New("orm: nicht gefunden")
	ErrNoTenant          = errors.New("orm: kein Tenant im Context (fail-closed)")
	ErrNoGeo             = errors.New("orm: kein Daten-Geo im Context bei Mehr-Regionen-Topologie")
	ErrRegionNotActive   = errors.New("orm: Region unbekannt oder nicht aktiv")
	ErrVersionConflict   = errors.New("orm: Versionskonflikt (optimistisches Locking)")
	ErrWaitTimeout       = errors.New("orm: WaitFor-Frist abgelaufen")
	ErrSchemaDrift       = errors.New("orm: Modelle geändert ohne SchemaVersion-Erhöhung")
	ErrMigrationPending  = errors.New("orm: Migration nicht abgeschlossen")
	ErrReadOnlyReplica   = errors.New("orm: Schreibzugriff auf Replikat (WriteHomeOnly)")
	ErrUnknownTenant     = errors.New("orm: Tenant existiert nicht oder ist archiviert")
	ErrRequiredField     = errors.New("orm: required-Feld nicht gesetzt")
	ErrInvalidReference  = errors.New("orm: Referenzziel existiert nicht oder fremder Tenant")
	ErrReferenceInUse    = errors.New("orm: Datensatz wird noch referenziert")
	ErrInvalidValue      = errors.New("orm: Wert außerhalb der enum-Wertemenge")
	ErrRequiresTx        = errors.New("orm: Operation nur innerhalb einer Transaktion")
	ErrTenantNotArchived = errors.New("orm: Purge erfordert archivierten Tenant")
)
