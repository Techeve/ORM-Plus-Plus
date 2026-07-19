---
title: Schema-Versionen & Migration
description: Expand/Contract mit Dual-Write — Migration ohne Ausfall.
sidebar:
  order: 4
---

## Deklaration

```go
orm.SchemaVersion(db, 3)   // Schema-Version, die diese App-Ausgabe erwartet

orm.MigrationTo(db, 3,     // Schritte von 2 nach 3; ältere MigrationTo bleiben im Code
	orm.ReplaceModel[ZoneV2, DNSZone](func(ctx, old ZoneV2) (DNSZone, error) {
		return DNSZone{ /* Umbau */ }, nil
	}),
	orm.BatchScript("normalize-records", func(ctx, b orm.Batch) error {
		// b liefert Zeilen häppchenweise; Checkpoint verwaltet die Engine
		return nil
	}),
)
```

- **Additive Änderungen** (neue Spalte/Index/Model) brauchen keinen Schritt (Auto-Diff).
- **Entfallende Felder** werden mit `deprecated` markiert, nie automatisch gelöscht.
- **Drift-Schutz:** Modelle geändert ohne Versions-Erhöhung ⇒ Startfehler (Checksum).

## Ausführung

```go
err := db.Migrate(ctx)   // Normalfall, idempotent
```

`Migrate` durchläuft die Zustandsmaschine
`idle → expanding → backfill → dual-write → finalizing → idle`:

- **expanding:** additive DDL, global, ein Leader (Lease).
- **backfill:** geo-parallel; Arbeitseinheit ist der Shard `(Schritt, Geo,
  Schlüsselbereich)`, vergeben per Lease nur an Worker **derselben Region**.
  Wiederaufnehmbar, drosselbar.
- **dual-write:** alte Instanzen laufen unverändert weiter; ihre Änderungen
  werden laufend in die neue Struktur nachgezogen. Beide App-Generationen
  koexistieren.
- **finalizing:** explizit — beendet Dual-Write, entfernt `deprecated`-Felder
  und Alt-Tabellen.

```go
err := db.FinalizeMigration(ctx, 3)
```

Vorbedingung (geprüft): keine lebende Instanz mit älterer Schema-Version im
Register, alle Regionen mit Backfill fertig.

## Rollout im Cluster

1. Neue App-Version (SchemaVersion n+1) regionsweise ausrollen — `Migrate`
   schaltet auf `expanding`, dann `backfill`, dann `dual-write`.
2. Alte Instanzen abbauen (Instanzregister leert sich).
3. `FinalizeMigration` — von einem Betriebs-Job oder manuell.

## Beispiel: Batch-Migrationsskript mit eigenem Checkpoint

`BatchScript` selbst legt keine Iterationsstrategie fest — das Skript nutzt
die normalen Query- und Update-APIs und sichert seinen eigenen Fortschritt.
So sieht eine Normalisierung aus, die bei einem Neustart genau dort
weitermacht, wo sie unterbrochen wurde:

```go
orm.MigrationTo(db, 4,
	orm.BatchScript("normalize-email", func(ctx context.Context, b orm.Batch) error {
		zuletzt, err := b.Checkpoint(ctx) // "" beim allerersten Lauf
		if err != nil {
			return err
		}

		var verarbeitet int64
		for konto, err := range orm.Query[ProviderAccount](db, ctx).
			Where(orm.Gt("ID", zuletzt)).
			OrderBy("ID", orm.Asc).
			Iter() {
			if err != nil {
				return err
			}
			normalisiert := strings.ToLower(strings.TrimSpace(konto.Email))
			if normalisiert == konto.Email {
				continue
			}
			if _, err := orm.Query[ProviderAccount](db, ctx).
				Where(orm.Eq("ID", konto.ID)).
				UpdateSet(orm.Set("Email", normalisiert)); err != nil {
				return err
			}
			verarbeitet++
			if verarbeitet%500 == 0 { // alle 500 Zeilen checkpointen, nicht jede einzelne
				if err := b.SaveCheckpoint(ctx, konto.ID.String(), verarbeitet); err != nil {
					return err
				}
			}
		}
		return nil
	}),
)
```

Bricht der Prozess mitten im Lauf ab, findet der nächste Versuch über
`b.Checkpoint(ctx)` die zuletzt gesicherte ID wieder und setzt die
`Where(orm.Gt("ID", zuletzt))`-Bedingung genau dort fort — das Skript muss
dafür nur idempotent aufsetzbar sein, nicht das gesamte Backfill neu
berechnen.
