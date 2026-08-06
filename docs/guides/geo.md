---
title: Geo-Partitionierung
description: Topologie, Geo-Modi, Replikate, lokal-bevorzugtes Lesen.
sidebar:
  order: 7
---

Die Topologie beschreibt, **welche Regionen der Cluster hat**. Sie ist
Cluster-Zustand, keine Instanz-Konfiguration: jede Instanz deklariert dieselbe
Topologie (gleiches Binary).

```go
orm.Topology(db,
	orm.Region("eu-central",   orm.Placement("ts_eu_central")),
	orm.Region("eu-southwest", orm.Placement("ts_eu_southwest")),
	orm.Region("na",           orm.Placement("ts_na")),
)
```

**Regeln:**

- Keine `Topology`-Deklaration ⇒ implizite Region `local`. Der einfachste Fall
  braucht null Geo-Code.
- **Region hinzufügen** = zusätzliche `orm.Region(...)`-Zeile + Rollout.
  Additiv, kein Sondermodus. `Migrate` gleicht die Partitionen bei **jedem**
  Lauf gegen die Topologie ab, auch ohne neue Schema-Version.
- **Region entfernen** = aus der Deklaration nehmen, ausrollen, dann
  `db.RemoveRegion(ctx, name)`. Hält die Region noch Daten, schlägt der Aufruf
  mit `ErrRegionHasData` fehl und nennt die Tabellen.
- Auf SQLite/Single-Region-Postgres kollabieren alle Regionen auf eine — die
  Deklaration bleibt gültig (Grundprinzip Verhaltensgleichheit).

## Geo-Partitionierung ist Kernfähigkeit

Sobald eine Topologie deklariert ist, partitioniert ORM++ auf PG/YB
**sämtliche** Tabellen eines Nicht-GeoGlobal-Models nach `geo` — CRUD-Tabellen,
ES-Read-Models, Event-Logs, Archive und Snapshots. Die API ändert sich dadurch
nicht: Reads filtern nie auf Geo, `Get`/`Update`/`Delete` per ID finden den
Datensatz in jeder Region. Bestehende Installationen mit unpartitionierten
Tabellen überführt `Migrate` selbsttätig (wiederaufnehmbar; den ersten
`Migrate` nach dem Upgrade in eine ruhige Phase legen).

Zwei Dinge übernimmt dabei die Engine, weil sie auf partitionierten Tabellen
kein natives Pendant haben — auf allen Backends gleich, SQLite eingeschlossen:

- **Referenzen auf partitionierte Ziele**: Existenz- und Tenant-Prüfung beim
  Schreiben, `restrict` vor dem Löschen, `setnull`/`cascade` als Emulation in
  der Lösch-Transaktion.
- **Unique-Constraints**: physisch gilt Eindeutigkeit pro Geo; die
  tenant-globale Eindeutigkeit prüft die Engine vor jedem Schreiben und
  meldet `orm.ErrUniqueConflict`.

## Placement: aus dem Etikett wird physische Residenz

Ohne `Placement` ist die Region ein Spaltenwert — die Zeilen liegen dort, wo
die Datenbank sie hinlegt. `orm.Placement("ts_eu_central")` benennt einen
**vorhandenen** Tablespace, an den ORM++ die Partitionen der Region bindet.

ORM++ legt keine Tablespaces an: Replikatzahl und Placement-Blöcke sind eine
Betriebsentscheidung. Existiert der benannte Tablespace nicht, bricht `Migrate`
mit `ErrPlacementNotFound` ab, bevor irgendeine DDL läuft.

```sql
-- Betriebsaufgabe, einmal je Region (YugabyteDB):
CREATE TABLESPACE ts_eu_central WITH (replica_placement='{
  "num_replicas": 3,
  "placement_blocks": [
    {"cloud":"cloud1","region":"eu-central","zone":"eu-central-1a","min_num_replicas":3}
  ]}');
```

Nachgemessen auf einem Drei-Knoten-Cluster mit je einem Knoten pro Region —
`yb_local_tablets` je Knoten, gezählt für die Partitionen einer CRUD-Tabelle
(für Event-Logs, Archive und Snapshots gilt dasselbe Muster):

| Partition | eu-central | eu-southwest | na |
|---|---|---|---|
| `geo_firma_geo_eu-central` | 1 | 0 | 0 |
| `geo_firma_geo_eu-southwest` | 0 | 1 | 0 |
| `geo_firma_geo_na` | 0 | 0 | 1 |
| `geo_firma_geo_default` (ungebunden) | 3 | 0 | 0 |

:::caution[ALTER TABLE … SET TABLESPACE bindet nicht um]
Die Bindung entsteht ausschließlich beim `CREATE TABLE` der Partition.
YugabyteDB nimmt `ALTER TABLE … SET TABLESPACE` zwar an, meldet
„data movement successfully initiated“ und schreibt den Katalog um — ohne
`ysql_beta_feature_tablespace_alteration` bewegen sich die Tablets aber nicht.
Der Katalog behauptete dann eine Platzierung, die es nicht gibt. ORM++
verwendet das deshalb nicht: liegt eine bestehende Partition nicht im
deklarierten Tablespace, meldet `Migrate` `ErrPlacementMismatch`, statt eine
Umbindung vorzutäuschen.
:::

## Umzug in eine andere Region

```go
// Eine Organisation zieht nach Nordamerika — alle Modelle auf einmal:
err := db.MoveTenant(ctx, tenant, "na")

// Oder gezielt ein Datensatz bzw. ein ganzes Aggregat:
err = orm.Repo[Zone](db).SetGeo(ctx, zoneID, "na")
```

- `SetGeo` gilt für alle Geo-Modi außer `GeoGlobal`. Replikat-Optionen bleiben
  `GeoFlexible` vorbehalten.
- Auf event-sourced Modellen zieht `SetGeo` das ganze Aggregat um — Event-Log,
  Archiv und Read-Model gemeinsam, sonst risse das Geo-Pinning.
- Auf partitionierten Backends wandern die Zeilen dabei **physisch** in die
  Partition der Zielregion.
- Umgezogene Events bekommen neue `seq`-Werte am Ende der Zielregion (die
  Geo-Sequenz ist pro Region monoton). Projektionen dort wenden sie als
  Nachzügler erneut an — at-least-once, idempotent.
- `MoveTenant` läuft batchweise und ist idempotent: nach einem Abbruch setzt
  ein erneuter Aufruf ihn fort.

## Geo-Modi je Modell

| Option | Bedeutung |
|---|---|
| `orm.GeoScoped()` | Jeder Datensatz liegt in genau einer Region (Normalfall). |
| `orm.GeoGlobal()` | Model ist in **allen** Regionen vorhanden (Stammdaten). Schreiben zahlt Cross-Region-Konsens. |
| `orm.GeoFlexible(opts...)` | **Pro Datensatz** wählbar: Heimatregion + lesende Replikate. |

## Geo im Context & Replikate

```go
ctx = orm.WithGeo(ctx, "eu-central", orm.ReplicateTo("us-east", "ap-south"))
err := groups.Insert(ctx, g)   // Heimat EU, lesende Kopien US + AP

// Später umziehen / Replikate ändern:
err = groups.SetGeo(ctx, g.ID, "us-east", orm.ReplicateTo("eu-central"))
```

`WithGeo` bestimmt das **Daten-Geo** (wohin ein Datensatz gehört) — unabhängig
vom Instanz-Geo. Eine EU-Instanz darf einen US-Datensatz schreiben; er landet
korrekt in der US-Partition (mit Latenz). Lesen ist **lokal-bevorzugt**:
existiert in der Region der lesenden Instanz eine Kopie, kommt sie von dort;
sonst antwortet die Heimatregion.

**Geo-Pinning (ES):** Das Daten-Geo klebt ab dem ersten Event am Aggregat.
`WithGeo` bestimmt die Heimatregion bei der Entstehung; Folge-Appends schreiben
immer in die Heimat-Partition.

## Beispiel: Konfigurationsdaten mit lokal-bevorzugtem Lesen

Ein Model, das häufig gelesen, aber selten geschrieben wird — etwa
Mandanten-Einstellungen — profitiert von `GeoFlexible`: jede Region bekommt
eine lesende Kopie, Schreibzugriffe laufen zur Heimatregion durch:

```go
type TenantSettings struct {
	ID    orm.ID `orm:"pk"`
	Key   string `orm:"unique"`
	Value string
}

orm.Register[TenantSettings](db, orm.CRUD(),
	orm.GeoFlexible(orm.WriteForwarding()), // Schreiben auf Replikat -> zur Heimat weitergeleitet
)

settings := orm.Repo[TenantSettings](db)

// Heimat EU, Replikate in US und AP direkt beim Anlegen:
ctx = orm.WithGeo(ctx, "eu-central", orm.ReplicateTo("us-east", "ap-south"))
_ = settings.Insert(ctx, &TenantSettings{Key: "theme", Value: "dark"})

// Eine Instanz in us-east liest anschließend lokal, ohne Cross-Region-Latenz —
// ganz ohne dass App-Code das explizit anfordert.
```

Ohne `orm.WriteForwarding()` würde derselbe Schreibversuch auf einer
us-east-Instanz mit einem Fehler abgelehnt (`orm.WriteHomeOnly()`, der
Default) — Replikate sind dann strikt lesend.
