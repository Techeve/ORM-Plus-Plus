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
	orm.Region("eu-central", orm.Placement("cloud1.eu-central-1")),
	orm.Region("us-east",    orm.Placement("cloud1.us-east-1")),
	orm.Region("ap-south",   orm.Placement("cloud1.ap-south-1")),
)
```

**Regeln:**

- Keine `Topology`-Deklaration ⇒ implizite Region `local`. Der einfachste Fall
  braucht null Geo-Code.
- **Region hinzufügen** = zusätzliche `orm.Region(...)`-Zeile + Rollout.
  Additiv, kein Sondermodus.
- Auf SQLite/Single-Region-Postgres kollabieren alle Regionen auf eine — die
  Deklaration bleibt gültig (Grundprinzip Verhaltensgleichheit).

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
