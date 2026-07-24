---
title: Einführung
description: Grundprinzip von ORM++ — ein perfekter Abstraktions-Layer über SQLite, PostgreSQL und YugabyteDB.
sidebar:
  order: 1
---

**ORM++** ist ein model-first Persistenz-Modul für Go: klassisches ORM-Mapping
**plus** Event Sourcing, Projektionen, Snapshots/Archivierung und
Expand/Contract-Migrationen — optimiert für wenige, dafür voll ausgereizte
Datenbanken:

- **SQLite** — eingebettet, Desktop-/Demo-Betrieb
- **PostgreSQL** — Server, On-Prem
- **YugabyteDB** — verteilt, multi-tenant, geo-partitioniert

Die konsumierende Anwendung deklariert Modelle (Go-Structs mit Tags) und
arbeitet mit Commands, Events und typisierten Queries — sie schreibt **kein
SQL** und kennt keine DB-Details.

## Grundprinzip: Verhaltensgleichheit

Für die konsumierende Anwendung ist irrelevant, welche Datenbank darunter
liegt. Jede Deklaration wird auf jedem Backend akzeptiert und semantisch
erfüllt — nativ, wo die DB es kann; emuliert oder kollabiert, wo nicht.
App-Code verzweigt **nie** nach dem Backend; dieselbe Anwendung läuft
byte-identisch auf allen dreien.

Konsequenzen für die API:

- Es gibt keine Funktion, die das Backend preisgibt (kein `db.Kind()`).
  Einzige Ausnahme: die Observability-APIs zeigen dem *Betreiber* die
  physische Wahrheit.
- Eine Topologie mit fünf Regionen auf SQLite ist gültig — SQLite hat implizit
  die eine Region `local`, alle deklarierten Regionen mappen darauf.
- Die Anwendung schreibt **kein SQL** und kennt keine Tabellen-, Treiber- oder
  Dialektdetails.

## Was ORM++ mitbringt

- **CRUD-Modelle** mit typisiertem Repository und Query-Builder.
- **Event-Sourced-Aggregate** (CloudEvents 1.0), Snapshots und Archivierung.
- **Projektionen/Read-Models**, die Worker aus dem Event-Strom materialisieren.
- **Multi-Tenancy & Geo-Partitionierung** ab v1 im Datenmodell verankert.
- **Feld-Verschlüsselung** (AES-256-GCM) über ein `encrypted`-Tag.
- **Expand/Contract-Migrationen** mit Dual-Write für Betrieb ohne Ausfall.

## Weiter

- [Installation](/start/installation/)
- [Schnellstart](/start/quickstart/)
