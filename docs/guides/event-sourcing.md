---
title: Event Sourcing
description: Aggregate, Append, Historie/Zeitreisen, Reaktoren, Upcaster, Snapshots.
sidebar:
  order: 3
---

Ein ES-Model besteht aus: dem Zustand (Struct mit eingebettetem
`orm.Aggregate`), den Event-Payloads und genau einer Pflichtfunktion `Apply`.

```go
type DNSZone struct {
	orm.Aggregate   // bringt ID/Version/Laden/History/… mit

	Name    string   `orm:"index,unique"`
	Records []Record `orm:"json"`
	Status  string   `orm:"index"`
}

// Event-Payloads: reine Daten-Structs, nur das Delta.
type ZoneCreated struct{ Name string }
type RecordAdded struct{ Record Record }
type ZoneDeleted struct{}

// Die einzige Pflicht des Entwicklers — pure Funktion, keine DB-Berührung.
// Sie ist zugleich Projektions-, Rebuild- UND Snapshot-Logik.
func (z *DNSZone) Apply(e orm.Event) error {
	switch ev := e.Payload.(type) {
	case ZoneCreated:  z.Name, z.Status = ev.Name, "active"
	case RecordAdded:  z.Records = append(z.Records, ev.Record)
	case ZoneDeleted:  z.Status = "deleted"
	}
	return nil
}
```

Registrierung mit Event-Namen; der volle CloudEvents-Typ ergibt sich aus
`EventTypePrefix + Name + ".v" + Version`, z. B. `de.techeve.dns.zone.record_added.v1`:

```go
orm.Register[DNSZone](db, orm.EventSourced(),
	orm.Events(
		orm.E[ZoneCreated]("zone.created"),
		orm.E[RecordAdded]("zone.record_added"),
		orm.E[ZoneDeleted]("zone.deleted"),
	),
	orm.SnapshotEvery(200),
)
```

## Schreiben: Append

```go
zone := orm.New[DNSZone](db)                        // erzeugt ID (UUIDv7)
pos, err := zone.Append(ctx, ZoneCreated{Name: "example.org"})

zone, err := orm.Load[DNSZone](ctx, db, zoneID)     // bestehendes Aggregat
pos, err = zone.Append(ctx, RecordAdded{Record: rec})
```

`Append` hängt Events **atomar** an und erwartet implizit die geladene
Aggregat-Version — kam jemand dazwischen: `ErrVersionConflict` (dann
`Refresh` + erneut). Jedes `Append` löst die Trigger-Kette aus: eingebaute
Projektion → `OnEvent`-Reaktoren → `Watch`-Streams.

## Lesen: Zustand, Historie, Zeitreisen

```go
zone, err := orm.Load[DNSZone](ctx, db, zoneID)  // aus dem Read-Model (schnell)

// Read-your-writes: auf die eigene Schreibposition warten:
zone, err = orm.Load[DNSZone](ctx, db, zoneID, orm.WaitFor(pos, 2*time.Second))

old, err := zone.AtVersion(ctx, 42)   // Zustand nach Event 42
old, err  = zone.AtTime(ctx, ts)      // Zustand zu einem Zeitpunkt

for ev, err := range zone.History(ctx) {  // Event-Strom (Audit) als CloudEvents
	fmt.Println(ev.Sequence, ev.Type, ev.Time, ev.Data)
}
```

## Reaktoren & Live-Streams

**`OnEvent` — der verlässliche Pfad.** Persistent, at-least-once,
checkpointed, lease-koordiniert, rebuildfähig — für abgeleitete Read-Views,
Such-Indizes, Benachrichtigungs-Fanout. Handler müssen **idempotent** sein.

```go
orm.OnEvent[DNSZone](db, "zone.*",
	func(ctx context.Context, ce orm.CloudEvent, tx orm.Tx) error {
		return updateZoneDashboardView(ctx, tx, ce)   // läuft transaktional
	},
)
```

**`Watch` — der schnelle Pfad.** Flüchtige Live-Benachrichtigung an
verbundene Clients (SSE/WebSocket).

## Upcaster (Event-Schema-Versionen)

Events sind unveränderlich. Ändert sich ein Format, wird eine neue Version
registriert und ein Upcaster transformiert alte Events **beim Lesen**:

```go
orm.Upcast(db, "zone.record_added", 1,
	func(old RecordAddedV1) (RecordAdded, error) {
		return RecordAdded{Record: old.Entry}, nil
	},
)
```

## Snapshots

Snapshots entstehen automatisch nach der Model-Politik (`SnapshotEvery`),
asynchron, nie im Schreibpfad. `Load` faltet Snapshot + Restevents transparent.
