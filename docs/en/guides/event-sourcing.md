---
title: Event sourcing
description: Aggregates, append, history/time travel, reactors, upcasters, snapshots.
sidebar:
  order: 3
---

An ES model consists of: the state (a struct embedding `orm.Aggregate`), the
event payloads, and exactly one mandatory `Apply` function.

```go
type DNSZone struct {
	orm.Aggregate   // brings ID/Version/Load/History/…

	Name    string   `orm:"index,unique"`
	Records []Record `orm:"json"`
	Status  string   `orm:"index"`
}

// Event payloads: pure data structs, only the delta.
type ZoneCreated struct{ Name string }
type RecordAdded struct{ Record Record }
type ZoneDeleted struct{}

// The developer's only duty — pure function, no DB access.
// It is projection, rebuild AND snapshot logic in one.
func (z *DNSZone) Apply(e orm.Event) error {
	switch ev := e.Payload.(type) {
	case ZoneCreated:  z.Name, z.Status = ev.Name, "active"
	case RecordAdded:  z.Records = append(z.Records, ev.Record)
	case ZoneDeleted:  z.Status = "deleted"
	}
	return nil
}
```

Register with event names; the full CloudEvents type is
`EventTypePrefix + Name + ".v" + Version`, e.g. `de.techeve.dns.zone.record_added.v1`:

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

## Writing: append

```go
zone := orm.New[DNSZone](db)                        // creates ID (UUIDv7)
pos, err := zone.Append(ctx, ZoneCreated{Name: "example.org"})

zone, err := orm.Load[DNSZone](ctx, db, zoneID)     // existing aggregate
pos, err = zone.Append(ctx, RecordAdded{Record: rec})
```

`Append` appends events **atomically** and implicitly expects the loaded
aggregate version — if someone came in between: `ErrVersionConflict` (then
`Refresh` + retry). `Append` also writes the read-model row **in the same
transaction**: from the commit on, every `Query` on every node sees the new
state without waiting for a worker. Every `Append` then triggers the chain:
built-in projection (catches up, forward only) → `OnEvent` reactors →
`Watch` streams.

## Reading: state, history, time travel

```go
zone, err := orm.Load[DNSZone](ctx, db, zoneID)  // from the read model (fast)

// Read-your-writes: wait for your own write position. For a position from
// your own Append this returns immediately — the row is already there.
zone, err = orm.Load[DNSZone](ctx, db, zoneID, orm.WaitFor(pos, 2*time.Second))

old, err := zone.AtVersion(ctx, 42)   // state after event 42
old, err  = zone.AtTime(ctx, ts)      // state at a point in time

for ev, err := range zone.History(ctx) {  // event stream (audit) as CloudEvents
	fmt.Println(ev.Sequence, ev.Type, ev.Time, ev.Data)
}
```

## Reactors & live streams

**`OnEvent` — the reliable path.** Persistent, at-least-once, checkpointed,
lease-coordinated, rebuildable — for derived read views, search indexes,
notification fan-out. Handlers must be **idempotent**.

```go
orm.OnEvent[DNSZone](db, "zone.*",
	func(ctx context.Context, ce orm.CloudEvent, tx orm.Tx) error {
		return updateZoneDashboardView(ctx, tx, ce)   // runs transactionally
	},
)
```

**`Watch` — the fast path.** Ephemeral live notification to connected clients
(SSE/WebSocket).

## Upcasters (event schema versions)

Events are immutable. When a format changes, a new version is registered and
an upcaster transforms old events **on read**:

```go
orm.Upcast(db, "zone.record_added", 1,
	func(old RecordAddedV1) (RecordAdded, error) {
		return RecordAdded{Record: old.Entry}, nil
	},
)
```

## Snapshots

Snapshots are created automatically per model policy (`SnapshotEvery`),
asynchronously, never in the write path. `Load` folds snapshot + remaining
events transparently.
