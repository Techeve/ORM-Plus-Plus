package orm

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"
)

// Testmodell: ein event-sourced Ticket.

type Ticket struct {
	Aggregate

	Title  string   `orm:"index"`
	Status string   `orm:"index"`
	Notes  []string `orm:"json"`
}

type TicketOpened struct{ Title string }
type NoteAdded struct{ Note string }
type TicketClosed struct{}

func (t *Ticket) Apply(e Event) error {
	switch ev := e.Payload.(type) {
	case TicketOpened:
		t.Title, t.Status = ev.Title, "open"
	case NoteAdded:
		t.Notes = append(t.Notes, ev.Note)
	case TicketClosed:
		t.Status = "closed"
	default:
		return fmt.Errorf("unbekanntes Event %T", e.Payload)
	}
	return nil
}

func ticketEvents() ModelOption {
	return Events(
		E[TicketOpened]("ticket.opened"),
		E[NoteAdded]("ticket.note_added"),
		E[TicketClosed]("ticket.closed"),
	)
}

func esTestDB(t *testing.T, opts ...ModelOption) (*DB, context.Context) {
	t.Helper()
	db, err := Open(newTestStore(t)())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	if len(opts) == 0 {
		opts = []ModelOption{ticketEvents()}
	}
	Register[Ticket](db, EventSourced(), opts...)
	if err := db.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	return db, WithTenant(context.Background(), SingleTenant)
}

func TestAppendLoadRoundtrip(t *testing.T) {
	t.Parallel()
	db, ctx := esTestDB(t)

	tk := New[Ticket](db)
	if tk.ID().IsZero() {
		t.Fatal("New muss eine ID vergeben")
	}
	if _, err := tk.Append(ctx, TicketOpened{Title: "Drucker brennt"}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if _, err := tk.Append(ctx, NoteAdded{Note: "Feuerwehr gerufen"}, TicketClosed{}); err != nil {
		t.Fatalf("Append mehrere: %v", err)
	}
	if tk.Version() != 3 {
		t.Fatalf("Version = %d, erwartet 3", tk.Version())
	}
	if tk.Status != "closed" || len(tk.Notes) != 1 {
		t.Fatalf("In-Memory-Zustand falsch: %+v", tk)
	}

	loaded, err := Load[Ticket](ctx, db, tk.ID())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded.Title != "Drucker brennt" || loaded.Status != "closed" || len(loaded.Notes) != 1 {
		t.Fatalf("geladener Zustand falsch: %+v", loaded)
	}
	if loaded.Version() != 3 || loaded.UpdatedAt().IsZero() {
		t.Fatalf("Version/UpdatedAt falsch: v=%d", loaded.Version())
	}

	// Unbekanntes Aggregat und fremder Tenant verhalten sich wie nicht existent.
	if _, err := Load[Ticket](ctx, db, NewID()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Load unbekannt: %v, erwartet ErrNotFound", err)
	}
	other, _ := db.Tenants().Create(ctx, TenantInfo{Name: "Fremd"})
	foreign := WithTenant(context.Background(), other.ID)
	if _, err := Load[Ticket](foreign, db, tk.ID()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Load fremder Tenant: %v, erwartet ErrNotFound", err)
	}
	// Ohne Tenant fail-closed.
	if _, err := Load[Ticket](context.Background(), db, tk.ID()); !errors.Is(err, ErrNoTenant) {
		t.Fatalf("Load ohne Tenant: %v, erwartet ErrNoTenant", err)
	}
}

func TestAppendVersionConflict(t *testing.T) {
	t.Parallel()
	db, ctx := esTestDB(t)

	tk := New[Ticket](db)
	if _, err := tk.Append(ctx, TicketOpened{Title: "A"}); err != nil {
		t.Fatalf("Append: %v", err)
	}

	a, _ := Load[Ticket](ctx, db, tk.ID())
	b, _ := Load[Ticket](ctx, db, tk.ID())
	if _, err := a.Append(ctx, NoteAdded{Note: "von A"}); err != nil {
		t.Fatalf("Append A: %v", err)
	}
	if _, err := b.Append(ctx, NoteAdded{Note: "von B"}); !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("Append B: %v, erwartet ErrVersionConflict", err)
	}
	// Refresh + erneut anhängen löst den Konflikt.
	if err := b.Refresh(ctx); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if _, err := b.Append(ctx, NoteAdded{Note: "von B nach Refresh"}); err != nil {
		t.Fatalf("Append nach Refresh: %v", err)
	}
	if len(b.Notes) != 2 {
		t.Fatalf("Notes = %v", b.Notes)
	}
}

func TestProjectionQueryAndWaitFor(t *testing.T) {
	t.Parallel()
	db, ctx := esTestDB(t)
	if err := db.StartWorkers(context.Background()); err != nil {
		t.Fatalf("StartWorkers: %v", err)
	}

	tk := New[Ticket](db)
	pos, err := tk.Append(ctx, TicketOpened{Title: "Projektion"})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}

	// Read-your-writes: warten, bis die Projektion die Position erreicht hat.
	if _, err := Load[Ticket](ctx, db, tk.ID(), WaitFor(pos, 5*time.Second)); err != nil {
		t.Fatalf("Load mit WaitFor: %v", err)
	}
	open, err := Query[Ticket](db, ctx).Where(Eq("Status", "open")).All()
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(open) != 1 || open[0].Title != "Projektion" {
		t.Fatalf("Read-Model: %+v", open)
	}
	if open[0].ID() != tk.ID() || open[0].Version() != 1 {
		t.Fatalf("Aggregat-Verdrahtung im Query-Ergebnis fehlt: id=%s v=%d", open[0].ID(), open[0].Version())
	}

	// Folge-Event aktualisiert die Zeile.
	pos, err = tk.Append(ctx, TicketClosed{})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if _, err := Load[Ticket](ctx, db, tk.ID(), WaitFor(pos, 5*time.Second)); err != nil {
		t.Fatalf("WaitFor: %v", err)
	}
	closed, _ := Query[Ticket](db, ctx).Where(Eq("Status", "closed")).Count()
	if closed != 1 {
		t.Fatalf("closed = %d, erwartet 1", closed)
	}

	// WaitFor auf eine nie erreichte Position ⇒ ErrWaitTimeout.
	fake := Position{seqs: map[string]int64{"local": 9999}}
	if _, err := Load[Ticket](ctx, db, tk.ID(), WaitFor(fake, 50*time.Millisecond)); !errors.Is(err, ErrWaitTimeout) {
		t.Fatalf("WaitFor fake: %v, erwartet ErrWaitTimeout", err)
	}
}

func TestProjectionTenantIsolation(t *testing.T) {
	t.Parallel()
	db, ctx := esTestDB(t)

	other, _ := db.Tenants().Create(ctx, TenantInfo{Name: "B"})
	ctxB := WithTenant(context.Background(), other.ID)

	ta := New[Ticket](db)
	pa, _ := ta.Append(ctx, TicketOpened{Title: "von A"})
	tb := New[Ticket](db)
	pb, _ := tb.Append(ctxB, TicketOpened{Title: "von B"})

	// Synchron projizieren (ohne Worker).
	if err := db.processProjection(context.Background(), db.reg.byName["Ticket"]); err != nil {
		t.Fatalf("processProjection: %v", err)
	}
	_ = pa
	_ = pb

	seen, err := Query[Ticket](db, ctx).All()
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(seen) != 1 || seen[0].Title != "von A" {
		t.Fatalf("Tenant-Isolation im Read-Model verletzt: %+v", seen)
	}
}

func TestSnapshotsCreatedUsedAndPruned(t *testing.T) {
	t.Parallel()
	db, ctx := esTestDB(t, ticketEvents(), SnapshotEvery(5), SnapshotKeepLast(2))

	tk := New[Ticket](db)
	if _, err := tk.Append(ctx, TicketOpened{Title: "Snap"}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	for i := 0; i < 11; i++ {
		if _, err := tk.Append(ctx, NoteAdded{Note: fmt.Sprintf("n%d", i)}); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	m := db.reg.byName["Ticket"]
	if err := db.maybeSnapshot(context.Background(), m); err != nil {
		t.Fatalf("maybeSnapshot: %v", err)
	}

	var count int
	var maxSeq int64
	if err := db.sql.QueryRow(`SELECT COUNT(*), COALESCE(MAX(aggregate_seq),0) FROM ticket_snapshots`).Scan(&count, &maxSeq); err != nil {
		t.Fatalf("Snapshot-Zählung: %v", err)
	}
	if count < 1 || maxSeq != 12 {
		t.Fatalf("Snapshots: count=%d maxSeq=%d, erwartet ≥1 und 12", count, maxSeq)
	}

	// Beweis, dass Load den Snapshot nutzt: alte Events entfernen (simuliert
	// Archivierung) — der Zustand muss trotzdem vollständig sein.
	if _, err := db.q().ExecContext(context.Background(), `DELETE FROM ticket_events WHERE aggregate_seq <= ?`, maxSeq); err != nil {
		t.Fatalf("Events löschen: %v", err)
	}
	loaded, err := Load[Ticket](ctx, db, tk.ID())
	if err != nil {
		t.Fatalf("Load nach Archivierung: %v", err)
	}
	if loaded.Title != "Snap" || len(loaded.Notes) != 11 || loaded.Version() != 12 {
		t.Fatalf("Snapshot-Restore falsch: title=%q notes=%d v=%d", loaded.Title, len(loaded.Notes), loaded.Version())
	}

	// KeepLast: weitere Events + Snapshots dürfen die Aufbewahrung nicht sprengen.
	for i := 0; i < 12; i++ {
		if _, err := loaded.Append(ctx, NoteAdded{Note: fmt.Sprintf("m%d", i)}); err != nil {
			t.Fatalf("Append: %v", err)
		}
		if err := db.maybeSnapshot(context.Background(), m); err != nil {
			t.Fatalf("maybeSnapshot: %v", err)
		}
	}
	if err := db.sql.QueryRow(`SELECT COUNT(*) FROM ticket_snapshots`).Scan(&count); err != nil {
		t.Fatalf("Snapshot-Zählung: %v", err)
	}
	if count > 2 {
		t.Fatalf("KeepLast(2) verletzt: %d Snapshots", count)
	}
}

func TestHistoryAtVersionAtTime(t *testing.T) {
	t.Parallel()
	db, ctx := esTestDB(t)

	tk := New[Ticket](db)
	if _, err := tk.Append(ctx, TicketOpened{Title: "Zeitreise"}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	time.Sleep(5 * time.Millisecond)
	between := time.Now().UTC()
	time.Sleep(5 * time.Millisecond)
	if _, err := tk.Append(ctx, NoteAdded{Note: "später"}); err != nil {
		t.Fatalf("Append: %v", err)
	}

	// History liefert CloudEvents in Aggregat-Reihenfolge.
	var types []string
	for ce, err := range tk.History(ctx) {
		if err != nil {
			t.Fatalf("History: %v", err)
		}
		if ce.SpecVersion != "1.0" || ce.Subject != tk.ID().String() || ce.Tenant != SingleTenant {
			t.Fatalf("CloudEvent-Envelope falsch: %+v", ce)
		}
		types = append(types, ce.Type)
	}
	if len(types) != 2 {
		t.Fatalf("History-Länge %d, erwartet 2", len(types))
	}

	// AtVersion 1: nur das erste Event angewandt.
	old, err := tk.AtVersion(ctx, 1)
	if err != nil {
		t.Fatalf("AtVersion: %v", err)
	}
	if v1 := old.(*Ticket); len(v1.Notes) != 0 || v1.Title != "Zeitreise" {
		t.Fatalf("AtVersion(1): %+v", v1)
	}

	// AtTime zwischen den Events: ebenfalls nur Event 1.
	att, err := tk.AtTime(ctx, between)
	if err != nil {
		t.Fatalf("AtTime: %v", err)
	}
	if v1 := att.(*Ticket); len(v1.Notes) != 0 {
		t.Fatalf("AtTime: %+v", v1)
	}
}

func TestOnEventReactorAndRebuildView(t *testing.T) {
	t.Parallel()
	db, ctx := esTestDB(t)

	var mu sync.Mutex
	deliveries := 0
	seen := map[string]bool{} // Idempotenz über Event-ID
	OnEvent[Ticket](db, "ticket.*", func(_ context.Context, ce CloudEvent, _ Tx) error {
		mu.Lock()
		defer mu.Unlock()
		deliveries++
		seen[ce.ID] = true
		return nil
	}, Named("ticket-view"))

	tk := New[Ticket](db)
	if _, err := tk.Append(ctx, TicketOpened{Title: "Reaktor"}, NoteAdded{Note: "eins"}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	// Synchron verarbeiten (ohne Worker-Timing).
	db.processOnce(context.Background())

	mu.Lock()
	if deliveries != 2 || len(seen) != 2 {
		mu.Unlock()
		t.Fatalf("Reaktor: deliveries=%d seen=%d, erwartet 2/2", deliveries, len(seen))
	}
	mu.Unlock()

	// Rebuild spielt alles erneut ein: Zustellungen steigen (at-least-once),
	// die idempotente Menge bleibt konstant.
	if err := RebuildView(context.Background(), db, "ticket-view"); err != nil {
		t.Fatalf("RebuildView: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if deliveries != 4 || len(seen) != 2 {
		t.Fatalf("nach Rebuild: deliveries=%d seen=%d, erwartet 4/2", deliveries, len(seen))
	}
}

func TestRebuildProjection(t *testing.T) {
	t.Parallel()
	db, ctx := esTestDB(t)

	tk := New[Ticket](db)
	if _, err := tk.Append(ctx, TicketOpened{Title: "Rebuild"}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := db.processProjection(context.Background(), db.reg.byName["Ticket"]); err != nil {
		t.Fatalf("processProjection: %v", err)
	}
	// Read-Model absichtlich zerstören.
	if _, err := db.sql.Exec(`UPDATE ticket SET title = 'kaputt'`); err != nil {
		t.Fatalf("korrumpieren: %v", err)
	}
	if err := RebuildProjection[Ticket](context.Background(), db); err != nil {
		t.Fatalf("RebuildProjection: %v", err)
	}
	got, err := Query[Ticket](db, ctx).First()
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if got.Title != "Rebuild" {
		t.Fatalf("Rebuild hat nicht repariert: %q", got.Title)
	}
}

func TestStreamAndWatch(t *testing.T) {
	t.Parallel()
	db, ctx := esTestDB(t)

	tk := New[Ticket](db)
	pos1, err := tk.Append(ctx, TicketOpened{Title: "Strom"})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}

	// Watch abonnieren, dann weitere Events anhängen.
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	ch := Watch[Ticket](wctx, db)

	if _, err := tk.Append(ctx, NoteAdded{Note: "live"}, TicketClosed{}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	var live []CloudEvent
	timeout := time.After(3 * time.Second)
	for len(live) < 2 {
		select {
		case ce := <-ch:
			live = append(live, ce)
		case <-timeout:
			t.Fatalf("Watch: nur %d Events empfangen", len(live))
		}
	}
	if live[0].AggregateSeq != 2 || live[1].AggregateSeq != 3 {
		t.Fatalf("Watch-Reihenfolge: %d, %d", live[0].AggregateSeq, live[1].AggregateSeq)
	}

	// Stream ab pos1: nur die zwei späteren Events.
	var streamed []CloudEvent
	for ce, err := range Stream[Ticket](ctx, db, From(pos1)) {
		if err != nil {
			t.Fatalf("Stream: %v", err)
		}
		streamed = append(streamed, ce)
	}
	if len(streamed) != 2 || streamed[0].AggregateSeq != 2 {
		t.Fatalf("Stream ab Position: %d Events", len(streamed))
	}
	// Ohne From: alle drei.
	total := 0
	for _, err := range Stream[Ticket](ctx, db) {
		if err != nil {
			t.Fatalf("Stream: %v", err)
		}
		total++
	}
	if total != 3 {
		t.Fatalf("Stream gesamt: %d, erwartet 3", total)
	}
}

// --- Upcaster ---

type NoteAddedV1 struct{ Text string }

func TestUpcasterChainAndValidation(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	bg := context.Background()

	// Alte App-Version: note_added v1 mit Feld "Text".
	db1, err := Open(store())
	if err != nil {
		t.Fatalf("Open v1: %v", err)
	}
	Register[Ticket](db1, EventSourced(), Events(
		E[TicketOpened]("ticket.opened"),
		E[NoteAddedV1]("ticket.note_added"),
		E[TicketClosed]("ticket.closed"),
	))
	if err := db1.Migrate(bg); err != nil {
		t.Fatalf("Migrate v1: %v", err)
	}
	ctx := WithTenant(bg, SingleTenant)
	tk := New[Ticket](db1)
	if _, err := tk.Append(ctx, TicketOpened{Title: "Up"}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	// v1-Payload direkt anhängen (Apply kennt ihn nicht — macht nichts,
	// wir prüfen das Upcasting beim späteren Lesen).
	if _, err := db1.q().ExecContext(bg,
		`INSERT INTO ticket_events (aggregate_id, aggregate_seq, geo, seq, event_id, occurred_at, type_id, data, tenant_id)
		 SELECT aggregate_id, 2, geo, 2, ?, occurred_at, type_id, '{"Text":"alte Notiz"}', tenant_id
		 FROM ticket_events WHERE aggregate_seq = 1`, NewID().String()); err != nil {
		t.Fatalf("v1-Event einfügen: %v", err)
	}
	if _, err := db1.q().ExecContext(bg,
		`UPDATE ticket_events SET type_id = (SELECT type_id FROM ormpp_event_types WHERE type LIKE '%.ticket.note_added.v1') WHERE aggregate_seq = 2`); err != nil {
		t.Fatalf("type_id setzen: %v", err)
	}
	aggID := tk.ID()
	db1.Close()

	// Neue App-Version ohne Upcaster ⇒ Migrate schlägt fehl.
	db2, err := Open(store())
	if err != nil {
		t.Fatalf("Open v2: %v", err)
	}
	Register[Ticket](db2, EventSourced(), Events(
		E[TicketOpened]("ticket.opened"),
		E[NoteAdded]("ticket.note_added", V(2)),
		E[TicketClosed]("ticket.closed"),
	))
	if err := db2.Migrate(bg); err == nil {
		t.Fatal("Migrate ohne Upcaster muss fehlschlagen")
	}
	db2.Close()

	// Mit Upcaster: Migrate läuft, alte Events werden beim Lesen gehoben.
	db3, err := Open(store())
	if err != nil {
		t.Fatalf("Open v3: %v", err)
	}
	t.Cleanup(func() { db3.Close() })
	Register[Ticket](db3, EventSourced(), Events(
		E[TicketOpened]("ticket.opened"),
		E[NoteAdded]("ticket.note_added", V(2)),
		E[TicketClosed]("ticket.closed"),
	))
	Upcast(db3, "ticket.note_added", 1, func(old NoteAddedV1) (NoteAdded, error) {
		return NoteAdded{Note: old.Text}, nil
	})
	if err := db3.Migrate(bg); err != nil {
		t.Fatalf("Migrate mit Upcaster: %v", err)
	}
	loaded, err := Load[Ticket](WithTenant(bg, SingleTenant), db3, aggID)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(loaded.Notes) != 1 || loaded.Notes[0] != "alte Notiz" {
		t.Fatalf("Upcasting fehlgeschlagen: %+v", loaded.Notes)
	}
}

func TestESRegistrationValidation(t *testing.T) {
	t.Parallel()
	bg := context.Background()

	// Ohne Aggregate-Einbettung.
	type NoAgg struct {
		Title string
	}
	db, _ := Open(newTestStore(t)())
	defer db.Close()
	Register[NoAgg](db, EventSourced(), Events(E[TicketOpened]("x.opened")))
	if err := db.Migrate(bg); err == nil {
		t.Fatal("Migrate muss fehlende Aggregate-Einbettung ablehnen")
	}

	// Ohne Events-Deklaration.
	db2, _ := Open(newTestStore(t)())
	defer db2.Close()
	Register[Ticket](db2, EventSourced())
	if err := db2.Migrate(bg); err == nil {
		t.Fatal("Migrate muss fehlende Events-Deklaration ablehnen")
	}

	// CRUD-Repository auf ES-Model ist gesperrt.
	db3, ctx := esTestDB(t)
	if err := Repo[Ticket](db3).Insert(ctx, &Ticket{Title: "x"}); err == nil {
		t.Fatal("Repo.Insert auf ES-Model muss fehlschlagen")
	}
}

func TestAppendInTransaction(t *testing.T) {
	t.Parallel()
	db, ctx := esTestDB(t)

	// Append in einer Transaktion — Rollback nimmt die Events zurück.
	err := db.Tx(ctx, func(tx Tx) error {
		tk := New[Ticket](tx)
		if _, err := tk.Append(ctx, TicketOpened{Title: "in Tx"}); err != nil {
			return err
		}
		return fmt.Errorf("abbruch")
	})
	if err == nil {
		t.Fatal("Tx muss den Testfehler liefern")
	}
	var n int
	if err := db.sql.QueryRow(`SELECT COUNT(*) FROM ticket_events`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("Rollback hat Events hinterlassen: %d", n)
	}
}

// --- Phase 4b: Archivierung, Geo-Pinning, Worker-Leases ---

func TestArchiverMovesOldEventsTransparently(t *testing.T) {
	t.Parallel()
	db, ctx := esTestDB(t, ticketEvents(), SnapshotEvery(6), SnapshotKeepLast(2))
	bg := context.Background()

	tk := New[Ticket](db)
	if _, err := tk.Append(ctx, TicketOpened{Title: "Archiv"}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	for i := 0; i < 11; i++ {
		if _, err := tk.Append(ctx, NoteAdded{Note: fmt.Sprintf("n%d", i)}); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
		// Snapshots entstehen bei 6 und 12 (SnapshotEvery(6)).
		if err := db.maybeSnapshot(bg, db.reg.byName["Ticket"]); err != nil {
			t.Fatalf("maybeSnapshot: %v", err)
		}
	}
	m := db.reg.byName["Ticket"]
	// Projektion vorziehen (Archiv-Grenze ist auch der Projektions-Checkpoint).
	if err := db.processProjection(bg, m); err != nil {
		t.Fatalf("processProjection: %v", err)
	}
	if err := db.maybeArchive(bg, m); err != nil {
		t.Fatalf("maybeArchive: %v", err)
	}

	var hot, archived int
	if err := db.sql.QueryRow(`SELECT COUNT(*) FROM ticket_events`).Scan(&hot); err != nil {
		t.Fatal(err)
	}
	if err := db.sql.QueryRow(`SELECT COUNT(*) FROM ticket_events_archive`).Scan(&archived); err != nil {
		t.Fatal(err)
	}
	if archived != 6 || hot != 6 {
		t.Fatalf("Archivierung: hot=%d archiv=%d, erwartet 6/6 (Grenze = zweitjüngster Snapshot)", hot, archived)
	}

	// Normalpfad bleibt korrekt (Snapshot + Hot).
	loaded, err := Load[Ticket](ctx, db, tk.ID())
	if err != nil || loaded.Version() != 12 || len(loaded.Notes) != 11 {
		t.Fatalf("Load nach Archivierung: v=%d notes=%d (%v)", loaded.Version(), len(loaded.Notes), err)
	}
	// Historie liest transparent Hot + Archiv.
	n := 0
	for _, err := range tk.History(ctx) {
		if err != nil {
			t.Fatalf("History: %v", err)
		}
		n++
	}
	if n != 12 {
		t.Fatalf("History über Hot+Archiv: %d Events, erwartet 12", n)
	}
	// Zeitreise unter die Archiv-Grenze (kein Snapshot ≤ 3 mehr — faltet
	// von null durch die archivierten Events).
	old, err := tk.AtVersion(ctx, 3)
	if err != nil {
		t.Fatalf("AtVersion(3): %v", err)
	}
	if v3 := old.(*Ticket); len(v3.Notes) != 2 || v3.Title != "Archiv" {
		t.Fatalf("AtVersion(3) über Archiv: %+v", v3)
	}
	// Stream liefert weiter alle Events.
	total := 0
	for _, err := range Stream[Ticket](ctx, db) {
		if err != nil {
			t.Fatalf("Stream: %v", err)
		}
		total++
	}
	if total != 12 {
		t.Fatalf("Stream über Hot+Archiv: %d, erwartet 12", total)
	}
}

func TestAggregateGeoPinning(t *testing.T) {
	t.Parallel()
	db, err := Open(newTestStore(t)())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	Register[Ticket](db, EventSourced(), ticketEvents())
	Topology(db, Region("eu-central"), Region("us-east"))
	if err := db.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	base := WithTenant(context.Background(), SingleTenant)

	// Aggregat entsteht in eu-central …
	eu := WithGeo(base, "eu-central")
	tk := New[Ticket](db)
	if _, err := tk.Append(eu, TicketOpened{Title: "Pin"}); err != nil {
		t.Fatalf("Append eu: %v", err)
	}
	// … Folge-Append mit US-Context bleibt trotzdem in der Heimatregion.
	us := WithGeo(base, "us-east")
	if _, err := tk.Append(us, NoteAdded{Note: "aus US-Context"}); err != nil {
		t.Fatalf("Append us: %v", err)
	}
	var inUS, inEU int
	if err := db.sql.QueryRow(`SELECT COUNT(*) FROM ticket_events WHERE geo = 'us-east'`).Scan(&inUS); err != nil {
		t.Fatal(err)
	}
	if err := db.sql.QueryRow(`SELECT COUNT(*) FROM ticket_events WHERE geo = 'eu-central'`).Scan(&inEU); err != nil {
		t.Fatal(err)
	}
	if inUS != 0 || inEU != 2 {
		t.Fatalf("Geo-Pinning verletzt: eu=%d us=%d, erwartet 2/0", inEU, inUS)
	}
}

func TestWorkerLeaseCoordination(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	bg := context.Background()
	open := func() *DB {
		db, err := Open(store())
		if err != nil {
			t.Fatal(err)
		}
		Register[Ticket](db, EventSourced(), ticketEvents())
		if err := db.Migrate(bg); err != nil {
			t.Fatal(err)
		}
		return db
	}
	db1 := open()
	t.Cleanup(func() { db1.Close() })
	db2 := open()
	t.Cleanup(func() { db2.Close() })

	ctx := WithTenant(bg, SingleTenant)
	tk := New[Ticket](db1)
	if _, err := tk.Append(ctx, TicketOpened{Title: "Lease"}); err != nil {
		t.Fatalf("Append: %v", err)
	}

	// Instanz 1 hält die Aufgaben-Lease — Instanz 2 überspringt die Projektion.
	if ok, err := db1.acquireLease(bg, "worker:es:ticket", time.Hour); err != nil || !ok {
		t.Fatalf("acquireLease: %v/%v", ok, err)
	}
	db2.processOnce(bg)
	cp, err := getCheckpoint(bg, db2.q(), "projection:ticket", "local")
	if err != nil || cp != 0 {
		t.Fatalf("Instanz 2 hat trotz fremder Lease projiziert: cp=%d (%v)", cp, err)
	}
	// Lease frei ⇒ Instanz 2 übernimmt.
	db1.releaseLease(bg, "worker:es:ticket")
	db2.processOnce(bg)
	cp, err = getCheckpoint(bg, db2.q(), "projection:ticket", "local")
	if err != nil || cp != 1 {
		t.Fatalf("Übernahme nach Freigabe fehlgeschlagen: cp=%d (%v)", cp, err)
	}
}

// TestAppendErrorsRecognizedOnPartitions deckt die Fehlererkennung gegen die
// Meldungstexte aller drei Backends ab.
//
// Bei Geo-Partitionierung nennt PG/YB den Index der PARTITION. Wurde nur der
// Elternname geprüft, blieb eine Sequenzkollision unerkannt und damit
// unwiederholt — genau im Cluster, wo sie überhaupt erst auftritt.
func TestAppendErrorsRecognizedOnPartitions(t *testing.T) {
	t.Parallel()
	const table = "ticket_events"

	seq := []struct {
		name string
		msg  string
	}{
		{"Elterntabelle", `duplicate key value violates unique constraint "ux_ticket_events_geo_seq"`},
		{"Partition (default)", `duplicate key value violates unique constraint "ticket_events_geo_default_geo_seq_idx"`},
		{"Partition (Region)", `duplicate key value violates unique constraint "ticket_events_geo_eu_geo_seq_idx"`},
		{"SQLite", `UNIQUE constraint failed: ticket_events.geo, ticket_events.seq`},
	}
	for _, c := range seq {
		if !isSeqCollision(errors.New(c.msg), table) {
			t.Errorf("isSeqCollision(%s) = false, erwartet true", c.name)
		}
	}

	pk := []struct {
		name string
		msg  string
	}{
		{"Elterntabelle", `duplicate key value violates unique constraint "ticket_events_pkey"`},
		{"Partition (default)", `duplicate key value violates unique constraint "ticket_events_geo_default_pkey"`},
		{"Partition (Region)", `duplicate key value violates unique constraint "ticket_events_geo_eu_pkey"`},
		{"SQLite", `UNIQUE constraint failed: ticket_events.aggregate_id, ticket_events.aggregate_seq`},
	}
	for _, c := range pk {
		if err := classifyAppendErr(errors.New(c.msg), table); !errors.Is(err, ErrVersionConflict) {
			t.Errorf("classifyAppendErr(%s) = %v, erwartet ErrVersionConflict", c.name, err)
		}
	}

	// Eine Sequenzkollision darf NICHT als Versionskonflikt durchgehen: Sie ist
	// wiederholbar, ein Versionskonflikt gehört dem Aufrufer gemeldet.
	seqMsg := errors.New(`duplicate key value violates unique constraint "ticket_events_geo_default_geo_seq_idx"`)
	if err := classifyAppendErr(seqMsg, table); errors.Is(err, ErrVersionConflict) {
		t.Error("Sequenzkollision als ErrVersionConflict eingestuft")
	}

	// Fremde Fehler bleiben unberührt.
	other := errors.New(`connection refused`)
	if isSeqCollision(other, table) {
		t.Error("isSeqCollision auf fremdem Fehler")
	}
	if err := classifyAppendErr(other, table); errors.Is(err, ErrVersionConflict) {
		t.Error("fremder Fehler als ErrVersionConflict eingestuft")
	}
}

// TestConcurrentAppendsInCallerTx: parallele Appends verschiedener Aggregate
// innerhalb je eigener Aufrufer-Transaktion.
//
// Innerhalb einer fremden Transaktion kann ORM++ nicht wiederholen — es darf
// sie nicht neu starten. Die Vergabe der Geo-Sequenz muss deshalb schon
// kollisionsfrei sein, sonst schlägt der Unique-Index bis zum Aufrufer durch.
// Auf SQLite verdeckt der Single-Writer den Fall vollständig.
func TestConcurrentAppendsInCallerTx(t *testing.T) {
	t.Parallel()
	db, ctx := esTestDB(t)
	if !db.dial.serializesAppends() {
		t.Skip("Backend ohne Advisory-Sperren: Appends in fremder Transaktion koennen kollidieren (dokumentierte Grenze)")
	}

	const n = 12
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = db.Tx(ctx, func(tx Tx) error {
				tk := New[Ticket](tx)
				_, err := tk.Append(ctx, TicketOpened{Title: fmt.Sprintf("T%d", i)})
				return err
			})
		}()
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("Append %d: %v", i, err)
		}
	}

	// Jedes Event muss eine eigene Geo-Sequenz bekommen haben.
	var rows, distinct int64
	q := db.q()
	if err := q.QueryRowContext(ctx,
		`SELECT COUNT(*), COUNT(DISTINCT seq) FROM ticket_events`).Scan(&rows, &distinct); err != nil {
		t.Fatalf("Zählen: %v", err)
	}
	if rows != n || distinct != n {
		t.Errorf("Zeilen = %d, verschiedene seq = %d, erwartet je %d", rows, distinct, n)
	}
}

// Read-Model beim Append (Anlass: DNS-Editor-Beta 2026-08-31 — vier
// adoptierte Zonen standen im Log, aber nie im Read-Model; der Worker auf
// einem anderen Knoten hatte den Checkpoint darueber hinweggeschoben).
// Seither schreibt Append die Zeile selbst, in derselben Transaktion.
func TestAppendProjectsInline(t *testing.T) {
	t.Parallel()
	db, ctx := esTestDB(t)
	// bewusst KEIN StartWorkers: die Zeile muss ohne Worker da sein

	tk := New[Ticket](db)
	pos, err := tk.Append(ctx, TicketOpened{Title: "Sofort sichtbar"})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	rows, err := Query[Ticket](db, ctx).Where(Eq("Title", "Sofort sichtbar")).All()
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(rows) != 1 || rows[0].Version() != 1 {
		t.Fatalf("Read-Model ohne Worker: %+v", rows)
	}

	// WaitFor darf nicht auf einen Worker warten, den es nicht gibt.
	started := time.Now()
	if _, err := Load[Ticket](ctx, db, tk.ID(), WaitFor(pos, 5*time.Second)); err != nil {
		t.Fatalf("WaitFor: %v", err)
	}
	if time.Since(started) > time.Second {
		t.Fatalf("WaitFor hat gewartet (%s) — die Zeile war bereits geschrieben", time.Since(started))
	}

	// Folge-Event: Zeile zieht mit, ebenfalls ohne Worker.
	if _, err := tk.Append(ctx, TicketClosed{}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	closed, err := Query[Ticket](db, ctx).Where(Eq("Status", "closed")).Count()
	if err != nil || closed != 1 {
		t.Fatalf("closed = %d (%v), erwartet 1", closed, err)
	}
}

// Der Worker faltet zu SEINER Lesezeit — ein aelterer Stand darf einen
// neueren nicht ueberschreiben.
func TestReadModelUpsertForwardOnly(t *testing.T) {
	t.Parallel()
	db, ctx := esTestDB(t)

	tk := New[Ticket](db)
	if _, err := tk.Append(ctx, TicketOpened{Title: "neu"}, TicketClosed{}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	m := db.reg.models[reflect.TypeFor[Ticket]()]
	stale := &Ticket{Title: "alt", Status: "open"}
	if err := db.upsertReadModel(ctx, db.q(), m, tk.ID().String(), SingleTenant, "local",
		reflect.ValueOf(stale).Elem(), 1); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	got, err := Query[Ticket](db, ctx).Where(Eq("Title", "neu")).First()
	if err != nil {
		t.Fatalf("Read-Model ueberschrieben: %v", err)
	}
	if got.Status != "closed" || got.Version() != 2 {
		t.Fatalf("Read-Model = %q v%d, erwartet closed v2", got.Status, got.Version())
	}
}

// Nachprojektion: was im Log steht und im Read-Model fehlt oder
// zurueckliegt, holt der Worker beim ersten Durchlauf nach.
func TestReconcileProjection(t *testing.T) {
	t.Parallel()
	db, ctx := esTestDB(t)

	missing := New[Ticket](db)
	if _, err := missing.Append(ctx, TicketOpened{Title: "verloren"}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	behind := New[Ticket](db)
	if _, err := behind.Append(ctx, TicketOpened{Title: "zurueck"}, TicketClosed{}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	// Den Schaden nachstellen: eine Zeile weg, eine auf dem alten Stand.
	q := db.q()
	if _, err := q.ExecContext(ctx, `DELETE FROM ticket WHERE id = ?`, missing.ID().String()); err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	if _, err := q.ExecContext(ctx, `UPDATE ticket SET status = 'open', aggregate_seq = 1 WHERE id = ?`, behind.ID().String()); err != nil {
		t.Fatalf("UPDATE: %v", err)
	}

	m := db.reg.models[reflect.TypeFor[Ticket]()]
	if err := db.reconcileProjection(ctx, m); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if _, err := Query[Ticket](db, ctx).Where(Eq("Title", "verloren")).First(); err != nil {
		t.Fatalf("fehlende Zeile nicht nachprojiziert: %v", err)
	}
	got, err := Query[Ticket](db, ctx).Where(Eq("Title", "zurueck")).First()
	if err != nil || got.Status != "closed" || got.Version() != 2 {
		t.Fatalf("zurueckliegende Zeile nicht nachgezogen: %+v (%v)", got, err)
	}
}
