package orm

import (
	"context"
	"testing"
	"time"
)

func TestMigrationStatusLifecycle(t *testing.T) {
	store := newTestStore(t)
	bg := context.Background()

	type Part struct {
		ID   ID     `orm:"pk"`
		Name string `orm:"required"`
	}
	db1, err := Open(store())
	if err != nil {
		t.Fatal(err)
	}
	Register[Part](db1, CRUD())
	SchemaVersion(db1, 1)
	if err := db1.Migrate(bg); err != nil {
		t.Fatal(err)
	}
	st, err := db1.MigrationStatus(bg)
	if err != nil || st.Phase != phaseIdle || st.CurrentVersion != 1 || st.TargetVersion != 0 {
		t.Fatalf("Status idle: %+v (%v)", st, err)
	}
	db1.Close()

	// v2 mit einem Schritt → Dual-Write; Fortschritt 100 %.
	db2, err := Open(store(), MigrationRole(MigrationWorker))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db2.Close() })
	Register[Part](db2, CRUD())
	SchemaVersion(db2, 2)
	MigrationTo(db2, 2, BatchScript("noop", func(ctx context.Context, b Batch) error { return nil }))
	if err := db2.Migrate(bg); err != nil {
		t.Fatal(err)
	}
	st, err = db2.MigrationStatus(bg)
	if err != nil || st.Phase != phaseDualWrite || st.CurrentVersion != 1 || st.TargetVersion != 2 {
		t.Fatalf("Status dual-write: %+v (%v)", st, err)
	}
	local := st.Geo["local"]
	if local.Percent != 100 {
		t.Fatalf("Percent = %v, erwartet 100", local.Percent)
	}
	if local.Workers != 1 {
		t.Fatalf("Workers = %d, erwartet 1 (MigrationWorker-Rolle)", local.Workers)
	}
	if err := db2.FinalizeMigration(bg, 2); err != nil {
		t.Fatal(err)
	}
	st, _ = db2.MigrationStatus(bg)
	if st.Phase != phaseIdle || st.CurrentVersion != 2 || st.TargetVersion != 0 {
		t.Fatalf("Status nach Finalize: %+v", st)
	}
}

func TestHealthInstancesLagAndRegions(t *testing.T) {
	db, ctx := esTestDB(t)
	bg := context.Background()

	// Event angehängt, Projektion noch nicht gelaufen ⇒ Lag 1.
	tk := New[Ticket](db)
	if _, err := tk.Append(ctx, TicketOpened{Title: "Lag"}); err != nil {
		t.Fatal(err)
	}
	h, err := db.Health(bg)
	if err != nil {
		t.Fatalf("Health: %v", err)
	}
	if len(h.Instances) != 1 || !h.Instances[0].Alive || h.Instances[0].SchemaVersion != 1 {
		t.Fatalf("Instances: %+v", h.Instances)
	}
	if h.Instances[0].LastHeartbeat.After(time.Now().Add(time.Minute)) {
		t.Fatal("Heartbeat in der Zukunft")
	}
	found := false
	for _, p := range h.Projections {
		if p.Consumer == "projection:ticket" && p.Geo == "local" {
			found = true
			if p.Lag != 1 {
				t.Fatalf("Lag = %d, erwartet 1", p.Lag)
			}
		}
	}
	if !found {
		t.Fatalf("projection:ticket fehlt: %+v", h.Projections)
	}
	// Ohne Topologie: die eine ehrliche Region.
	if len(h.Regions) != 1 || h.Regions[0].Name != "local" || h.Regions[0].Status != "active" {
		t.Fatalf("Regions: %+v", h.Regions)
	}

	// Projektion aufholen lassen ⇒ Lag 0.
	if err := db.processProjection(bg, db.reg.byName["Ticket"]); err != nil {
		t.Fatal(err)
	}
	h, _ = db.Health(bg)
	for _, p := range h.Projections {
		if p.Consumer == "projection:ticket" && p.Lag != 0 {
			t.Fatalf("Lag nach Projektion = %d, erwartet 0", p.Lag)
		}
	}
}
