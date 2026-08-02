package orm

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
)

// Die Tests laufen unverändert auf allen drei Backends: SQLite kollabiert
// die Regionen (die Deklaration bleibt gültig, nur ohne physische
// Wirkung), PG/YB partitionieren nativ. Was nur mit Partitionen prüfbar
// ist, hängt an partitioniert(db) — nicht am Backend-Namen.

func partitioniert(db *DB) bool { return db.dial.partitionClause() != "" }

// geoTestDB öffnet denselben Speicher mehrfach — so lässt sich eine
// zweite App-Generation mit erweiterter Topologie starten.
func geoTestDB(t *testing.T, store func() Driver, regions ...RegionDecl) (*DB, context.Context) {
	t.Helper()
	db, err := Open(store())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	Register[Ticket](db, EventSourced(), ticketEvents())
	Register[Note](db, CRUD())
	if len(regions) > 0 {
		Topology(db, regions...)
	}
	if err := db.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	return db, WithTenant(context.Background(), SingleTenant)
}

type Note struct {
	ID   ID     `orm:"pk"`
	Text string `orm:"required"`
}

// TestGeoPartitionenWerdenNachgezogen bildet den gemeldeten Fall ab: eine
// Anlage läuft erst ohne Topologie, die Ereignisse landen in der
// DEFAULT-Partition. Danach werden Regionen deklariert — Migrate muss die
// Partitionen nachlegen UND die liegengebliebenen Zeilen mitnehmen.
func TestGeoPartitionenWerdenNachgezogen(t *testing.T) {
	t.Parallel()
	bg := context.Background()
	store := newTestStore(t)

	db, ctx := geoTestDB(t, store) // keine Topologie ⇒ alles nach 'local'
	tk := New[Ticket](db)
	if _, err := tk.Append(ctx, TicketOpened{Title: "vor der Topologie"}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if _, err := tk.Append(ctx, NoteAdded{Note: "noch ohne Region"}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if partitioniert(db) {
		parts, err := db.dial.geoPartitions(db.q(), "ticket_events")
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := parts[geoDefaultRegion]; !ok || len(parts) != 1 {
			t.Fatalf("vor der Topologie erwartet: nur die DEFAULT-Partition, ist: %v", parts)
		}
		if n, err := db.countGeoRows(bg, "ticket_events_geo_default", "local"); err != nil || n != 2 {
			t.Fatalf("Zeilen in der DEFAULT-Partition = %d (%v)", n, err)
		}
	}
	db.Close()

	// Zweite Generation: dieselbe Anlage, jetzt mit drei Regionen. Die
	// Schema-Version bleibt gleich — der Abgleich darf davon nicht abhängen.
	db2, ctx2 := geoTestDB(t, store,
		Region("eu-central"), Region("eu-southwest"), Region("na"), Region("local"))

	if partitioniert(db2) {
		parts, err := db2.dial.geoPartitions(db2.q(), "ticket_events")
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range []string{"eu-central", "eu-southwest", "na", "local"} {
			if _, ok := parts[r]; !ok {
				t.Fatalf("Partition für Region %s fehlt nach Migrate: %v", r, parts)
			}
		}
		// Die zwei Altzeilen tragen geo='local' und müssen aus der
		// DEFAULT-Partition in die local-Partition umgezogen sein.
		if n, err := db2.countGeoRows(bg, "ticket_events_geo_default", "local"); err != nil || n != 0 {
			t.Fatalf("DEFAULT-Partition hält noch %d local-Zeilen (%v)", n, err)
		}
		if n, err := db2.countGeoRows(bg, "ticket_events_geo_local", "local"); err != nil || n != 2 {
			t.Fatalf("local-Partition hält %d Zeilen, erwartet 2 (%v)", n, err)
		}
	}

	// Entscheidend: die Ereignisse sind über den Umzug hinweg vollständig.
	loaded, err := Load[Ticket](ctx2, db2, tk.ID())
	if err != nil {
		t.Fatalf("Load nach Partitionsabgleich: %v", err)
	}
	if loaded.Title != "vor der Topologie" || len(loaded.Notes) != 1 {
		t.Fatalf("Aggregat nach Umzug: %+v", loaded)
	}
	if loaded.Version() != 2 {
		t.Fatalf("Version nach Umzug = %d, erwartet 2", loaded.Version())
	}
	// Und neue Regionen nehmen Schreibvorgänge an.
	tk2 := New[Ticket](db2)
	if _, err := tk2.Append(WithGeo(ctx2, "na"), TicketOpened{Title: "in na"}); err != nil {
		t.Fatalf("Append in neuer Region: %v", err)
	}
}

// TestMoveTenant prüft den Umzug eines Mandanten über alle Modelle:
// CRUD-Zeilen, Read-Model und Event-Log wandern mit, das Aggregat bleibt
// vollständig und die Geo-Sequenz der Zielregion bleibt kollisionsfrei.
func TestMoveTenant(t *testing.T) {
	t.Parallel()
	bg := context.Background()
	db, _ := geoTestDB(t, newTestStore(t), Region("eu-central"), Region("na"))

	umzugstenant, err := db.Tenants().Create(bg, TenantInfo{Name: "Umzug"})
	if err != nil {
		t.Fatal(err)
	}
	bleibt, err := db.Tenants().Create(bg, TenantInfo{Name: "Bleibt"})
	if err != nil {
		t.Fatal(err)
	}
	eu := WithGeo(WithTenant(bg, umzugstenant.ID), "eu-central")
	fremd := WithGeo(WithTenant(bg, bleibt.ID), "eu-central")

	// Der bleibende Mandant belegt in 'na' schon Geo-Sequenzen — der Umzug
	// darf nicht auf sie draufschreiben.
	vorbelegt := New[Ticket](db)
	if _, err := vorbelegt.Append(WithGeo(WithTenant(bg, bleibt.ID), "na"),
		TicketOpened{Title: "schon in na"}, NoteAdded{Note: "belegt seq 1+2"}); err != nil {
		t.Fatal(err)
	}

	tk := New[Ticket](db)
	if _, err := tk.Append(eu, TicketOpened{Title: "zieht um"}, NoteAdded{Note: "Kiste 1"}); err != nil {
		t.Fatal(err)
	}
	n := &Note{Text: "CRUD-Zeile"}
	if err := Repo[Note](db).Insert(eu, n); err != nil {
		t.Fatal(err)
	}
	nFremd := &Note{Text: "fremd"}
	if err := Repo[Note](db).Insert(fremd, nFremd); err != nil {
		t.Fatal(err)
	}
	if err := db.processProjection(bg, db.reg.byName["Ticket"]); err != nil {
		t.Fatal(err)
	}

	if err := db.MoveTenant(bg, umzugstenant.ID, "na"); err != nil {
		t.Fatalf("MoveTenant: %v", err)
	}

	// Alles des Mandanten liegt jetzt in 'na' …
	for _, q := range []struct {
		tabelle  string
		erwartet int
	}{{"ticket_events", 2}, {"ticket", 1}, {"note", 1}} {
		var got int
		if err := db.q().QueryRowContext(bg, fmt.Sprintf(
			`SELECT COUNT(*) FROM %q WHERE tenant_id = ? AND geo = ?`, q.tabelle),
			umzugstenant.ID.String(), "na").Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != q.erwartet {
			t.Fatalf("%s: %d Zeilen in na, erwartet %d", q.tabelle, got, q.erwartet)
		}
		var rest int
		if err := db.q().QueryRowContext(bg, fmt.Sprintf(
			`SELECT COUNT(*) FROM %q WHERE tenant_id = ? AND geo <> ?`, q.tabelle),
			umzugstenant.ID.String(), "na").Scan(&rest); err != nil {
			t.Fatal(err)
		}
		if rest != 0 {
			t.Fatalf("%s: %d Zeilen sind in der Altregion liegengeblieben", q.tabelle, rest)
		}
	}
	// … der fremde Mandant nicht.
	var fremdGeo string
	if err := db.q().QueryRowContext(bg, `SELECT geo FROM note WHERE id = ?`, nFremd.ID.String()).Scan(&fremdGeo); err != nil {
		t.Fatal(err)
	}
	if fremdGeo != "eu-central" {
		t.Fatalf("fremder Mandant wurde mitgezogen: geo=%q", fremdGeo)
	}

	// Das Aggregat ist vollständig und weiter fortschreibbar.
	nach := WithTenant(bg, umzugstenant.ID)
	loaded, err := Load[Ticket](nach, db, tk.ID())
	if err != nil {
		t.Fatalf("Load nach MoveTenant: %v", err)
	}
	if loaded.Title != "zieht um" || len(loaded.Notes) != 1 {
		t.Fatalf("Aggregat nach MoveTenant: %+v", loaded)
	}
	// Das Geo im Context bleibt Pflicht (fail-closed); das Pinning schreibt
	// die Ereignisse anschließend ohnehin in die Heimat 'na'.
	if _, err := loaded.Append(WithGeo(nach, "eu-central"), NoteAdded{Note: "nach dem Umzug"}); err != nil {
		t.Fatalf("Append nach MoveTenant: %v", err)
	}

	// Geo-Sequenz in 'na' bleibt eindeutig und lückenlos.
	rows, err := db.q().QueryContext(bg, `SELECT seq FROM ticket_events WHERE geo = ? ORDER BY seq`, "na")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var seqs []int64
	for rows.Next() {
		var s int64
		if err := rows.Scan(&s); err != nil {
			t.Fatal(err)
		}
		seqs = append(seqs, s)
	}
	if len(seqs) != 5 {
		t.Fatalf("seqs in na: %v, erwartet 5 Ereignisse", seqs)
	}
	for i, s := range seqs {
		if s != int64(i+1) {
			t.Fatalf("Geo-Sequenz in na nicht lückenlos: %v", seqs)
		}
	}

	// Wiederholbar: der zweite Aufruf findet nichts mehr zu tun.
	if err := db.MoveTenant(bg, umzugstenant.ID, "na"); err != nil {
		t.Fatalf("MoveTenant erneut: %v", err)
	}

	// Guards.
	if err := db.MoveTenant(bg, umzugstenant.ID, "mars"); !errors.Is(err, ErrRegionNotActive) {
		t.Fatalf("MoveTenant mars: %v", err)
	}
	if err := db.MoveTenant(bg, ID{}, "na"); !errors.Is(err, ErrNoTenant) {
		t.Fatalf("MoveTenant ohne Tenant: %v", err)
	}
}

// TestSetGeoAggregat: der Umzug eines einzelnen Datensatzes nimmt beim
// event-sourced Model den ganzen Event-Log mit — sonst risse das
// Geo-Pinning.
func TestSetGeoAggregat(t *testing.T) {
	t.Parallel()
	bg := context.Background()
	db, ctx := geoTestDB(t, newTestStore(t), Region("eu-central"), Region("na"))

	tk := New[Ticket](db)
	if _, err := tk.Append(WithGeo(ctx, "eu-central"),
		TicketOpened{Title: "Einzelumzug"}, NoteAdded{Note: "mitnehmen"}); err != nil {
		t.Fatal(err)
	}
	bleibt := New[Ticket](db)
	if _, err := bleibt.Append(WithGeo(ctx, "eu-central"), TicketOpened{Title: "bleibt"}); err != nil {
		t.Fatal(err)
	}
	if err := db.processProjection(bg, db.reg.byName["Ticket"]); err != nil {
		t.Fatal(err)
	}

	if err := Repo[Ticket](db).SetGeo(ctx, tk.ID(), "na"); err != nil {
		t.Fatalf("SetGeo auf Aggregat: %v", err)
	}
	var geo string
	if err := db.q().QueryRowContext(bg, `SELECT geo FROM ticket WHERE id = ?`, tk.ID().String()).Scan(&geo); err != nil {
		t.Fatal(err)
	}
	if geo != "na" {
		t.Fatalf("Read-Model geo=%q", geo)
	}
	var inNa, inEu int
	if err := db.q().QueryRowContext(bg,
		`SELECT COUNT(*) FROM ticket_events WHERE aggregate_id = ? AND geo = ?`, tk.ID().String(), "na").Scan(&inNa); err != nil {
		t.Fatal(err)
	}
	if err := db.q().QueryRowContext(bg,
		`SELECT COUNT(*) FROM ticket_events WHERE aggregate_id = ? AND geo = ?`, tk.ID().String(), "eu-central").Scan(&inEu); err != nil {
		t.Fatal(err)
	}
	if inNa != 2 || inEu != 0 {
		t.Fatalf("Event-Log nach SetGeo: na=%d eu=%d", inNa, inEu)
	}

	// Das andere Aggregat bleibt, wo es war.
	if err := db.q().QueryRowContext(bg, `SELECT geo FROM ticket WHERE id = ?`, bleibt.ID().String()).Scan(&geo); err != nil {
		t.Fatal(err)
	}
	if geo != "eu-central" {
		t.Fatalf("fremdes Aggregat mitgezogen: geo=%q", geo)
	}

	// Geo-Pinning greift danach auf die neue Heimat: ein Append mit
	// abweichendem Context-Geo landet trotzdem in 'na'.
	loaded, err := Load[Ticket](ctx, db, tk.ID())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := loaded.Append(WithGeo(ctx, "eu-central"), NoteAdded{Note: "folgt der Heimat"}); err != nil {
		t.Fatal(err)
	}
	if err := db.q().QueryRowContext(bg,
		`SELECT COUNT(*) FROM ticket_events WHERE aggregate_id = ? AND geo = ?`, tk.ID().String(), "na").Scan(&inNa); err != nil {
		t.Fatal(err)
	}
	if inNa != 3 {
		t.Fatalf("Geo-Pinning nach SetGeo: %d Ereignisse in na, erwartet 3", inNa)
	}
}

// TestSetGeoInTransaktion: der Umzug muss die laufende Transaktion
// benutzen. Eine eigene aufzumachen wäre auf SQLite (eine einzige
// Schreibverbindung) ein Deadlock und anderswo ein Schreibzugriff am
// Aufrufer vorbei — der Rollback unten würde ihn nicht zurücknehmen.
func TestSetGeoInTransaktion(t *testing.T) {
	t.Parallel()
	bg := context.Background()
	db, ctx := geoTestDB(t, newTestStore(t), Region("eu-central"), Region("na"))

	tk := New[Ticket](db)
	if _, err := tk.Append(WithGeo(ctx, "eu-central"), TicketOpened{Title: "in Tx"}); err != nil {
		t.Fatal(err)
	}

	gewollt := errors.New("Abbruch mit Absicht")
	err := db.Tx(ctx, func(tx Tx) error {
		if err := Repo[Ticket](tx).SetGeo(ctx, tk.ID(), "na"); err != nil {
			return err
		}
		return gewollt
	})
	if !errors.Is(err, gewollt) {
		t.Fatalf("Tx: %v", err)
	}
	// Rollback muss auch den Event-Log zurückgenommen haben.
	var geo string
	if err := db.q().QueryRowContext(bg,
		`SELECT geo FROM ticket_events WHERE aggregate_id = ?`, tk.ID().String()).Scan(&geo); err != nil {
		t.Fatal(err)
	}
	if geo != "eu-central" {
		t.Fatalf("Rollback hat den Umzug nicht zurückgenommen: geo=%q", geo)
	}
}

// TestPlacementMussExistieren: ORM++ legt keine Tablespaces an — ein
// deklariertes Placement, das es nicht gibt, muss Migrate stoppen statt
// stillschweigend ohne physische Wirkung durchzulaufen.
func TestPlacementMussExistieren(t *testing.T) {
	t.Parallel()
	db, err := Open(newTestStore(t)())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	Register[Ticket](db, EventSourced(), ticketEvents())
	Topology(db, Region("eu-central", Placement("ts_gibt_es_nicht")))

	err = db.Migrate(context.Background())
	if !partitioniert(db) {
		// SQLite kollabiert: die Deklaration bleibt gültig, ohne Wirkung.
		if err != nil {
			t.Fatalf("kollabiertes Backend darf am Placement nicht scheitern: %v", err)
		}
		return
	}
	if !errors.Is(err, ErrPlacementNotFound) {
		t.Fatalf("Migrate mit unbekanntem Placement: %v", err)
	}
	if !strings.Contains(err.Error(), "ts_gibt_es_nicht") {
		t.Fatalf("Fehler nennt den Tablespace nicht: %v", err)
	}
}

// TestRemoveRegion: eine Region verschwindet nicht stillschweigend,
// solange sie Daten hält.
func TestRemoveRegion(t *testing.T) {
	t.Parallel()
	bg := context.Background()
	store := newTestStore(t)
	db, ctx := geoTestDB(t, store, Region("eu-central"), Region("na"))

	tk := New[Ticket](db)
	if _, err := tk.Append(WithGeo(ctx, "na"), TicketOpened{Title: "in na"}); err != nil {
		t.Fatal(err)
	}
	// Solange die Region deklariert ist, ist RemoveRegion die falsche
	// Reihenfolge — sonst legte der nächste Migrate sie sofort wieder an.
	if err := db.RemoveRegion(bg, "na"); err == nil || !strings.Contains(err.Error(), "Topology") {
		t.Fatalf("RemoveRegion bei deklarierter Region: %v", err)
	}
	db.Close()

	// Neue Generation ohne 'na': jetzt greift der Datenschutz.
	db2, _ := geoTestDB(t, store, Region("eu-central"))
	err := db2.RemoveRegion(bg, "na")
	if !errors.Is(err, ErrRegionHasData) {
		t.Fatalf("RemoveRegion mit Daten: %v", err)
	}
	if !strings.Contains(err.Error(), "ticket_events") {
		t.Fatalf("Fehler nennt die Tabelle nicht: %v", err)
	}

	// Nach dem Umzug der Daten geht es.
	if err := Repo[Ticket](db2).SetGeo(WithTenant(bg, SingleTenant), tk.ID(), "eu-central"); err != nil {
		t.Fatal(err)
	}
	if err := db2.RemoveRegion(bg, "na"); err != nil {
		t.Fatalf("RemoveRegion nach Umzug: %v", err)
	}
	var status string
	if err := db2.q().QueryRowContext(bg,
		`SELECT status FROM ormpp_geo_regions WHERE name = ?`, "na").Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "removed" {
		t.Fatalf("Registerstatus nach RemoveRegion: %q", status)
	}
	if partitioniert(db2) {
		parts, err := db2.dial.geoPartitions(db2.q(), "ticket_events")
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := parts["na"]; ok {
			t.Fatalf("Partition der Region na besteht weiter: %v", parts)
		}
	}
}

// TestGeoResidenzPhysisch ist der eigentliche Nachweis: er misst nicht den
// Spaltenwert, sondern wo die Tablets liegen. Er braucht einen echten
// Mehr-Knoten-Cluster mit mindestens zwei Platzierungen und zwei
// vorbereitete Tablespaces:
//
//	ORMPP_TEST_BACKEND=yugabyte ORMPP_TEST_DSN=… \
//	ORMPP_TEST_PLACEMENTS="eu-central=ts_eu_central,na=ts_na" go test -run TestGeoResidenzPhysisch
//
// Aufbau des Clusters und der Tablespaces: docker-compose.geo.yml.
func TestGeoResidenzPhysisch(t *testing.T) {
	t.Parallel()
	spec := os.Getenv("ORMPP_TEST_PLACEMENTS")
	if spec == "" {
		t.Skip("ORMPP_TEST_PLACEMENTS nicht gesetzt — braucht einen Cluster mit mehreren Platzierungen")
	}
	bg := context.Background()
	var regionen []RegionDecl
	var namen []string
	for _, teil := range strings.Split(spec, ",") {
		r, ts, ok := strings.Cut(strings.TrimSpace(teil), "=")
		if !ok {
			t.Fatalf("ORMPP_TEST_PLACEMENTS: %q ist kein region=tablespace", teil)
		}
		regionen = append(regionen, Region(r, Placement(ts)))
		namen = append(namen, r)
	}
	if len(regionen) < 2 {
		t.Fatalf("mindestens zwei Platzierungen nötig, bekommen: %d", len(regionen))
	}
	db, ctx := geoTestDB(t, newTestStore(t), regionen...)
	if !partitioniert(db) {
		t.Skip("Backend partitioniert nicht")
	}

	for _, r := range namen {
		tk := New[Ticket](db)
		if _, err := tk.Append(WithGeo(ctx, r), TicketOpened{Title: "residiert in " + r}); err != nil {
			t.Fatalf("Append in %s: %v", r, err)
		}
		// CRUD-Zeilen residieren genauso — Kernfähigkeit, nicht ES-Sonderfall.
		if err := Repo[Note](db).Insert(WithGeo(ctx, r), &Note{Text: "residiert in " + r}); err != nil {
			t.Fatalf("Insert in %s: %v", r, err)
		}
	}

	// 1. Die Partitionen hängen am deklarierten Tablespace — Event-Log
	// UND CRUD-Tabelle.
	for _, tbl := range []string{"ticket_events", "note"} {
		parts, err := db.dial.geoPartitions(db.q(), tbl)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range regionen {
			if parts[r.name] != r.placement {
				t.Fatalf("%s: Partition der Region %s liegt in %q, deklariert war %q", tbl, r.name, parts[r.name], r.placement)
			}
		}
	}

	// 2. Und der Tablespace hält die Tablets tatsächlich nur in seiner
	// Platzierung. yb_local_tablets zeigt je Knoten die dort liegenden
	// Tablets — die Partition einer Region darf nur auf deren Knoten
	// auftauchen, die ungebundene DEFAULT-Partition dagegen überall.
	rows, err := db.q().QueryContext(bg, `
		SELECT t.table_name, count(*)
		FROM yb_local_tablets t
		WHERE t.table_name LIKE 'ticket_events_geo%' OR t.table_name LIKE 'note_geo%'
		GROUP BY t.table_name ORDER BY t.table_name`)
	if err != nil {
		t.Skipf("yb_local_tablets nicht verfügbar (kein YugabyteDB?): %v", err)
	}
	defer rows.Close()
	lokal := map[string]int{}
	for rows.Next() {
		var name string
		var n int
		if err := rows.Scan(&name, &n); err != nil {
			t.Fatal(err)
		}
		lokal[name] = n
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	// Der Test verbindet sich mit EINEM Knoten. Genau eine der
	// regionsgebundenen Partitionen je Tabelle darf hier liegen — läge
	// jede hier, wäre das Placement wirkungslos.
	for _, tbl := range []string{"ticket_events", "note"} {
		var hier []string
		for _, r := range regionen {
			if lokal[geoPartName(tbl, r.name)] > 0 {
				hier = append(hier, r.name)
			}
		}
		if len(hier) == 0 {
			t.Fatalf("%s: keine regionsgebundene Partition auf diesem Knoten: %v", tbl, lokal)
		}
		if len(hier) == len(regionen) {
			t.Fatalf("%s: alle Regionspartitionen liegen auf demselben Knoten (%v) — das Placement wirkt nicht: %v", tbl, hier, lokal)
		}
		t.Logf("%s: auf diesem Knoten liegen die Partitionen der Regionen %v (von %v)", tbl, hier, namen)
	}
}

// --- Geo-Residenz als Kernfähigkeit: CRUD-Tabellen ---

type GeoFirma struct {
	ID   ID     `orm:"pk"`
	Name string `orm:"unique,required"`
}

type GeoVertrag struct {
	ID      ID     `orm:"pk"`
	FirmaID ID     `orm:"ref=GeoFirma,ondelete=cascade,required"`
	Nummer  string `orm:"required"`
}

type GeoRechnung struct {
	ID        ID `orm:"pk"`
	VertragID ID `orm:"ref=GeoVertrag,ondelete=cascade,required"`
}

type GeoKontakt struct {
	ID      ID  `orm:"pk"`
	FirmaID *ID `orm:"ref=GeoFirma,ondelete=setnull"`
}

type GeoAuditor struct {
	ID      ID `orm:"pk"`
	FirmaID ID `orm:"ref=GeoFirma"` // ondelete-Default: restrict
}

func geoCRUDTestDB(t *testing.T, store func() Driver, regions ...RegionDecl) (*DB, context.Context) {
	t.Helper()
	db, err := Open(store())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	Register[GeoFirma](db, CRUD())
	Register[GeoVertrag](db, CRUD())
	Register[GeoRechnung](db, CRUD())
	Register[GeoKontakt](db, CRUD())
	Register[GeoAuditor](db, CRUD())
	Register[Ticket](db, EventSourced(), ticketEvents())
	if len(regions) > 0 {
		Topology(db, regions...)
	}
	if err := db.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	return db, WithTenant(context.Background(), SingleTenant)
}

// TestCRUDGeoPartitioniert: die Kernfähigkeit — CRUD-Zeilen liegen in der
// Partition ihrer Region, und die Constraint-Semantik (unique, ref,
// ondelete) bleibt über Partitionsgrenzen hinweg identisch zu vorher.
func TestCRUDGeoPartitioniert(t *testing.T) {
	t.Parallel()
	bg := context.Background()
	db, ctx := geoCRUDTestDB(t, newTestStore(t), Region("eu-central"), Region("na"))
	eu, na := WithGeo(ctx, "eu-central"), WithGeo(ctx, "na")

	fEU := &GeoFirma{Name: "ACME"}
	if err := Repo[GeoFirma](db).Insert(eu, fEU); err != nil {
		t.Fatal(err)
	}
	fNA := &GeoFirma{Name: "Globex"}
	if err := Repo[GeoFirma](db).Insert(na, fNA); err != nil {
		t.Fatal(err)
	}

	if partitioniert(db) {
		// Die Zeilen liegen physisch in den Partitionen ihrer Region.
		if n, err := db.countGeoRows(bg, "geo_firma_geo_eu-central", "eu-central"); err != nil || n != 1 {
			t.Fatalf("eu-Partition: %d Zeilen (%v)", n, err)
		}
		if n, err := db.countGeoRows(bg, "geo_firma_geo_na", "na"); err != nil || n != 1 {
			t.Fatalf("na-Partition: %d Zeilen (%v)", n, err)
		}
	}

	// Unique gilt über ALLE Regionen — nicht nur pro Partition.
	if err := Repo[GeoFirma](db).Insert(na, &GeoFirma{Name: "ACME"}); !errors.Is(err, ErrUniqueConflict) {
		t.Fatalf("Cross-Geo-Unique: %v", err)
	}
	dup := &GeoFirma{Name: "Globex"}
	dup.ID = NewID()
	if err := Repo[GeoFirma](db).Update(eu, &GeoFirma{ID: fEU.ID, Name: "Globex"}); !errors.Is(err, ErrUniqueConflict) {
		t.Fatalf("Cross-Geo-Unique bei Update: %v", err)
	}

	// Referenz auf eine partitionierte Tabelle: engine-geprüft.
	v := &GeoVertrag{FirmaID: fEU.ID, Nummer: "V-1"}
	if err := Repo[GeoVertrag](db).Insert(eu, v); err != nil {
		t.Fatal(err)
	}
	if err := Repo[GeoVertrag](db).Insert(eu, &GeoVertrag{FirmaID: NewID(), Nummer: "V-2"}); !errors.Is(err, ErrInvalidReference) {
		t.Fatalf("Referenz auf fehlende Firma: %v", err)
	}

	// restrict blockiert, solange ein Auditor auf die Firma zeigt.
	a := &GeoAuditor{FirmaID: fEU.ID}
	if err := Repo[GeoAuditor](db).Insert(eu, a); err != nil {
		t.Fatal(err)
	}
	if err := Repo[GeoFirma](db).Delete(ctx, fEU.ID); !errors.Is(err, ErrReferenceInUse) {
		t.Fatalf("restrict: %v", err)
	}
	if err := Repo[GeoAuditor](db).Delete(ctx, a.ID); err != nil {
		t.Fatal(err)
	}

	// setnull + zweistufige Kaskade (Firma -> Vertrag -> Rechnung).
	k := &GeoKontakt{FirmaID: &fEU.ID}
	if err := Repo[GeoKontakt](db).Insert(eu, k); err != nil {
		t.Fatal(err)
	}
	re := &GeoRechnung{VertragID: v.ID}
	if err := Repo[GeoRechnung](db).Insert(eu, re); err != nil {
		t.Fatal(err)
	}
	if err := Repo[GeoFirma](db).Delete(ctx, fEU.ID); err != nil {
		t.Fatalf("Delete mit Emulation: %v", err)
	}
	if _, err := Repo[GeoVertrag](db).Get(ctx, v.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Kaskade Stufe 1: %v", err)
	}
	if _, err := Repo[GeoRechnung](db).Get(ctx, re.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Kaskade Stufe 2: %v", err)
	}
	kNach, err := Repo[GeoKontakt](db).Get(ctx, k.ID)
	if err != nil {
		t.Fatalf("setnull-Kontakt weg: %v", err)
	}
	if kNach.FirmaID != nil {
		t.Fatalf("setnull: FirmaID = %v, erwartet nil", kNach.FirmaID)
	}
}

// TestUpsertNachUmzug: der Upsert findet einen umgezogenen Datensatz in
// seiner neuen Region — ein ON CONFLICT träfe nur das Context-Geo und
// legte ihn doppelt an.
func TestUpsertNachUmzug(t *testing.T) {
	t.Parallel()
	bg := context.Background()
	db, ctx := geoCRUDTestDB(t, newTestStore(t), Region("eu-central"), Region("na"))
	eu := WithGeo(ctx, "eu-central")

	f := &GeoFirma{Name: "Initech"}
	if err := Repo[GeoFirma](db).Upsert(eu, f); err != nil { // Insert-Zweig
		t.Fatal(err)
	}
	if err := Repo[GeoFirma](db).SetGeo(ctx, f.ID, "na"); err != nil {
		t.Fatal(err)
	}
	f.Name = "Initech GmbH"
	if err := Repo[GeoFirma](db).Upsert(eu, f); err != nil { // Update-Zweig, Context zeigt auf die ALTE Region
		t.Fatal(err)
	}
	var n int
	if err := db.q().QueryRowContext(bg, `SELECT COUNT(*) FROM geo_firma WHERE id = ?`, f.ID.String()).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("Upsert nach Umzug: %d Zeilen, erwartet 1", n)
	}
	var geo, name string
	if err := db.q().QueryRowContext(bg, `SELECT geo, name FROM geo_firma WHERE id = ?`, f.ID.String()).Scan(&geo, &name); err != nil {
		t.Fatal(err)
	}
	if geo != "na" || name != "Initech GmbH" {
		t.Fatalf("Upsert nach Umzug: geo=%q name=%q", geo, name)
	}
}

// TestBestandsUmbau bildet das Upgrade einer echten Anlage ab: die Tabellen
// entstanden vor der Topologie (bzw. vor dieser Version) unpartitioniert,
// Migrate überführt sie in die partitionierte Form — ohne Datenverlust,
// mit intakter Constraint-Semantik und wiederhergestellter Snapshot-Geo.
func TestBestandsUmbau(t *testing.T) {
	t.Parallel()
	bg := context.Background()
	store := newTestStore(t)

	// Generation 1: ohne Topologie — auf PG/YB entstehen normale Tabellen.
	db1, ctx1 := geoCRUDTestDB(t, store)
	f := &GeoFirma{Name: "Legacy AG"}
	if err := Repo[GeoFirma](db1).Insert(ctx1, f); err != nil {
		t.Fatal(err)
	}
	v := &GeoVertrag{FirmaID: f.ID, Nummer: "V-alt"}
	if err := Repo[GeoVertrag](db1).Insert(ctx1, v); err != nil {
		t.Fatal(err)
	}
	tk := New[Ticket](db1)
	if _, err := tk.Append(ctx1, TicketOpened{Title: "Altbestand"}, NoteAdded{Note: "vor dem Umbau"}); err != nil {
		t.Fatal(err)
	}
	if err := db1.snapshotAggregate(bg, db1.reg.byName["Ticket"], tk.ID().String(), SingleTenant); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	// Bestands-Form simulieren: Snapshots älterer Versionen hatten keine
	// geo-Spalte — Migrate muss sie ergänzen und aus dem Log füllen.
	if _, err := db1.q().ExecContext(bg, `ALTER TABLE "ticket_snapshots" DROP COLUMN "geo"`); err != nil {
		t.Fatalf("geo-Spalte entfernen: %v", err)
	}
	db1.Close()

	// Generation 2: Topologie deklariert — Migrate baut um.
	db2, ctx2 := geoCRUDTestDB(t, store, Region("local"), Region("na"))

	if partitioniert(db2) {
		for _, tbl := range []string{"geo_firma", "geo_vertrag", "ticket", "ticket_events_archive", "ticket_snapshots"} {
			kind, err := db2.dial.tableKind(db2.q(), tbl)
			if err != nil {
				t.Fatal(err)
			}
			if kind != 'p' {
				t.Fatalf("%s: relkind %q, erwartet 'p'", tbl, kind)
			}
		}
		// Keine Umbau-Reste.
		for _, tbl := range []string{"geo_firma_vorgeo", "ticket_snapshots_vorgeo"} {
			if kind, _ := db2.dial.tableKind(db2.q(), tbl); kind != 0 {
				t.Fatalf("Umbau-Rest %s existiert noch", tbl)
			}
		}
	}

	// Daten vollständig, Semantik intakt.
	fNach, err := Repo[GeoFirma](db2).Get(ctx2, f.ID)
	if err != nil || fNach.Name != "Legacy AG" {
		t.Fatalf("Firma nach Umbau: %+v (%v)", fNach, err)
	}
	if err := Repo[GeoFirma](db2).Insert(WithGeo(ctx2, "na"), &GeoFirma{Name: "Legacy AG"}); !errors.Is(err, ErrUniqueConflict) {
		t.Fatalf("Unique nach Umbau: %v", err)
	}
	loaded, err := Load[Ticket](ctx2, db2, tk.ID())
	if err != nil || loaded.Title != "Altbestand" || len(loaded.Notes) != 1 {
		t.Fatalf("Aggregat nach Umbau: %+v (%v)", loaded, err)
	}
	// Snapshot-Geo ergänzt und aus dem Event-Log gefüllt.
	var snGeo string
	if err := db2.q().QueryRowContext(bg,
		`SELECT geo FROM ticket_snapshots WHERE aggregate_id = ?`, tk.ID().String()).Scan(&snGeo); err != nil {
		t.Fatalf("Snapshot-Geo: %v", err)
	}
	if snGeo != "local" {
		t.Fatalf("Snapshot-Geo = %q, erwartet local", snGeo)
	}
	// Kaskade über die umgebauten (FK-freien) Tabellen.
	if err := Repo[GeoFirma](db2).Delete(ctx2, f.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := Repo[GeoVertrag](db2).Get(ctx2, v.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Kaskade nach Umbau: %v", err)
	}
	// Und die neue Region nimmt Schreibvorgänge an.
	if err := Repo[GeoFirma](db2).Insert(WithGeo(ctx2, "na"), &GeoFirma{Name: "Neu GmbH"}); err != nil {
		t.Fatal(err)
	}
}

// TestSnapshotResidiertBeimAggregat: der Snapshot ist derselbe Zustand wie
// das Read-Model und trägt dessen Region — auch nach einem Umzug.
func TestSnapshotResidiertBeimAggregat(t *testing.T) {
	t.Parallel()
	bg := context.Background()
	db, ctx := geoTestDB(t, newTestStore(t), Region("eu-central"), Region("na"))

	tk := New[Ticket](db)
	if _, err := tk.Append(WithGeo(ctx, "eu-central"), TicketOpened{Title: "Snap"}); err != nil {
		t.Fatal(err)
	}
	m := db.reg.byName["Ticket"]
	if err := db.snapshotAggregate(bg, m, tk.ID().String(), SingleTenant); err != nil {
		t.Fatal(err)
	}
	var geo string
	if err := db.q().QueryRowContext(bg,
		`SELECT geo FROM ticket_snapshots WHERE aggregate_id = ?`, tk.ID().String()).Scan(&geo); err != nil {
		t.Fatal(err)
	}
	if geo != "eu-central" {
		t.Fatalf("Snapshot-Geo = %q", geo)
	}

	if err := Repo[Ticket](db).SetGeo(ctx, tk.ID(), "na"); err != nil {
		t.Fatal(err)
	}
	if err := db.q().QueryRowContext(bg,
		`SELECT geo FROM ticket_snapshots WHERE aggregate_id = ?`, tk.ID().String()).Scan(&geo); err != nil {
		t.Fatal(err)
	}
	if geo != "na" {
		t.Fatalf("Snapshot-Geo nach SetGeo = %q, erwartet na", geo)
	}
	// Und der Snapshot bleibt nutzbar: Load läuft über Snapshot + Log.
	loaded, err := Load[Ticket](ctx, db, tk.ID())
	if err != nil || loaded.Title != "Snap" {
		t.Fatalf("Load nach Snapshot-Umzug: %+v (%v)", loaded, err)
	}
}
