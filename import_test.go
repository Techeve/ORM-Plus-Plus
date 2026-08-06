package orm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

// importTestDB registriert ein ES-Model (Ticket) und ein CRUD-Model mit
// verschlüsseltem Feld (Vault) — der Export deckt damit alle Satztypen ab.
func importTestDB(t *testing.T, store func() Driver, key byte, regions ...RegionDecl) *DB {
	t.Helper()
	db, err := Open(store(), Encryption(StaticKey(testKey(key))))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	Register[Ticket](db, EventSourced(), ticketEvents())
	Register[Vault](db, CRUD())
	if len(regions) > 0 {
		Topology(db, regions...)
	}
	if err := db.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	return db
}

// exportZaehlen liefert die Satzzahlen je Typ/Model eines Export-Stroms.
func exportZaehlen(t *testing.T, strom []byte) map[string]int {
	t.Helper()
	out := map[string]int{}
	dec := json.NewDecoder(bytes.NewReader(strom))
	for {
		var rec importRecord
		if err := dec.Decode(&rec); err != nil {
			break
		}
		schluessel := rec.Type
		if rec.Model != "" {
			schluessel += ":" + rec.Model
		}
		out[schluessel]++
	}
	return out
}

// TestImportRoundTrip: Export → Tenant leeren → Import → erneuter Export
// ist inhaltlich gleich, und das verschlüsselte Feld liegt danach wieder
// verschlüsselt in der Datenbank (neu verschlüsselt mit dem aktuellen
// Schlüssel, nicht im Klartext aus dem Strom übernommen).
func TestImportRoundTrip(t *testing.T) {
	t.Parallel()
	bg := context.Background()
	db := importTestDB(t, newTestStore(t), 1)

	mandant, err := db.Tenants().Create(bg, TenantInfo{Name: "Sicherung"})
	if err != nil {
		t.Fatal(err)
	}
	ctx := WithTenant(bg, mandant.ID)

	v := &Vault{Name: "prod", APIKey: "streng-geheim", Blob: []byte{1, 2, 3}}
	if err := Repo[Vault](db).Insert(ctx, v); err != nil {
		t.Fatal(err)
	}
	tk := New[Ticket](db)
	if _, err := tk.Append(ctx, TicketOpened{Title: "Vorfall"}, NoteAdded{Note: "erste Notiz"}); err != nil {
		t.Fatal(err)
	}
	if err := db.processProjection(bg, db.reg.byName["Ticket"]); err != nil {
		t.Fatal(err)
	}

	var vorher bytes.Buffer
	if err := db.Tenants().Export(bg, mandant.ID, &vorher); err != nil {
		t.Fatalf("Export: %v", err)
	}

	// Archiviert ⇒ Import ersetzt den Bestand.
	if err := db.Tenants().Archive(bg, mandant.ID); err != nil {
		t.Fatal(err)
	}
	if err := db.Tenants().Import(bg, mandant.ID, bytes.NewReader(vorher.Bytes())); err != nil {
		t.Fatalf("Import: %v", err)
	}

	info, err := db.Tenants().Get(bg, mandant.ID)
	if err != nil || info.Status != "active" {
		t.Fatalf("Status nach Import: %q (%v)", info.Status, err)
	}

	var nachher bytes.Buffer
	if err := db.Tenants().Export(bg, mandant.ID, &nachher); err != nil {
		t.Fatalf("Export nach Import: %v", err)
	}
	a, b := exportZaehlen(t, vorher.Bytes()), exportZaehlen(t, nachher.Bytes())
	for k, want := range a {
		if b[k] != want {
			t.Fatalf("Satzzahl %s: vorher %d, nachher %d (%v)", k, want, b[k], b)
		}
	}
	if len(a) != len(b) {
		t.Fatalf("Satztypen unterschiedlich: vorher %v, nachher %v", a, b)
	}

	// Stichproben: Klartext ist zurück …
	got, err := Repo[Vault](db).Get(ctx, v.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.APIKey != "streng-geheim" || string(got.Blob) != "\x01\x02\x03" || got.Name != "prod" {
		t.Fatalf("Vault nach Import: %+v", got)
	}
	// … liegt in der Datenbank aber verschlüsselt.
	var roh []byte
	if err := db.q().QueryRowContext(bg, `SELECT api_key FROM vault WHERE id = ?`, v.ID.String()).Scan(&roh); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(roh, []byte("streng-geheim")) {
		t.Fatalf("api_key liegt im Klartext in der Datenbank: %q", roh)
	}

	// Das Aggregat ist vollständig, das Read-Model neu projiziert.
	geladen, err := Load[Ticket](ctx, db, tk.ID())
	if err != nil {
		t.Fatalf("Load nach Import: %v", err)
	}
	if geladen.Title != "Vorfall" || len(geladen.Notes) != 1 || geladen.Version() != 2 {
		t.Fatalf("Aggregat nach Import: %+v (v%d)", geladen, geladen.Version())
	}
	var titel string
	if err := db.q().QueryRowContext(bg, `SELECT title FROM ticket WHERE id = ?`, tk.ID().String()).Scan(&titel); err != nil {
		t.Fatalf("Read-Model nach Import: %v", err)
	}
	if titel != "Vorfall" {
		t.Fatalf("Read-Model title = %q", titel)
	}
}

// TestImportWeiterschreiben: nach dem Import setzt ein Append nahtlos fort —
// keine Sequenzkollision, WaitFor und Projektion arbeiten wie auf einem
// gewachsenen Bestand.
func TestImportWeiterschreiben(t *testing.T) {
	t.Parallel()
	bg := context.Background()
	db := importTestDB(t, newTestStore(t), 2)

	mandant, err := db.Tenants().Create(bg, TenantInfo{Name: "Weiter"})
	if err != nil {
		t.Fatal(err)
	}
	ctx := WithTenant(bg, mandant.ID)

	// Ein zweiter Mandant belegt die Geo-Sequenz vor — der Import darf
	// nicht auf seine Nummern schreiben.
	fremd, err := db.Tenants().Create(bg, TenantInfo{Name: "Fremd"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := New[Ticket](db).Append(WithTenant(bg, fremd.ID), TicketOpened{Title: "fremd"}); err != nil {
		t.Fatal(err)
	}

	tk := New[Ticket](db)
	if _, err := tk.Append(ctx, TicketOpened{Title: "Alt"}, NoteAdded{Note: "eins"}); err != nil {
		t.Fatal(err)
	}
	var strom bytes.Buffer
	if err := db.Tenants().Export(bg, mandant.ID, &strom); err != nil {
		t.Fatal(err)
	}
	if err := db.Tenants().Archive(bg, mandant.ID); err != nil {
		t.Fatal(err)
	}
	if err := db.Tenants().Import(bg, mandant.ID, bytes.NewReader(strom.Bytes())); err != nil {
		t.Fatalf("Import: %v", err)
	}

	geladen, err := Load[Ticket](ctx, db, tk.ID())
	if err != nil {
		t.Fatal(err)
	}
	pos, err := geladen.Append(ctx, NoteAdded{Note: "nach dem Import"})
	if err != nil {
		t.Fatalf("Append nach Import: %v", err)
	}
	if err := db.processProjection(bg, db.reg.byName["Ticket"]); err != nil {
		t.Fatal(err)
	}
	frisch, err := Load[Ticket](ctx, db, tk.ID(), WaitFor(pos, 5*time.Second))
	if err != nil {
		t.Fatalf("Load mit WaitFor: %v", err)
	}
	if len(frisch.Notes) != 2 || frisch.Version() != 3 {
		t.Fatalf("nach dem Weiterschreiben: %+v (v%d)", frisch, frisch.Version())
	}

	// Die Geo-Sequenz bleibt eindeutig und lückenlos — über beide Mandanten.
	rows, err := db.q().QueryContext(bg, `SELECT seq FROM ticket_events ORDER BY seq`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var i int64
	for rows.Next() {
		var s int64
		if err := rows.Scan(&s); err != nil {
			t.Fatal(err)
		}
		i++
		if s != i {
			t.Fatalf("Geo-Sequenz nach Import nicht lückenlos: %d an Position %d", s, i)
		}
	}
	if i != 4 {
		t.Fatalf("%d Events insgesamt, erwartet 4", i)
	}
}

// TestImportAbbruch: ein abgeschnittener Strom hinterlässt keinen stillen
// Halb-Zustand. Der Tenant bleibt erkennbar unvollständig, Schreibzugriffe
// scheitern, und ein Wiederholen mit dem vollständigen Strom führt zum
// korrekten Endstand.
func TestImportAbbruch(t *testing.T) {
	t.Parallel()
	bg := context.Background()
	db := importTestDB(t, newTestStore(t), 3)

	mandant, err := db.Tenants().Create(bg, TenantInfo{Name: "Abbruch"})
	if err != nil {
		t.Fatal(err)
	}
	ctx := WithTenant(bg, mandant.ID)
	for _, name := range []string{"a", "b", "c"} {
		if err := Repo[Vault](db).Insert(ctx, &Vault{Name: name, APIKey: "k-" + name}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := New[Ticket](db).Append(ctx, TicketOpened{Title: "Vorfall"}); err != nil {
		t.Fatal(err)
	}
	var strom bytes.Buffer
	if err := db.Tenants().Export(bg, mandant.ID, &strom); err != nil {
		t.Fatal(err)
	}

	zeilen := strings.SplitAfter(strom.String(), "\n")
	if len(zeilen) < 5 {
		t.Fatalf("Export hat nur %d Zeilen", len(zeilen))
	}
	halb := strings.Join(zeilen[:len(zeilen)/2], "")

	if err := db.Tenants().Archive(bg, mandant.ID); err != nil {
		t.Fatal(err)
	}
	err = db.Tenants().Import(bg, mandant.ID, strings.NewReader(halb))
	if !errors.Is(err, ErrImportIncomplete) {
		t.Fatalf("abgeschnittener Strom: %v, erwartet ErrImportIncomplete", err)
	}

	// Erkennbar unvollständig — und kein Schreibzugriff kommt durch.
	info, err := db.Tenants().Get(bg, mandant.ID)
	if err != nil {
		t.Fatal(err)
	}
	if info.Status != "importing" {
		t.Fatalf("Status nach Abbruch: %q, erwartet importing", info.Status)
	}
	if err := Repo[Vault](db).Insert(ctx, &Vault{Name: "d", APIKey: "k-d"}); !errors.Is(err, ErrImportIncomplete) {
		t.Fatalf("Schreibzugriff auf unvollständigen Tenant: %v", err)
	}

	// Wiederholung mit dem vollständigen Strom führt zum korrekten Endstand.
	if err := db.Tenants().Import(bg, mandant.ID, bytes.NewReader(strom.Bytes())); err != nil {
		t.Fatalf("Wiederholung: %v", err)
	}
	var n int
	if err := db.q().QueryRowContext(bg,
		`SELECT COUNT(*) FROM vault WHERE tenant_id = ?`, mandant.ID.String()).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("%d Vault-Zeilen nach der Wiederholung, erwartet 3", n)
	}
	if err := Repo[Vault](db).Insert(ctx, &Vault{Name: "d", APIKey: "k-d"}); err != nil {
		t.Fatalf("Schreibzugriff nach erfolgreichem Import: %v", err)
	}
}

// TestImportVollerTenant: in einen aktiven Tenant mit Daten wird nicht
// still hineingeschrieben.
func TestImportVollerTenant(t *testing.T) {
	t.Parallel()
	bg := context.Background()
	db := importTestDB(t, newTestStore(t), 4)

	mandant, err := db.Tenants().Create(bg, TenantInfo{Name: "Voll"})
	if err != nil {
		t.Fatal(err)
	}
	ctx := WithTenant(bg, mandant.ID)
	if err := Repo[Vault](db).Insert(ctx, &Vault{Name: "a", APIKey: "k"}); err != nil {
		t.Fatal(err)
	}
	var strom bytes.Buffer
	if err := db.Tenants().Export(bg, mandant.ID, &strom); err != nil {
		t.Fatal(err)
	}
	if err := db.Tenants().Import(bg, mandant.ID, bytes.NewReader(strom.Bytes())); !errors.Is(err, ErrTenantNotEmpty) {
		t.Fatalf("Import in vollen Tenant: %v, erwartet ErrTenantNotEmpty", err)
	}
}

// TestImportFremderSchemastand: ein Export von einem anderen Schemastand
// wird abgelehnt — nie still eingespielt —, lässt sich aber bewusst
// zulassen.
func TestImportFremderSchemastand(t *testing.T) {
	t.Parallel()
	bg := context.Background()
	db := importTestDB(t, newTestStore(t), 5)

	mandant, err := db.Tenants().Create(bg, TenantInfo{Name: "Alt"})
	if err != nil {
		t.Fatal(err)
	}
	ctx := WithTenant(bg, mandant.ID)
	if err := Repo[Vault](db).Insert(ctx, &Vault{Name: "a", APIKey: "k"}); err != nil {
		t.Fatal(err)
	}
	var strom bytes.Buffer
	if err := db.Tenants().Export(bg, mandant.ID, &strom); err != nil {
		t.Fatal(err)
	}
	zeilen := strings.SplitAfter(strom.String(), "\n")
	var kopf map[string]any
	if err := json.Unmarshal([]byte(zeilen[0]), &kopf); err != nil {
		t.Fatal(err)
	}
	kopf["schema_version"] = 99
	neuerKopf, err := json.Marshal(kopf)
	if err != nil {
		t.Fatal(err)
	}
	fremd := string(neuerKopf) + "\n" + strings.Join(zeilen[1:], "")

	if err := db.Tenants().Archive(bg, mandant.ID); err != nil {
		t.Fatal(err)
	}
	err = db.Tenants().Import(bg, mandant.ID, strings.NewReader(fremd))
	if !errors.Is(err, ErrExportSchemaMismatch) {
		t.Fatalf("fremder Schemastand: %v, erwartet ErrExportSchemaMismatch", err)
	}
	// Ein Export ganz ohne Kopfangabe (vor v1.2.0) ebenso.
	ohneKopf := `{"type":"tenant","data":{"ID":"` + mandant.ID.String() + `"}}` + "\n" + strings.Join(zeilen[1:], "")
	if err := db.Tenants().Import(bg, mandant.ID, strings.NewReader(ohneKopf)); !errors.Is(err, ErrExportSchemaMismatch) {
		t.Fatalf("Export ohne Schemaangabe: %v, erwartet ErrExportSchemaMismatch", err)
	}
	// Bewusst zugelassen: geht durch.
	if err := db.Tenants().Import(bg, mandant.ID, strings.NewReader(fremd), AllowSchemaDrift()); err != nil {
		t.Fatalf("Import mit AllowSchemaDrift: %v", err)
	}
}

// TestImportGeoGegenwart: die Heimatregion des ZIELS gewinnt. Ein Export
// aus Region A landet nach dem Umzug vollständig in B — sonst zerstreute
// jedes Zurückspielen die Daten wieder über die Regionen.
func TestImportGeoGegenwart(t *testing.T) {
	t.Parallel()
	bg := context.Background()
	db := importTestDB(t, newTestStore(t), 6, Region("eu-central"), Region("na"))

	mandant, err := db.Tenants().Create(bg, TenantInfo{Name: "Umzug"})
	if err != nil {
		t.Fatal(err)
	}
	eu := WithGeo(WithTenant(bg, mandant.ID), "eu-central")
	if err := Repo[Vault](db).Insert(eu, &Vault{Name: "a", APIKey: "k"}); err != nil {
		t.Fatal(err)
	}
	if _, err := New[Ticket](db).Append(eu, TicketOpened{Title: "in eu"}); err != nil {
		t.Fatal(err)
	}
	var strom bytes.Buffer
	if err := db.Tenants().Export(bg, mandant.ID, &strom); err != nil {
		t.Fatal(err)
	}
	if err := db.MoveTenant(bg, mandant.ID, "na"); err != nil {
		t.Fatal(err)
	}

	if err := db.Tenants().Archive(bg, mandant.ID); err != nil {
		t.Fatal(err)
	}
	// Ohne Geo im Context ist bei mehreren Regionen Schluss (fail-closed).
	if err := db.Tenants().Import(bg, mandant.ID, bytes.NewReader(strom.Bytes())); !errors.Is(err, ErrNoGeo) {
		t.Fatalf("Import ohne Geo: %v, erwartet ErrNoGeo", err)
	}
	na := WithGeo(bg, "na")
	if err := db.Tenants().Import(na, mandant.ID, bytes.NewReader(strom.Bytes())); err != nil {
		t.Fatalf("Import nach na: %v", err)
	}

	for _, tabelle := range []string{"vault", "ticket", "ticket_events", "ticket_snapshots"} {
		var falsch int
		if err := db.q().QueryRowContext(bg, `SELECT COUNT(*) FROM `+`"`+tabelle+`"`+
			` WHERE tenant_id = ? AND geo <> ?`, mandant.ID.String(), "na").Scan(&falsch); err != nil {
			t.Fatal(err)
		}
		if falsch != 0 {
			t.Fatalf("%s: %d Zeilen tragen nach dem Import die alte Region", tabelle, falsch)
		}
	}
	var n int
	if err := db.q().QueryRowContext(bg,
		`SELECT COUNT(*) FROM ticket_events WHERE tenant_id = ? AND geo = ?`,
		mandant.ID.String(), "na").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("%d Events in na, erwartet 1", n)
	}
}
