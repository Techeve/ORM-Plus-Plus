package orm

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"
)

// Meilenstein Phase 3: Zwei App-Generationen laufen gleichzeitig gegen
// dieselbe DB durch eine komplette Expand/Contract-Migration — ohne
// Datenverlust. Alte Instanz schreibt weiter (Trigger-Nachlauf), Finalize
// verweigert bis die alte Instanz weg ist, dann fällt die Alt-Tabelle.

type Customer struct { // Model der alten Generation (Schema v1)
	ID     ID     `orm:"pk"`
	Name   string `orm:"required"`
	Street string
	City   string
}

type CustomerV1 struct { // eingefrorene Kopie für die Migration (liest Tabelle "customer")
	ID     ID     `orm:"pk"`
	Name   string `orm:"required"`
	Street string
	City   string
}

type Account struct { // Model der neuen Generation (Schema v2)
	ID      ID     `orm:"pk"`
	Name    string `orm:"required"`
	Address string
}

func TestMigrationExpandContractTwoInstances(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	bg := context.Background()

	// Alte Generation (v1) startet und schreibt Bestandsdaten.
	db1, err := Open(store())
	if err != nil {
		t.Fatalf("Open v1: %v", err)
	}
	Register[Customer](db1, CRUD())
	SchemaVersion(db1, 1)
	if err := db1.Migrate(bg); err != nil {
		t.Fatalf("Migrate v1: %v", err)
	}
	ctx := WithTenant(bg, SingleTenant)
	a := &Customer{Name: "Alba", Street: "Allee 1", City: "Aachen"}
	b := &Customer{Name: "Bruno", Street: "Berg 2", City: "Bonn"}
	for _, c := range []*Customer{a, b} {
		if err := Repo[Customer](db1).Insert(ctx, c); err != nil {
			t.Fatalf("Insert: %v", err)
		}
	}

	// Neue Generation (v2) migriert online, während v1 weiterläuft.
	db2, err := Open(store())
	if err != nil {
		t.Fatalf("Open v2: %v", err)
	}
	t.Cleanup(func() { db2.Close() })
	Register[Account](db2, CRUD())
	SchemaVersion(db2, 2)
	MigrationTo(db2, 2,
		ReplaceModel[CustomerV1, Account](func(_ context.Context, old CustomerV1) (Account, error) {
			return Account{Name: old.Name, Address: old.Street + ", " + old.City}, nil
		}),
	)
	if err := db2.Migrate(bg); err != nil {
		t.Fatalf("Migrate v2: %v", err)
	}

	// Zustand: Dual-Write, Ziel v2, Bestand transformiert (Identität erhalten).
	st, err := db2.readSchemaState(bg)
	if err != nil || st.phase != phaseDualWrite || st.target != 2 || st.current != 1 {
		t.Fatalf("Zustand nach Migrate: %+v (%v)", st, err)
	}
	got, err := Repo[Account](db2).Get(ctx, a.ID)
	if err != nil {
		t.Fatalf("Account nach Backfill: %v", err)
	}
	if got.Address != "Allee 1, Aachen" || got.Name != "Alba" {
		t.Fatalf("Transformation falsch: %+v", got)
	}

	// Alte Instanz schreibt weiter: Insert, Update, Delete → Trigger-Nachlauf.
	c := &Customer{Name: "Cesar", Street: "Weg 3", City: "Ulm"}
	if err := Repo[Customer](db1).Insert(ctx, c); err != nil {
		t.Fatalf("Insert alt: %v", err)
	}
	b.City = "Bad Godesberg"
	if err := Repo[Customer](db1).Update(ctx, b); err != nil {
		t.Fatalf("Update alt: %v", err)
	}
	if err := Repo[Customer](db1).Delete(ctx, a.ID); err != nil {
		t.Fatalf("Delete alt: %v", err)
	}
	if err := db2.drainDualWrite(bg); err != nil {
		t.Fatalf("drainDualWrite: %v", err)
	}
	if acc, err := Repo[Account](db2).Get(ctx, c.ID); err != nil || acc.Address != "Weg 3, Ulm" {
		t.Fatalf("Nachlauf-Insert: %+v (%v)", acc, err)
	}
	if acc, err := Repo[Account](db2).Get(ctx, b.ID); err != nil || acc.Address != "Berg 2, Bad Godesberg" {
		t.Fatalf("Nachlauf-Update: %+v (%v)", acc, err)
	}
	if _, err := Repo[Account](db2).Get(ctx, a.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Nachlauf-Delete: %v, erwartet ErrNotFound", err)
	}

	// Finalize verweigert, solange die alte Instanz lebt.
	if err := db2.FinalizeMigration(bg, 2); !errors.Is(err, ErrMigrationPending) {
		t.Fatalf("Finalize mit lebender Alt-Instanz: %v, erwartet ErrMigrationPending", err)
	}
	db1.Close()
	if err := db2.FinalizeMigration(bg, 2); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	// Idempotent.
	if err := db2.FinalizeMigration(bg, 2); err != nil {
		t.Fatalf("Finalize erneut: %v", err)
	}

	// Alt-Tabelle ist weg, Zustand idle auf v2, Daten vollständig.
	var n int
	if err := db2.sql.QueryRow(`SELECT COUNT(*) FROM customer`).Scan(&n); err == nil {
		t.Fatal("Alt-Tabelle customer existiert noch")
	}
	st, _ = db2.readSchemaState(bg)
	if st.current != 2 || st.target != 0 || st.phase != phaseIdle {
		t.Fatalf("Endzustand: %+v", st)
	}
	count, err := Query[Account](db2, ctx).Count()
	if err != nil || count != 2 {
		t.Fatalf("Accounts am Ende: %d (%v), erwartet 2", count, err)
	}
}

func TestAdditiveDeprecatedLifecycle(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	bg := context.Background()
	ctx := WithTenant(bg, SingleTenant)

	// v1: Ausgangsmodell.
	{
		type Gadget struct {
			ID     ID     `orm:"pk"`
			Name   string `orm:"required"`
			Legacy string
		}
		db, err := Open(store())
		if err != nil {
			t.Fatal(err)
		}
		Register[Gadget](db, CRUD())
		SchemaVersion(db, 1)
		if err := db.Migrate(bg); err != nil {
			t.Fatalf("Migrate v1: %v", err)
		}
		if err := Repo[Gadget](db).Insert(ctx, &Gadget{Name: "G1", Legacy: "alt"}); err != nil {
			t.Fatal(err)
		}
		db.Close()
	}

	// v2: neue Spalte additiv, Legacy als deprecated markiert.
	{
		type Gadget struct {
			ID     ID     `orm:"pk"`
			Name   string `orm:"required"`
			Legacy string `orm:"deprecated"`
			Color  string
		}
		db, err := Open(store())
		if err != nil {
			t.Fatal(err)
		}
		Register[Gadget](db, CRUD())
		SchemaVersion(db, 2)
		if err := db.Migrate(bg); err != nil {
			t.Fatalf("Migrate v2: %v", err)
		}
		// Spalte color ist da, Legacy noch lesbar (markiert, nicht gelöscht).
		g, err := Query[Gadget](db, ctx).First()
		if err != nil || g.Legacy != "alt" {
			t.Fatalf("v2 liest Bestand: %+v (%v)", g, err)
		}
		g.Color = "rot"
		if err := Repo[Gadget](db).Update(ctx, g); err != nil {
			t.Fatal(err)
		}
		if err := db.FinalizeMigration(bg, 2); err != nil {
			t.Fatalf("Finalize v2: %v", err)
		}
		// deprecated + noch im Struct ⇒ Spalte bleibt.
		cols, _ := db.dial.tableColumns(db.q(), "gadget")
		if !containsStr(cols, "legacy") {
			t.Fatal("legacy-Spalte darf erst fallen, wenn das Feld aus dem Struct ist")
		}
		db.Close()
	}

	// v3: Feld aus dem Struct entfernt ⇒ Finalize droppt die Spalte.
	{
		type Gadget struct {
			ID    ID     `orm:"pk"`
			Name  string `orm:"required"`
			Color string
		}
		db, err := Open(store())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { db.Close() })
		Register[Gadget](db, CRUD())
		SchemaVersion(db, 3)
		if err := db.Migrate(bg); err != nil {
			t.Fatalf("Migrate v3: %v", err)
		}
		if err := db.FinalizeMigration(bg, 3); err != nil {
			t.Fatalf("Finalize v3: %v", err)
		}
		cols, _ := db.dial.tableColumns(db.q(), "gadget")
		if containsStr(cols, "legacy") {
			t.Fatal("legacy-Spalte wurde bei Finalize nicht entfernt")
		}
		g, err := Query[Gadget](db, ctx).First()
		if err != nil || g.Name != "G1" || g.Color != "rot" {
			t.Fatalf("Daten nach Contract: %+v (%v)", g, err)
		}
	}
}

func containsStr(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

func TestBatchScriptCheckpointResume(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	bg := context.Background()

	type Item struct {
		ID   ID     `orm:"pk"`
		Name string `orm:"required"`
	}

	// v1 anlegen.
	db1, err := Open(store())
	if err != nil {
		t.Fatal(err)
	}
	Register[Item](db1, CRUD())
	SchemaVersion(db1, 1)
	if err := db1.Migrate(bg); err != nil {
		t.Fatal(err)
	}
	db1.Close()

	// v2 mit einem Skript, das beim ersten Lauf nach dem Checkpoint abbricht.
	calls := 0
	var resumedFrom string
	newDB := func() *DB {
		db, err := Open(store())
		if err != nil {
			t.Fatal(err)
		}
		Register[Item](db, CRUD())
		SchemaVersion(db, 2)
		MigrationTo(db, 2, BatchScript("normalize", func(ctx context.Context, b Batch) error {
			calls++
			cp, err := b.Checkpoint(ctx)
			if err != nil {
				return err
			}
			resumedFrom = cp
			if cp == "" {
				if err := b.SaveCheckpoint(ctx, "halbzeit", 50); err != nil {
					return err
				}
				return fmt.Errorf("simulierter Absturz")
			}
			return nil // zweiter Lauf: ab Checkpoint fertig machen
		}))
		return db
	}

	db2 := newDB()
	if err := db2.Migrate(bg); err == nil {
		t.Fatal("Migrate muss den Skriptfehler liefern")
	}
	st, _ := db2.readSchemaState(bg)
	if st.phase != phaseBackfill {
		t.Fatalf("Phase nach Absturz: %q, erwartet backfill", st.phase)
	}
	db2.Close()

	db3 := newDB()
	t.Cleanup(func() { db3.Close() })
	if err := db3.Migrate(bg); err != nil {
		t.Fatalf("Wiederaufnahme: %v", err)
	}
	if calls != 2 || resumedFrom != "halbzeit" {
		t.Fatalf("Resume: calls=%d checkpoint=%q, erwartet 2/halbzeit", calls, resumedFrom)
	}
	if st, _ := db3.readSchemaState(bg); st.phase != phaseDualWrite {
		t.Fatalf("Phase nach Wiederaufnahme: %q", st.phase)
	}
}

func TestLeasesAndInstanceRegister(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	bg := context.Background()

	open := func() *DB {
		db, err := Open(store())
		if err != nil {
			t.Fatal(err)
		}
		if err := db.Migrate(bg); err != nil {
			t.Fatal(err)
		}
		return db
	}
	db1 := open()
	db2 := open()
	t.Cleanup(func() { db2.Close() })

	// Beide Instanzen sind registriert.
	var n int
	if err := db1.sql.QueryRow(`SELECT COUNT(*) FROM ormpp_instances`).Scan(&n); err != nil || n != 2 {
		t.Fatalf("Instanzregister: %d (%v), erwartet 2", n, err)
	}

	// Lease: nur ein Halter; Renewal durch den Halter; Übernahme nach Ablauf.
	if ok, err := db1.acquireLease(bg, "test", time.Hour); err != nil || !ok {
		t.Fatalf("acquire db1: %v/%v", ok, err)
	}
	if ok, _ := db2.acquireLease(bg, "test", time.Hour); ok {
		t.Fatal("db2 darf die gültige Lease von db1 nicht übernehmen")
	}
	if ok, _ := db1.acquireLease(bg, "test", time.Hour); !ok {
		t.Fatal("Halter muss seine Lease erneuern können")
	}
	db1.releaseLease(bg, "test")
	if ok, _ := db2.acquireLease(bg, "test", time.Hour); !ok {
		t.Fatal("freigegebene Lease muss übernehmbar sein")
	}
	var token int64
	if err := db2.sql.QueryRow(`SELECT fencing_token FROM ormpp_leases WHERE name = 'test'`).Scan(&token); err != nil || token != 1 {
		// releaseLease löscht die Zeile — Neuanlage startet bei 1.
		t.Fatalf("fencing_token = %d (%v)", token, err)
	}

	// Ablauf: kurze Lease läuft ab, fremde Instanz übernimmt mit Token-Bump.
	if ok, _ := db2.acquireLease(bg, "exp", 30*time.Millisecond); !ok {
		t.Fatal("acquire exp")
	}
	time.Sleep(50 * time.Millisecond)
	if ok, _ := db1.acquireLease(bg, "exp", time.Hour); !ok {
		t.Fatal("abgelaufene Lease muss übernehmbar sein")
	}
	if err := db1.sql.QueryRow(`SELECT fencing_token FROM ormpp_leases WHERE name = 'exp'`).Scan(&token); err != nil || token != 2 {
		t.Fatalf("Fencing-Token nach Übernahme = %d (%v), erwartet 2", token, err)
	}

	// Close räumt Register und Leases auf.
	db1.Close()
	if err := db2.sql.QueryRow(`SELECT COUNT(*) FROM ormpp_instances`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("Instanzregister nach Close: %d (%v), erwartet 1", n, err)
	}
	if err := db2.q().QueryRowContext(bg, `SELECT COUNT(*) FROM ormpp_leases WHERE holder = ?`, db1.instanceID.String()).Scan(&n); err != nil || n != 0 {
		t.Fatalf("Leases nach Close: %d (%v), erwartet 0", n, err)
	}
}

func TestFinalizeGuards(t *testing.T) {
	t.Parallel()
	db, _ := testDB(t)
	bg := context.Background()

	// Nichts im Dual-Write ⇒ ErrMigrationPending (aber: bereits finalisierte
	// aktuelle Version ist idempotent OK).
	if err := db.FinalizeMigration(bg, 99); !errors.Is(err, ErrMigrationPending) {
		t.Fatalf("Finalize(99): %v, erwartet ErrMigrationPending", err)
	}
	if err := db.FinalizeMigration(bg, 1); err != nil {
		t.Fatalf("Finalize(aktuelle Version, idle) muss idempotent OK sein: %v", err)
	}
}

// Regression: eine per ALTER ADD COLUMN nachgerüstete JSON-Spalte muss für
// Bestandszeilen lesbar sein. Vor dem Fix befüllte das Text-Zero-Literal ”
// die Zeilen — json.Unmarshal("") scheiterte mit "unexpected end of JSON
// input". Jetzt ist das Zero-Literal 'null' (gültiges JSON auf allen
// Backends). Die Heilung von ”-Altbeständen testet
// TestDecodeJSONHealsLegacyEmptyCell — ”-Zellen kann es nur auf SQLite
// geben (TEXT-Spalte); JSONB auf PG/YB lehnt ” physisch ab.
func TestAlterAddedJSONColumnReadsAsZero(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	bg := context.Background()
	ctx := WithTenant(bg, SingleTenant)

	// v1: Modell ohne JSON-Feld, eine Bestandszeile.
	{
		type Widget struct {
			ID   ID     `orm:"pk"`
			Name string `orm:"required"`
		}
		db, err := Open(store())
		if err != nil {
			t.Fatal(err)
		}
		Register[Widget](db, CRUD())
		SchemaVersion(db, 1)
		if err := db.Migrate(bg); err != nil {
			t.Fatalf("Migrate v1: %v", err)
		}
		if err := Repo[Widget](db).Insert(ctx, &Widget{Name: "alt"}); err != nil {
			t.Fatal(err)
		}
		db.Close()
	}

	// v2: JSON-Spalte kommt per ALTER dazu — Bestandszeile bleibt lesbar.
	{
		type Widget struct {
			ID   ID       `orm:"pk"`
			Name string   `orm:"required"`
			Tags []string `orm:"json"`
		}
		db, err := Open(store())
		if err != nil {
			t.Fatal(err)
		}
		Register[Widget](db, CRUD())
		SchemaVersion(db, 2)
		if err := db.Migrate(bg); err != nil {
			t.Fatalf("Migrate v2: %v", err)
		}
		defer db.Close()

		rows, err := Query[Widget](db, ctx).All()
		if err != nil {
			t.Fatalf("Bestandszeile mit nachgerüsteter JSON-Spalte lesen: %v", err)
		}
		if len(rows) != 1 || rows[0].Name != "alt" || rows[0].Tags != nil {
			t.Fatalf("rows = %+v", rows)
		}

		// Schreiben und Zurücklesen funktioniert normal weiter.
		w := rows[0]
		w.Tags = []string{"a", "b"}
		if err := Repo[Widget](db).Update(ctx, w); err != nil {
			t.Fatal(err)
		}
		w, err = Query[Widget](db, ctx).First()
		if err != nil || len(w.Tags) != 2 {
			t.Fatalf("roundtrip = %+v (%v)", w, err)
		}
	}
}

// Heilung von ”-Altbeständen (vom alten Text-Zero-Literal auf SQLite
// hinterlassen): leere JSON-Zellen zählen beim Dekodieren wie NULL statt an
// json.Unmarshal zu scheitern. Bewusst ein Unit-Test der Dekodierung — auf
// PG/YB kann ” in einer JSONB-Spalte gar nicht existieren, also lässt sich
// der Zustand dort nicht verhaltensgleich provozieren; der Dekodier-Pfad
// selbst ist backend-frei und damit überall identisch.
func TestDecodeJSONHealsLegacyEmptyCell(t *testing.T) {
	t.Parallel()
	f := &field{column: "tags", dk: dJSON}

	for name, raw := range map[string]any{
		"leerer String": "",
		"leeres Blob":   []byte{},
		"NULL":          nil,
	} {
		tags := []string{"vorbelegt"} // beweist, dass wirklich genullt wird
		target := reflect.ValueOf(&tags).Elem()
		if err := decodeField(nil, f, target, raw); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if tags != nil {
			t.Fatalf("%s: tags = %+v, erwartet Zero-Value", name, tags)
		}
	}

	// Gültiges JSON dekodiert unverändert weiter.
	var tags []string
	if err := decodeField(nil, f, reflect.ValueOf(&tags).Elem(), `["a","b"]`); err != nil {
		t.Fatal(err)
	}
	if len(tags) != 2 || tags[0] != "a" {
		t.Fatalf("tags = %+v", tags)
	}
}

// TenantFree-Modelle müssen genauso ersetzbar sein: die Alt-Tabelle hat
// keine tenant_id-Spalte — das Alt-Struct erbt die Bindung des Ziel-Models
// (Regression: compileReplace kompilierte das Alt-Struct immer
// tenant-gebunden und scheiterte an der Bindungsprüfung).
type Directory struct { // altes TenantFree-Model (v1)
	ID   ID     `orm:"pk"`
	Name string `orm:"required"`
	Kind string
}

type DirectoryV1 struct { // eingefrorene Kopie (liest Tabelle "directory")
	ID   ID     `orm:"pk"`
	Name string `orm:"required"`
	Kind string
}

type Catalog struct { // neues TenantFree-Model (v2)
	ID    ID     `orm:"pk"`
	Title string `orm:"required"`
}

func TestReplaceModelTenantFree(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	bg := context.Background()

	db1, err := Open(store())
	if err != nil {
		t.Fatalf("Open v1: %v", err)
	}
	Register[Directory](db1, CRUD(), TenantFree())
	SchemaVersion(db1, 1)
	if err := db1.Migrate(bg); err != nil {
		t.Fatalf("Migrate v1: %v", err)
	}
	ctx := WithTenant(bg, SingleTenant)
	d := &Directory{Name: "Stammdaten", Kind: "global"}
	if err := Repo[Directory](db1).Insert(ctx, d); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if err := db1.Close(); err != nil {
		t.Fatalf("Close v1: %v", err)
	}

	db2, err := Open(store())
	if err != nil {
		t.Fatalf("Open v2: %v", err)
	}
	t.Cleanup(func() { db2.Close() })
	Register[Catalog](db2, CRUD(), TenantFree())
	SchemaVersion(db2, 2)
	MigrationTo(db2, 2,
		ReplaceModel[DirectoryV1, Catalog](func(_ context.Context, old DirectoryV1) (Catalog, error) {
			return Catalog{Title: old.Name + " (" + old.Kind + ")"}, nil
		}),
	)
	if err := db2.Migrate(bg); err != nil {
		t.Fatalf("Migrate v2: %v", err)
	}
	got, err := Repo[Catalog](db2).Get(ctx, d.ID)
	if err != nil {
		t.Fatalf("Catalog nach Backfill: %v", err)
	}
	if got.Title != "Stammdaten (global)" {
		t.Fatalf("Transformation falsch: %+v", got)
	}
	if err := db2.FinalizeMigration(bg, 2); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
}
