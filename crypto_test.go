package orm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

type Vault struct {
	ID     ID     `orm:"pk"`
	Name   string `orm:"index"`
	APIKey string `orm:"encrypted,required"`
	Blob   []byte `orm:"encrypted"`
	Note   *string
}

func testKey(b byte) []byte {
	k := make([]byte, 32)
	for i := range k {
		k[i] = b
	}
	return k
}

// rotatingKeys: aktueller Schlüssel + Altbestand (Rotations-Szenario).
type rotatingKeys struct {
	current string
	keys    map[string][]byte
}

func (r rotatingKeys) CurrentKey() (string, []byte, error) { return r.current, r.keys[r.current], nil }
func (r rotatingKeys) Key(id string) ([]byte, error) {
	k, ok := r.keys[id]
	if !ok {
		return nil, fmt.Errorf("unbekannte Key-ID %q", id)
	}
	return k, nil
}

func cryptoDB(t *testing.T, store func() Driver, p KeyProvider) (*DB, context.Context) {
	t.Helper()
	db, err := Open(store(), Encryption(p))
	if err != nil {
		t.Fatal(err)
	}
	Register[Vault](db, CRUD())
	if err := db.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	return db, WithTenant(context.Background(), SingleTenant)
}

func TestEncryptionRoundtripAndOpacity(t *testing.T) {
	t.Parallel()
	db, ctx := cryptoDB(t, newTestStore(t), StaticKey(testKey(1)))
	defer db.Close()

	v := &Vault{Name: "prod", APIKey: "geheim-123", Blob: []byte("bytes")}
	if err := Repo[Vault](db).Insert(ctx, v); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	got, err := Repo[Vault](db).Get(ctx, v.ID)
	if err != nil || got.APIKey != "geheim-123" || string(got.Blob) != "bytes" || got.Note != nil {
		t.Fatalf("Roundtrip: %+v (%v)", got, err)
	}

	// Die DB sieht nur Ciphertext.
	var raw []byte
	if err := db.q().QueryRowContext(ctx, `SELECT api_key FROM vault WHERE id = ?`, v.ID.String()).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, []byte("geheim-123")) {
		t.Fatal("Klartext in der Spalte")
	}
	if len(raw) == 0 || raw[0] != cipherVersion {
		t.Fatalf("Ciphertext-Format: %x", raw[:2])
	}

	// Update verschlüsselt neu; Roundtrip bleibt korrekt.
	got.APIKey = "geheim-456"
	if err := Repo[Vault](db).Update(ctx, got); err != nil {
		t.Fatalf("Update: %v", err)
	}
	got2, _ := Repo[Vault](db).Get(ctx, v.ID)
	if got2.APIKey != "geheim-456" {
		t.Fatalf("Update-Roundtrip: %q", got2.APIKey)
	}

	// Mengenbasiertes UpdateSet verschlüsselt engine-seitig.
	if _, err := Query[Vault](db, ctx).Where(Eq("Name", "prod")).UpdateSet(Set("APIKey", "geheim-789")); err != nil {
		t.Fatalf("UpdateSet: %v", err)
	}
	got3, _ := Repo[Vault](db).Get(ctx, v.ID)
	if got3.APIKey != "geheim-789" {
		t.Fatalf("UpdateSet-Roundtrip: %q", got3.APIKey)
	}
}

func TestEncryptedNotFilterable(t *testing.T) {
	t.Parallel()
	db, ctx := cryptoDB(t, newTestStore(t), StaticKey(testKey(1)))
	defer db.Close()

	if _, err := Query[Vault](db, ctx).Where(Eq("APIKey", "x")).All(); err == nil || !strings.Contains(err.Error(), "encrypted") {
		t.Fatalf("Filter auf encrypted: %v, erwartet Fehler", err)
	}
	if _, err := Query[Vault](db, ctx).OrderBy("APIKey", Asc).All(); err == nil {
		t.Fatal("OrderBy auf encrypted muss fehlschlagen")
	}
}

func TestEncryptionRequiresProvider(t *testing.T) {
	t.Parallel()
	db, err := Open(newTestStore(t)())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	Register[Vault](db, CRUD())
	if err := db.Migrate(context.Background()); err == nil || !strings.Contains(err.Error(), "Encryption") {
		t.Fatalf("Migrate ohne Provider: %v, erwartet Encryption-Fehler", err)
	}
}

func TestEncryptionKeyRotation(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)

	// Generation 1 schreibt mit k1.
	db1, ctx := cryptoDB(t, store, rotatingKeys{current: "k1", keys: map[string][]byte{"k1": testKey(1)}})
	v := &Vault{Name: "rot", APIKey: "alt"}
	if err := Repo[Vault](db1).Insert(ctx, v); err != nil {
		t.Fatal(err)
	}
	db1.Close()

	// Generation 2: k2 ist aktuell, k1 nur noch zum Lesen.
	db2, ctx := cryptoDB(t, store, rotatingKeys{current: "k2", keys: map[string][]byte{"k1": testKey(1), "k2": testKey(2)}})
	defer db2.Close()
	got, err := Repo[Vault](db2).Get(ctx, v.ID)
	if err != nil || got.APIKey != "alt" {
		t.Fatalf("Lesen nach Rotation: %+v (%v)", got, err)
	}
	// Lazy-Rotation: nächstes Schreiben nutzt k2.
	got.APIKey = "neu"
	if err := Repo[Vault](db2).Update(ctx, got); err != nil {
		t.Fatal(err)
	}
	var raw []byte
	if err := db2.q().QueryRowContext(ctx, `SELECT api_key FROM vault WHERE id = ?`, v.ID.String()).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	if len(raw) < 4 || string(raw[2:4]) != "k2" {
		t.Fatalf("Ciphertext trägt nicht die neue Key-ID: %q", raw[:6])
	}
	// Falscher Schlüssel scheitert sauber.
	db3, ctx3 := cryptoDB(t, store, rotatingKeys{current: "k3", keys: map[string][]byte{"k3": testKey(3)}})
	defer db3.Close()
	if _, err := Repo[Vault](db3).Get(ctx3, v.ID); err == nil {
		t.Fatal("Lesen ohne passenden Schlüssel muss fehlschlagen")
	}
}

// SecretTicket ist gültig ES (Aggregate + Apply) — nur das encrypted-Feld
// muss die Registrierung scheitern lassen.
type SecretTicket struct {
	Aggregate
	Secret string `orm:"encrypted"`
}

func (s *SecretTicket) Apply(e Event) error { return nil }

func TestEncryptedOnEventSourcedRejected(t *testing.T) {
	t.Parallel()
	db, err := Open(newTestStore(t)(), Encryption(StaticKey(testKey(1))))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	Register[SecretTicket](db, EventSourced(), Events(E[TicketOpened]("s.opened")))
	err = db.Migrate(context.Background())
	if err == nil || !strings.Contains(err.Error(), "encrypted") {
		t.Fatalf("encrypted auf ES-Model: %v, erwartet encrypted-Fehler", err)
	}
}

func TestTenantExport(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	db, err := Open(store(), Encryption(StaticKey(testKey(1))))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	Register[Vault](db, CRUD())
	Register[Ticket](db, EventSourced(), ticketEvents(), SnapshotEvery(2))
	if err := db.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	bg := context.Background()
	tenA, _ := db.Tenants().Create(bg, TenantInfo{Name: "A"})
	tenB, _ := db.Tenants().Create(bg, TenantInfo{Name: "B"})
	ctxA := WithTenant(bg, tenA.ID)
	ctxB := WithTenant(bg, tenB.ID)

	if err := Repo[Vault](db).Insert(ctxA, &Vault{Name: "va", APIKey: "klartext-geheim"}); err != nil {
		t.Fatal(err)
	}
	if err := Repo[Vault](db).Insert(ctxB, &Vault{Name: "vb", APIKey: "fremdes-geheimnis"}); err != nil {
		t.Fatal(err)
	}
	tk := New[Ticket](db)
	if _, err := tk.Append(ctxA, TicketOpened{Title: "Export"}, NoteAdded{Note: "n1"}, NoteAdded{Note: "n2"}); err != nil {
		t.Fatal(err)
	}
	m := db.reg.byName["Ticket"]
	if err := db.processProjection(bg, m); err != nil {
		t.Fatal(err)
	}
	if err := db.maybeSnapshot(bg, m); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	if err := db.Tenants().Export(bg, tenA.ID, &buf); err != nil {
		t.Fatalf("Export: %v", err)
	}
	out := buf.String()
	lines := strings.Split(strings.TrimSpace(out), "\n")

	types := map[string]int{}
	for _, l := range lines {
		var rec struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal([]byte(l), &rec); err != nil {
			t.Fatalf("keine gültige JSON-Line: %q (%v)", l, err)
		}
		types[rec.Type]++
	}
	if types["tenant"] != 1 || types["row"] != 2 || types["event"] != 3 || types["snapshot"] != 1 {
		t.Fatalf("Export-Inhalt: %v, erwartet tenant=1 row=2 event=3 snapshot=1", types)
	}
	// Entschlüsselt (DSGVO-Auskunft) …
	if !strings.Contains(out, "klartext-geheim") {
		t.Fatal("encrypted-Feld muss im Export entschlüsselt sein")
	}
	// … und strikt tenant-getrennt.
	if strings.Contains(out, "fremdes-geheimnis") || strings.Contains(out, "vb") {
		t.Fatal("Export enthält Daten eines fremden Tenants")
	}

	// Unbekannter Tenant.
	if err := db.Tenants().Export(bg, NewID(), &buf); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Export unbekannter Tenant: %v, erwartet ErrNotFound", err)
	}
	// Archivierte Tenants bleiben exportierbar.
	if err := db.Tenants().Archive(bg, tenA.ID); err != nil {
		t.Fatal(err)
	}
	buf.Reset()
	if err := db.Tenants().Export(bg, tenA.ID, &buf); err != nil {
		t.Fatalf("Export archiviert: %v", err)
	}
}

func TestTenantPurge(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	db, err := Open(store(), Encryption(StaticKey(testKey(1))))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	Register[Vault](db, CRUD())
	Register[Ticket](db, EventSourced(), ticketEvents(), SnapshotEvery(2))
	if err := db.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	bg := context.Background()
	tenA, _ := db.Tenants().Create(bg, TenantInfo{Name: "Löschkandidat"})
	tenB, _ := db.Tenants().Create(bg, TenantInfo{Name: "Bleibt"})
	ctxA := WithTenant(bg, tenA.ID)
	ctxB := WithTenant(bg, tenB.ID)

	if err := Repo[Vault](db).Insert(ctxA, &Vault{Name: "va", APIKey: "s"}); err != nil {
		t.Fatal(err)
	}
	if err := Repo[Vault](db).Insert(ctxB, &Vault{Name: "vb", APIKey: "s"}); err != nil {
		t.Fatal(err)
	}
	tk := New[Ticket](db)
	if _, err := tk.Append(ctxA, TicketOpened{Title: "weg"}, NoteAdded{Note: "n"}, NoteAdded{Note: "n2"}); err != nil {
		t.Fatal(err)
	}
	tkB := New[Ticket](db)
	if _, err := tkB.Append(ctxB, TicketOpened{Title: "bleibt"}); err != nil {
		t.Fatal(err)
	}
	m := db.reg.byName["Ticket"]
	if err := db.processProjection(bg, m); err != nil {
		t.Fatal(err)
	}
	if err := db.maybeSnapshot(bg, m); err != nil {
		t.Fatal(err)
	}

	// Nicht archiviert ⇒ verweigert.
	if err := db.Tenants().Purge(bg, tenA.ID); !errors.Is(err, ErrTenantNotArchived) {
		t.Fatalf("Purge aktiv: %v, erwartet ErrTenantNotArchived", err)
	}
	if err := db.Tenants().Archive(bg, tenA.ID); err != nil {
		t.Fatal(err)
	}
	if err := db.Tenants().Purge(bg, tenA.ID); err != nil {
		t.Fatalf("Purge: %v", err)
	}

	// Alles weg — über alle Tabellen; Tenant B unberührt.
	tidA, tidB := tenA.ID.String(), tenB.ID.String()
	for _, tbl := range []string{"vault", "ticket", "ticket_events", "ticket_events_archive", "ticket_snapshots", "ormpp_tenants"} {
		var a, b int
		if err := db.q().QueryRowContext(bg, fmt.Sprintf(`SELECT COUNT(*) FROM %q WHERE tenant_id = ?`, tbl), tidA).Scan(&a); err != nil {
			t.Fatalf("%s: %v", tbl, err)
		}
		if a != 0 {
			t.Fatalf("Purge unvollständig: %d Zeilen in %s", a, tbl)
		}
		if tbl == "ormpp_tenants" || tbl == "ticket_events_archive" {
			continue
		}
		if err := db.q().QueryRowContext(bg, fmt.Sprintf(`SELECT COUNT(*) FROM %q WHERE tenant_id = ?`, tbl), tidB).Scan(&b); err != nil {
			t.Fatal(err)
		}
		if (tbl == "vault" || tbl == "ticket_events") && b == 0 {
			t.Fatalf("Purge hat fremde Daten gelöscht (%s)", tbl)
		}
	}
	// Cache invalidiert: Schreiben auf den gelöschten Tenant scheitert.
	if err := Repo[Vault](db).Insert(ctxA, &Vault{Name: "x", APIKey: "s"}); !errors.Is(err, ErrUnknownTenant) {
		t.Fatalf("Insert nach Purge: %v, erwartet ErrUnknownTenant", err)
	}
	// Auditiert.
	var audits int
	if err := db.q().QueryRowContext(bg,
		`SELECT COUNT(*) FROM ormpp_schema_history WHERE phase_from = 'tenant-purge' AND phase_to = ?`, tidA).Scan(&audits); err != nil {
		t.Fatal(err)
	}
	if audits != 1 {
		t.Fatalf("Audit-Eintrag fehlt: %d", audits)
	}
}

// Nachträglich per Migrate ergänzte encrypted-Spalten: ALTER ADD COLUMN
// befüllt Bestandszeilen über das Blob-Zero-Literal ('\x' auf PG/YB, x”
// auf SQLite) mit einem LEEREN Nicht-NULL-Blob. Der zählt beim Dekodieren
// wie NULL statt am Ciphertext-Parser zu scheitern — vor dem Fix brach
// damit JEDER Lesezugriff aufs Model, einschließlich der Migration selbst
// (Fund beim DNS-Editor-Beta-Update auf YugabyteDB, 2026-08-07).
func TestAlterAddedEncryptedColumnReadsAsZero(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	bg := context.Background()
	ctx := WithTenant(bg, SingleTenant)

	// v1: Modell ohne encrypted-Felder, eine Bestandszeile.
	{
		type Locker struct {
			ID   ID     `orm:"pk"`
			Name string `orm:"required"`
		}
		db, err := Open(store())
		if err != nil {
			t.Fatal(err)
		}
		Register[Locker](db, CRUD())
		SchemaVersion(db, 1)
		if err := db.Migrate(bg); err != nil {
			t.Fatalf("Migrate v1: %v", err)
		}
		if err := Repo[Locker](db).Insert(ctx, &Locker{Name: "alt"}); err != nil {
			t.Fatal(err)
		}
		db.Close()
	}

	// v2: encrypted-Felder kommen per ALTER dazu — die Bestandszeile bleibt
	// lesbar und liefert Zero-Values (Pointer bleibt nil, wie bei NULL).
	{
		type Locker struct {
			ID     ID      `orm:"pk"`
			Name   string  `orm:"required"`
			Secret string  `orm:"encrypted"`
			Raw    []byte  `orm:"encrypted"`
			Hint   *string `orm:"encrypted"`
		}
		db, err := Open(store(), Encryption(StaticKey(testKey(7))))
		if err != nil {
			t.Fatal(err)
		}
		Register[Locker](db, CRUD())
		SchemaVersion(db, 2)
		if err := db.Migrate(bg); err != nil {
			t.Fatalf("Migrate v2: %v", err)
		}
		defer db.Close()

		rows, err := Query[Locker](db, ctx).All()
		if err != nil {
			t.Fatalf("Bestandszeile mit nachgerüsteter encrypted-Spalte lesen: %v", err)
		}
		if len(rows) != 1 || rows[0].Name != "alt" {
			t.Fatalf("rows = %+v", rows)
		}
		if rows[0].Secret != "" || rows[0].Raw != nil || rows[0].Hint != nil {
			t.Fatalf("Zero-Values erwartet, bekam %+v", rows[0])
		}

		// Schreiben und Zurücklesen funktioniert normal weiter — danach liegt
		// echter Ciphertext in der Spalte.
		l := rows[0]
		l.Secret = "streng geheim"
		hint := "unter der Matte"
		l.Hint = &hint
		if err := Repo[Locker](db).Update(ctx, l); err != nil {
			t.Fatal(err)
		}
		l, err = Query[Locker](db, ctx).First()
		if err != nil || l.Secret != "streng geheim" || l.Hint == nil || *l.Hint != "unter der Matte" {
			t.Fatalf("roundtrip = %+v (%v)", l, err)
		}
	}
}

// Der Dekodier-Pfad selbst, backend-frei: leere Blobs heilen zu Zero-Values,
// NULL bleibt NULL, echter Ciphertext dekodiert unverändert.
func TestDecodeEncryptedHealsEmptyBlob(t *testing.T) {
	t.Parallel()
	d := &DB{opts: openOptions{keys: StaticKey(testKey(3))}}
	f := &field{column: "secret", dk: dEncrypted}

	for name, raw := range map[string]any{
		"leeres Blob": []byte{},
		"NULL":        nil,
	} {
		s := "vorbelegt" // beweist, dass wirklich genullt wird
		target := reflect.ValueOf(&s).Elem()
		if err := decodeField(d, f, target, raw); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if s != "" {
			t.Fatalf("%s: s = %q, erwartet leer", name, s)
		}
		// Pointer-Ziel: bleibt nil, symmetrisch zu NULL.
		p := &s
		ptarget := reflect.ValueOf(&p).Elem()
		if err := decodeField(d, f, ptarget, raw); err != nil {
			t.Fatalf("%s (Pointer): %v", name, err)
		}
		if p != nil {
			t.Fatalf("%s (Pointer): p = %v, erwartet nil", name, p)
		}
	}

	// Echter Ciphertext dekodiert weiterhin.
	blob, err := encryptValue(d.opts.keys, []byte("wert"))
	if err != nil {
		t.Fatal(err)
	}
	var s string
	if err := decodeField(d, f, reflect.ValueOf(&s).Elem(), blob); err != nil {
		t.Fatal(err)
	}
	if s != "wert" {
		t.Fatalf("s = %q", s)
	}
}
