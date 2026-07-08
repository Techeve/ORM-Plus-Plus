package orm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
