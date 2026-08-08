package orm

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

type LoginAccount struct {
	ID    ID     `orm:"pk"`
	Name  string `orm:"index"`
	Email string `orm:"encrypted,lookup,unique"`
	Note  string `orm:"encrypted,lookup"`
}

func lookupDB(t *testing.T, store func() Driver) (*DB, context.Context) {
	t.Helper()
	db, err := Open(store(), Encryption(StaticKey(testKey(9))))
	if err != nil {
		t.Fatal(err)
	}
	Register[LoginAccount](db, CRUD())
	if err := db.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	return db, WithTenant(context.Background(), SingleTenant)
}

// Blind-Index-Grundverhalten: Eq findet über die Index-Spalte, normalisiert
// (ToLower+TrimSpace), die DB sieht nur Ciphertext + HMAC; unique wirkt auf
// dem Index; Updates ziehen den Index nach.
func TestLookupEqUniqueAndNormalization(t *testing.T) {
	t.Parallel()
	db, ctx := lookupDB(t, newTestStore(t))
	defer db.Close()

	a := &LoginAccount{Name: "a", Email: "Alice@Example.ORG", Note: "geheim"}
	if err := Repo[LoginAccount](db).Insert(ctx, a); err != nil {
		t.Fatal(err)
	}
	if err := Repo[LoginAccount](db).Insert(ctx, &LoginAccount{Name: "b", Email: "bob@example.org"}); err != nil {
		t.Fatal(err)
	}

	// Eq normalisiert den Parameter wie den gespeicherten Wert.
	got, err := Query[LoginAccount](db, ctx).Where(Eq("Email", "  alice@example.org  ")).First()
	if err != nil || got.Name != "a" || got.Email != "Alice@Example.ORG" {
		t.Fatalf("Eq-Lookup: %+v (%v)", got, err)
	}
	// In und Ne laufen ebenfalls über den Index.
	list, err := Query[LoginAccount](db, ctx).Where(In("Email", "ALICE@example.org", "bob@example.org")).All()
	if err != nil || len(list) != 2 {
		t.Fatalf("In-Lookup: %d (%v)", len(list), err)
	}
	list, err = Query[LoginAccount](db, ctx).Where(Ne("Email", "alice@example.org")).All()
	if err != nil || len(list) != 1 || list[0].Name != "b" {
		t.Fatalf("Ne-Lookup: %+v (%v)", list, err)
	}

	// unique wirkt auf dem normalisierten Index: gleiche Adresse in anderer
	// Schreibweise kollidiert.
	if err := Repo[LoginAccount](db).Insert(ctx, &LoginAccount{Name: "dup", Email: "ALICE@EXAMPLE.org"}); err == nil {
		t.Fatal("unique auf lookup-Feld: Duplikat wurde angenommen")
	}

	// Mehr als Gleichheit gibt es nicht.
	if _, err := Query[LoginAccount](db, ctx).Where(Like("Email", "%example%")).All(); err == nil {
		t.Fatal("LIKE auf lookup-Feld muss scheitern")
	}
	if _, err := Query[LoginAccount](db, ctx).Where(Gt("Email", "a")).All(); err == nil {
		t.Fatal("Gt auf lookup-Feld muss scheitern")
	}
	if _, err := Query[LoginAccount](db, ctx).OrderBy("Email", Asc).All(); err == nil {
		t.Fatal("OrderBy auf lookup-Feld muss scheitern")
	}

	// Die DB sieht Ciphertext und einen 32-Byte-HMAC — keinen Klartext.
	var rawMail, rawIdx []byte
	if err := db.q().QueryRowContext(ctx, `SELECT "email", "email_lookup" FROM "login_account" WHERE "name" = ?`, "a").
		Scan(&rawMail, &rawIdx); err != nil {
		t.Fatal(err)
	}
	if len(rawMail) < 2 || rawMail[0] != cipherVersion || bytes.Contains(rawMail, []byte("alice")) {
		t.Fatalf("email-Spalte ist kein Ciphertext: %x", rawMail[:min(8, len(rawMail))])
	}
	if len(rawIdx) != 32 {
		t.Fatalf("email_lookup: %d Bytes statt HMAC-SHA-256", len(rawIdx))
	}

	// Update zieht den Index nach: alte Adresse verschwindet, neue findet.
	a.Email = "carol@example.org"
	if err := Repo[LoginAccount](db).Update(ctx, a); err != nil {
		t.Fatal(err)
	}
	if _, err := Query[LoginAccount](db, ctx).Where(Eq("Email", "alice@example.org")).First(); err != ErrNotFound {
		t.Fatalf("alte Adresse noch auffindbar: %v", err)
	}
	if got, err := Query[LoginAccount](db, ctx).Where(Eq("Email", "carol@example.org")).First(); err != nil || got.Name != "a" {
		t.Fatalf("neue Adresse nicht auffindbar: %+v (%v)", got, err)
	}

	// UpdateSet verschlüsselt UND indexiert engine-seitig.
	if _, err := Query[LoginAccount](db, ctx).Where(Eq("Name", "b")).UpdateSet(Set("Email", "Bobby@Example.org")); err != nil {
		t.Fatal(err)
	}
	if got, err := Query[LoginAccount](db, ctx).Where(Eq("Email", "bobby@example.org")).First(); err != nil || got.Name != "b" {
		t.Fatalf("UpdateSet-Lookup: %+v (%v)", got, err)
	}
}

// Leere Werte liegen als NULL im Index: sie kollidieren nie (auch nicht
// unter unique) und Eq("") findet sie über IS NULL.
func TestLookupEmptyValuesAreNull(t *testing.T) {
	t.Parallel()
	db, ctx := lookupDB(t, newTestStore(t))
	defer db.Close()

	for _, name := range []string{"x", "y"} {
		if err := Repo[LoginAccount](db).Insert(ctx, &LoginAccount{Name: name, Email: "   "}); err != nil {
			t.Fatalf("leere Adresse %s: %v", name, err)
		}
	}
	list, err := Query[LoginAccount](db, ctx).Where(Eq("Email", "")).All()
	if err != nil || len(list) != 2 {
		t.Fatalf("Eq(leer): %d (%v)", len(list), err)
	}
	n, err := Query[LoginAccount](db, ctx).Where(NotNull("Email")).Count()
	if err != nil || n != 0 {
		t.Fatalf("NotNull: %d (%v)", n, err)
	}
}

// Export/Import-Roundtrip mit Schlüsselwechsel: der Export trägt Klartext,
// der Import verschlüsselt mit dem Schlüssel der Zielanlage neu und
// berechnet die Lookup-Indizes neu — Eq funktioniert im Ziel sofort,
// obwohl der Index-Schlüssel ein anderer ist.
func TestLookupSurvivesExportImportWithKeyChange(t *testing.T) {
	t.Parallel()
	bg := context.Background()

	src, err := Open(newTestStore(t)(), Encryption(StaticKey(testKey(1))))
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	Register[LoginAccount](src, CRUD())
	if err := src.Migrate(bg); err != nil {
		t.Fatal(err)
	}
	tenant, err := src.Tenants().Create(bg, TenantInfo{Name: "acme"})
	if err != nil {
		t.Fatal(err)
	}
	ctxSrc := WithTenant(bg, tenant.ID)
	if err := Repo[LoginAccount](src).Insert(ctxSrc, &LoginAccount{Name: "a", Email: "Alice@Example.org"}); err != nil {
		t.Fatal(err)
	}
	var strom bytes.Buffer
	if err := src.Tenants().Export(bg, tenant.ID, &strom); err != nil {
		t.Fatal(err)
	}
	// Der Export enthält den Klartext (DSGVO-Auskunft), nie den Index.
	if !strings.Contains(strom.String(), "Alice@Example.org") || strings.Contains(strom.String(), "lookup") {
		t.Fatalf("Exportinhalt unerwartet: %s", strom.String())
	}

	dst, err := Open(newTestStore(t)(), Encryption(StaticKey(testKey(2)))) // anderer Schlüssel
	if err != nil {
		t.Fatal(err)
	}
	defer dst.Close()
	Register[LoginAccount](dst, CRUD())
	if err := dst.Migrate(bg); err != nil {
		t.Fatal(err)
	}
	if _, err := dst.Tenants().Create(bg, TenantInfo{ID: tenant.ID, Name: "acme"}); err != nil {
		t.Fatal(err)
	}
	if err := dst.Tenants().Import(bg, tenant.ID, bytes.NewReader(strom.Bytes())); err != nil {
		t.Fatalf("Import: %v", err)
	}
	ctxDst := WithTenant(bg, tenant.ID)
	got, err := Query[LoginAccount](dst, ctxDst).Where(Eq("Email", "alice@example.org")).First()
	if err != nil || got.Email != "Alice@Example.org" {
		t.Fatalf("Lookup nach Import: %+v (%v)", got, err)
	}

	// Der Index ist mit dem Ziel-Schlüssel berechnet — anders als in der Quelle.
	var srcIdx, dstIdx []byte
	if err := src.q().QueryRowContext(bg, `SELECT "email_lookup" FROM "login_account"`).Scan(&srcIdx); err != nil {
		t.Fatal(err)
	}
	if err := dst.q().QueryRowContext(bg, `SELECT "email_lookup" FROM "login_account"`).Scan(&dstIdx); err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(srcIdx, dstIdx) {
		t.Fatal("Index-Schlüssel haengt nicht am Hauptschluessel (Quelle und Ziel identisch)")
	}
}
