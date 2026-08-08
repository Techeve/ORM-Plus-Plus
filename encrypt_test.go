package orm

import (
	"bytes"
	"context"
	"fmt"
	"testing"
)

// EncryptFields: In-Place-Nachverschluesselung bestehender Klartext-Spalten.
// v1 schreibt Klartext, v2 traegt encrypted(+lookup)-Tags und laesst den
// Schritt laufen — danach liest das Model unveraendert, die DB haelt
// Ciphertext, und der Blind-Index ist befuellt.
func TestEncryptFieldsInPlace(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	bg := context.Background()
	ctx := WithTenant(bg, SingleTenant)
	const rows = 25

	// v1: Klartext-Modell mit Bestandszeilen.
	{
		type Wallet struct {
			ID     ID     `orm:"pk"`
			Owner  string `orm:"required"`
			Secret string
			Email  string
		}
		db, err := Open(store())
		if err != nil {
			t.Fatal(err)
		}
		Register[Wallet](db, CRUD())
		SchemaVersion(db, 1)
		if err := db.Migrate(bg); err != nil {
			t.Fatalf("Migrate v1: %v", err)
		}
		for i := 0; i < rows; i++ {
			w := &Wallet{Owner: fmt.Sprintf("o%02d", i), Secret: fmt.Sprintf("s3cr3t-%02d", i),
				Email: fmt.Sprintf("User%02d@Example.org", i)}
			if i == 3 {
				w.Secret, w.Email = "", "" // Leerwerte muessen ueberleben
			}
			if err := Repo[Wallet](db).Insert(ctx, w); err != nil {
				t.Fatal(err)
			}
		}
		db.Close()
	}

	type Wallet struct {
		ID     ID     `orm:"pk"`
		Owner  string `orm:"required"`
		Secret string `orm:"encrypted"`
		Email  string `orm:"encrypted,lookup"`
	}
	open := func() *DB {
		db, err := Open(store(), Encryption(StaticKey(testKey(4))))
		if err != nil {
			t.Fatal(err)
		}
		Register[Wallet](db, CRUD())
		SchemaVersion(db, 2)
		MigrationTo(db, 2, EncryptFields[Wallet]("Secret", "Email"))
		return db
	}

	// v2: Schritt laeuft in kleinen Batches (Checkpoints pro Batch).
	db := open()
	if err := db.Migrate(bg, MigrationPlan{BatchSize: 7}); err != nil {
		t.Fatalf("Migrate v2: %v", err)
	}

	// Model liest die Klartexte unveraendert.
	all, err := Query[Wallet](db, ctx).All()
	if err != nil || len(all) != rows {
		t.Fatalf("All: %d (%v)", len(all), err)
	}
	for _, w := range all {
		if w.Owner == "o05" && w.Secret != "s3cr3t-05" {
			t.Fatalf("Klartext nach Umzug falsch: %+v", w)
		}
		if w.Owner == "o03" && (w.Secret != "" || w.Email != "") {
			t.Fatalf("Leerwerte nach Umzug falsch: %+v", w)
		}
	}
	// Lookup funktioniert sofort (Index beim Umzug befuellt).
	got, err := Query[Wallet](db, ctx).Where(Eq("Email", "user07@example.org")).First()
	if err != nil || got.Owner != "o07" {
		t.Fatalf("Lookup nach Umzug: %+v (%v)", got, err)
	}
	// Die DB haelt Ciphertext, keinen Klartext.
	var rawSecret []byte
	if err := db.q().QueryRowContext(ctx,
		`SELECT "secret" FROM "wallet" WHERE "owner" = ?`, "o05").Scan(&rawSecret); err != nil {
		t.Fatal(err)
	}
	if len(rawSecret) < 2 || rawSecret[0] != cipherVersion || bytes.Contains(rawSecret, []byte("s3cr3t")) {
		t.Fatalf("secret-Spalte ist kein Ciphertext: %x", rawSecret[:min(8, len(rawSecret))])
	}
	db.Close()
}

// Abbruch und Wiederaufnahme: der Schritt setzt am gesicherten Checkpoint
// wieder auf (Zeilen davor werden nicht erneut angefasst) und ist ohne
// Checkpoint idempotent (gueltiger Ciphertext wird nie neu verschluesselt).
func TestEncryptFieldsResumeAndIdempotence(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	bg := context.Background()
	ctx := WithTenant(bg, SingleTenant)
	const rows = 12

	{
		type Locker2 struct {
			ID     ID     `orm:"pk"`
			Owner  string `orm:"required"`
			Secret string
		}
		db, err := Open(store())
		if err != nil {
			t.Fatal(err)
		}
		Register[Locker2](db, CRUD())
		SchemaVersion(db, 1)
		if err := db.Migrate(bg); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < rows; i++ {
			if err := Repo[Locker2](db).Insert(ctx, &Locker2{Owner: fmt.Sprintf("o%02d", i),
				Secret: fmt.Sprintf("plain-%02d", i)}); err != nil {
				t.Fatal(err)
			}
		}
		db.Close()
	}

	type Locker2 struct {
		ID     ID     `orm:"pk"`
		Owner  string `orm:"required"`
		Secret string `orm:"encrypted"`
	}
	open := func() *DB {
		db, err := Open(store(), Encryption(StaticKey(testKey(5))))
		if err != nil {
			t.Fatal(err)
		}
		Register[Locker2](db, CRUD())
		SchemaVersion(db, 2)
		MigrationTo(db, 2, EncryptFields[Locker2]("Secret"))
		return db
	}

	// Erster vollstaendiger Lauf, dann kuenstlich auf einen abgebrochenen
	// Stand zurueckdrehen: Zustand auf v1, ein Checkpoint mitten in der
	// Tabelle, die Zeilen HINTER dem Checkpoint wieder Klartext — und eine
	// Kanarien-Zeile VOR dem Checkpoint ebenfalls Klartext (sie darf beim
	// Wiederaufsetzen NICHT angefasst werden, das beweist den Checkpoint).
	db := open()
	if err := db.Migrate(bg, MigrationPlan{BatchSize: 5}); err != nil {
		t.Fatalf("Migrate v2: %v", err)
	}
	type rawRow struct {
		id     string
		secret []byte
	}
	var sorted []rawRow
	{
		rowsQ, err := db.q().QueryContext(ctx, `SELECT "id", "secret" FROM "locker2" ORDER BY "id"`)
		if err != nil {
			t.Fatal(err)
		}
		for rowsQ.Next() {
			var r rawRow
			if err := rowsQ.Scan(&r.id, &r.secret); err != nil {
				t.Fatal(err)
			}
			sorted = append(sorted, r)
		}
		rowsQ.Close()
		if len(sorted) != rows {
			t.Fatalf("Zeilen: %d", len(sorted))
		}
	}
	pivot := sorted[rows/2].id
	canary := sorted[2].id
	const step = "encrypt:Locker2.Secret"
	rewind := func(lastKey string, withCheckpoint bool) {
		for _, stmt := range []struct {
			sql  string
			args []any
		}{
			{`UPDATE ormpp_schema_state SET schema_version = 1, target_version = 0, phase = 'idle', models_checksum = '' WHERE id = 1`, nil},
			{`DELETE FROM ormpp_instances`, nil},
			{`DELETE FROM ormpp_migration_progress WHERE version = 2 AND step = ?`, []any{step}},
		} {
			if _, err := db.q().ExecContext(bg, stmt.sql, stmt.args...); err != nil {
				t.Fatalf("Rewind: %v", err)
			}
		}
		if withCheckpoint {
			if err := db.writeProgress(bg, db.q(), 2, step, lastKey, int64(rows/2), "running"); err != nil {
				t.Fatal(err)
			}
		}
	}
	plaintextify := func(ids ...string) {
		for _, id := range ids {
			var owner string
			if err := db.q().QueryRowContext(bg, `SELECT "owner" FROM "locker2" WHERE "id" = ?`, id).Scan(&owner); err != nil {
				t.Fatal(err)
			}
			if _, err := db.q().ExecContext(bg, `UPDATE "locker2" SET "secret" = ? WHERE "id" = ?`,
				[]byte("plain-"+owner[1:]), id); err != nil {
				t.Fatal(err)
			}
		}
	}
	rewind(pivot, true)
	var behind []string
	for _, r := range sorted {
		if r.id > pivot {
			behind = append(behind, r.id)
		}
	}
	plaintextify(append(behind, canary)...)
	db.Close()

	// Wiederaufnahme: nur die Zeilen hinter dem Checkpoint werden umgezogen.
	db = open()
	if err := db.Migrate(bg, MigrationPlan{BatchSize: 5}); err != nil {
		t.Fatalf("Migrate (Wiederaufnahme): %v", err)
	}
	var canarySecret []byte
	if err := db.q().QueryRowContext(bg, `SELECT "secret" FROM "locker2" WHERE "id" = ?`, canary).Scan(&canarySecret); err != nil {
		t.Fatal(err)
	}
	if len(canarySecret) > 0 && canarySecret[0] == cipherVersion {
		t.Fatal("Checkpoint ignoriert: Kanarien-Zeile vor dem Checkpoint wurde neu angefasst")
	}
	for _, id := range behind {
		var s []byte
		if err := db.q().QueryRowContext(bg, `SELECT "secret" FROM "locker2" WHERE "id" = ?`, id).Scan(&s); err != nil {
			t.Fatal(err)
		}
		if len(s) < 2 || s[0] != cipherVersion {
			t.Fatalf("Zeile %s hinter dem Checkpoint blieb Klartext", id)
		}
	}
	db.Close()

	// Voller idempotenter Lauf ohne Checkpoint: die Kanarien-Zeile wird
	// jetzt verschluesselt, schon gueltiger Ciphertext bleibt BYTE-GLEICH
	// (kein Nonce-Wechsel — der Schritt schreibt Erledigtes nicht neu).
	db = open()
	before := map[string][]byte{}
	{
		rowsQ, err := db.q().QueryContext(bg, `SELECT "id", "secret" FROM "locker2"`)
		if err != nil {
			t.Fatal(err)
		}
		for rowsQ.Next() {
			var id string
			var s []byte
			if err := rowsQ.Scan(&id, &s); err != nil {
				t.Fatal(err)
			}
			before[id] = append([]byte(nil), s...)
		}
		rowsQ.Close()
	}
	rewind("", false)
	if err := db.Migrate(bg, MigrationPlan{BatchSize: 5}); err != nil {
		t.Fatalf("Migrate (Idempotenz): %v", err)
	}
	all, err := Query[Locker2](db, ctx).All()
	if err != nil || len(all) != rows {
		t.Fatalf("All: %d (%v)", len(all), err)
	}
	for _, w := range all {
		if w.Secret != "plain-"+w.Owner[1:] {
			t.Fatalf("Klartext falsch: %+v", w)
		}
	}
	for id, prev := range before {
		if id == canary {
			continue
		}
		var s []byte
		if err := db.q().QueryRowContext(bg, `SELECT "secret" FROM "locker2" WHERE "id" = ?`, id).Scan(&s); err != nil {
			t.Fatal(err)
		}
		if prev[0] == cipherVersion && !bytes.Equal(prev, s) {
			t.Fatalf("Zeile %s wurde neu verschluesselt, obwohl schon Ciphertext vorlag", id)
		}
	}
	db.Close()
}
