package orm

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

// Testmodelle — decken die Tag-Familie ab.

type User struct {
	ID        ID        `orm:"pk"`
	Email     string    `orm:"unique,required"`
	Name      string    `orm:"index"`
	CreatedAt time.Time `orm:"autocreate"`
}

type Document struct {
	ID        ID        `orm:"pk"`
	Title     string    `orm:"required"`
	Status    string    `orm:"enum=draft|published|archived,default=draft"`
	CreatedBy ID        `orm:"ref=User,immutable,required"`
	Reviewer  *ID       `orm:"ref=User,ondelete=setnull"`
	Labels    []string  `orm:"json"`
	Version   int64     `orm:"version"`
	CreatedAt time.Time `orm:"autocreate"`
	UpdatedAt time.Time `orm:"autoupdate"`
	Internal  string    `orm:"-"`
}

type SysConfig struct {
	ID    ID     `orm:"pk"`
	Key   string `orm:"unique,required"`
	Value string
}

func testDB(t *testing.T) (*DB, context.Context) {
	t.Helper()
	db, err := Open(SQLite(filepath.Join(t.TempDir(), "test.db")))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	Register[User](db, CRUD())
	Register[Document](db, CRUD(), Unique("CreatedBy", "Title"))
	Register[SysConfig](db, CRUD(), TenantFree())

	if err := db.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	ctx := WithTenant(context.Background(), SingleTenant)
	return db, ctx
}

func mustInsertUser(t *testing.T, db *DB, ctx context.Context, email string) *User {
	t.Helper()
	u := &User{Email: email, Name: "Test"}
	if err := Repo[User](db).Insert(ctx, u); err != nil {
		t.Fatalf("Insert User: %v", err)
	}
	return u
}

func TestInsertGetRoundtrip(t *testing.T) {
	db, ctx := testDB(t)
	u := mustInsertUser(t, db, ctx, "a@example.org")

	if u.ID.IsZero() {
		t.Fatal("Insert hat keine ID vergeben")
	}
	if u.CreatedAt.IsZero() {
		t.Fatal("autocreate nicht gefüllt")
	}

	got, err := Repo[User](db).Get(ctx, u.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Email != "a@example.org" || got.Name != "Test" {
		t.Fatalf("Roundtrip verfälscht: %+v", got)
	}
}

func TestTenantScopeFailClosed(t *testing.T) {
	db, _ := testDB(t)
	noTenant := context.Background()

	if err := Repo[User](db).Insert(noTenant, &User{Email: "x@example.org"}); !errors.Is(err, ErrNoTenant) {
		t.Fatalf("Insert ohne Tenant: %v, erwartet ErrNoTenant", err)
	}
	if _, err := Repo[User](db).Query(noTenant).All(); !errors.Is(err, ErrNoTenant) {
		t.Fatalf("Query ohne Tenant: %v, erwartet ErrNoTenant", err)
	}
}

func TestTenantIsolationAlsoByID(t *testing.T) {
	db, ctx := testDB(t)
	u := mustInsertUser(t, db, ctx, "a@example.org")

	other, err := db.Tenants().Create(context.Background(), TenantInfo{Name: "Zweiter"})
	if err != nil {
		t.Fatalf("Tenant anlegen: %v", err)
	}
	otherCtx := WithTenant(context.Background(), other.ID)

	// Fremder Tenant sieht den Datensatz auch per ID nicht.
	if _, err := Repo[User](db).Get(otherCtx, u.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get über Tenant-Grenze: %v, erwartet ErrNotFound", err)
	}
	if err := Repo[User](db).Delete(otherCtx, u.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Delete über Tenant-Grenze: %v, erwartet ErrNotFound", err)
	}
}

func TestUnknownTenantRejected(t *testing.T) {
	db, _ := testDB(t)
	ghost := WithTenant(context.Background(), NewID())
	if err := Repo[User](db).Insert(ghost, &User{Email: "g@example.org"}); !errors.Is(err, ErrUnknownTenant) {
		t.Fatalf("Insert mit erfundenem Tenant: %v, erwartet ErrUnknownTenant", err)
	}
}

func TestArchivedTenantBlocksWrites(t *testing.T) {
	db, _ := testDB(t)
	ten, _ := db.Tenants().Create(context.Background(), TenantInfo{Name: "Kunde"})
	ctx := WithTenant(context.Background(), ten.ID)
	u := mustInsertUser(t, db, ctx, "k@example.org")

	if err := db.Tenants().Archive(context.Background(), ten.ID); err != nil {
		t.Fatalf("Archive: %v", err)
	}
	if err := Repo[User](db).Insert(ctx, &User{Email: "neu@example.org"}); !errors.Is(err, ErrUnknownTenant) {
		t.Fatalf("Insert auf archivierten Tenant: %v, erwartet ErrUnknownTenant", err)
	}
	// Bestandsdaten bleiben lesbar.
	if _, err := Repo[User](db).Get(ctx, u.ID); err != nil {
		t.Fatalf("Lesen nach Archivierung: %v", err)
	}
}

func TestRequiredEnumDefault(t *testing.T) {
	db, ctx := testDB(t)
	u := mustInsertUser(t, db, ctx, "a@example.org")
	docs := Repo[Document](db)

	if err := docs.Insert(ctx, &Document{CreatedBy: u.ID}); !errors.Is(err, ErrRequiredField) {
		t.Fatalf("Insert ohne Title: %v, erwartet ErrRequiredField", err)
	}
	if err := docs.Insert(ctx, &Document{Title: "T", CreatedBy: u.ID, Status: "bogus"}); !errors.Is(err, ErrInvalidValue) {
		t.Fatalf("Insert mit ungültigem enum: %v, erwartet ErrInvalidValue", err)
	}

	d := &Document{Title: "T", CreatedBy: u.ID, Labels: []string{"a", "b"}}
	if err := docs.Insert(ctx, d); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	got, _ := docs.Get(ctx, d.ID)
	if got.Status != "draft" {
		t.Fatalf("default nicht angewandt: %q", got.Status)
	}
	if len(got.Labels) != 2 || got.Labels[0] != "a" {
		t.Fatalf("json-Roundtrip verfälscht: %+v", got.Labels)
	}
}

func TestReferences(t *testing.T) {
	db, ctx := testDB(t)
	u := mustInsertUser(t, db, ctx, "a@example.org")
	docs := Repo[Document](db)

	// Referenz auf nicht existierenden User.
	if err := docs.Insert(ctx, &Document{Title: "T", CreatedBy: NewID()}); !errors.Is(err, ErrInvalidReference) {
		t.Fatalf("Insert mit kaputter Referenz: %v, erwartet ErrInvalidReference", err)
	}

	d := &Document{Title: "T", CreatedBy: u.ID}
	if err := docs.Insert(ctx, d); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	// restrict: User wird referenziert, Löschen verweigert.
	if err := Repo[User](db).Delete(ctx, u.ID); !errors.Is(err, ErrReferenceInUse) {
		t.Fatalf("Delete referenzierten Users: %v, erwartet ErrReferenceInUse", err)
	}

	// setnull: Reviewer löschen setzt Feld auf NULL.
	rev := mustInsertUser(t, db, ctx, "rev@example.org")
	d2 := &Document{Title: "R", CreatedBy: u.ID, Reviewer: &rev.ID}
	if err := docs.Insert(ctx, d2); err != nil {
		t.Fatalf("Insert mit Reviewer: %v", err)
	}
	if err := Repo[User](db).Delete(ctx, rev.ID); err != nil {
		t.Fatalf("Delete Reviewer: %v", err)
	}
	got, _ := docs.Get(ctx, d2.ID)
	if got.Reviewer != nil {
		t.Fatalf("ondelete=setnull nicht angewandt: %v", got.Reviewer)
	}
}

func TestOptimisticLocking(t *testing.T) {
	db, ctx := testDB(t)
	u := mustInsertUser(t, db, ctx, "a@example.org")
	docs := Repo[Document](db)
	d := &Document{Title: "T", CreatedBy: u.ID}
	if err := docs.Insert(ctx, d); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	a, _ := docs.Get(ctx, d.ID)
	b, _ := docs.Get(ctx, d.ID)
	a.Title = "A"
	if err := docs.Update(ctx, a); err != nil {
		t.Fatalf("Update a: %v", err)
	}
	b.Title = "B"
	if err := docs.Update(ctx, b); !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("Update b: %v, erwartet ErrVersionConflict", err)
	}
}

func TestImmutableExcludedFromUpdate(t *testing.T) {
	db, ctx := testDB(t)
	u := mustInsertUser(t, db, ctx, "a@example.org")
	u2 := mustInsertUser(t, db, ctx, "b@example.org")
	docs := Repo[Document](db)
	d := &Document{Title: "T", CreatedBy: u.ID}
	_ = docs.Insert(ctx, d)

	d.CreatedBy = u2.ID // Versuch, immutable zu ändern
	d.Title = "T2"
	if err := docs.Update(ctx, d); err != nil {
		t.Fatalf("Update: %v", err)
	}
	got, _ := docs.Get(ctx, d.ID)
	if got.CreatedBy != u.ID {
		t.Fatal("immutable-Feld wurde geändert")
	}
	if got.Title != "T2" {
		t.Fatal("normales Feld wurde nicht geändert")
	}
}

func TestQueryBuilder(t *testing.T) {
	db, ctx := testDB(t)
	u := mustInsertUser(t, db, ctx, "a@example.org")
	docs := Repo[Document](db)
	for _, title := range []string{"Alpha", "Beta", "Gamma"} {
		if err := docs.Insert(ctx, &Document{Title: title, CreatedBy: u.ID}); err != nil {
			t.Fatalf("Insert %s: %v", title, err)
		}
	}
	_, _ = docs.Query(ctx).Where(Eq("Title", "Beta")).UpdateSet(Set("Status", "published"))

	list, err := docs.Query(ctx).
		Where(And(Eq("CreatedBy", u.ID), Ne("Status", "archived"))).
		OrderBy("Title", Desc).Limit(2).All()
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(list) != 2 || list[0].Title != "Gamma" {
		t.Fatalf("Query-Ergebnis falsch: %+v", list)
	}

	n, err := docs.Query(ctx).Where(Eq("Status", "draft")).Count()
	if err != nil || n != 2 {
		t.Fatalf("Count: %d, %v — erwartet 2", n, err)
	}

	first, err := docs.Query(ctx).Where(Like("Title", "Al%")).First()
	if err != nil || first.Title != "Alpha" {
		t.Fatalf("First: %+v, %v", first, err)
	}

	if _, err := docs.Query(ctx).Where(Eq("Unbekannt", 1)).All(); err == nil {
		t.Fatal("unbekanntes Feld nicht abgelehnt")
	}

	var streamed int
	for _, err := range docs.Query(ctx).Iter() {
		if err != nil {
			t.Fatalf("Iter: %v", err)
		}
		streamed++
	}
	if streamed != 3 {
		t.Fatalf("Iter: %d Zeilen, erwartet 3", streamed)
	}

	deleted, err := docs.Query(ctx).Where(In("Title", "Alpha", "Beta")).Delete()
	if err != nil || deleted != 2 {
		t.Fatalf("Bulk-Delete: %d, %v", deleted, err)
	}
}

func TestInsertManyChunked(t *testing.T) {
	db, ctx := testDB(t)
	var users []*User
	for i := 0; i < 25; i++ {
		users = append(users, &User{Email: string(rune('a'+i%26)) + string(rune('0'+i/26)) + "@example.org", Name: "Bulk"})
	}
	if err := Repo[User](db).InsertMany(ctx, users, Chunked(10)); err != nil {
		t.Fatalf("InsertMany: %v", err)
	}
	n, _ := Repo[User](db).Query(ctx).Where(Eq("Name", "Bulk")).Count()
	if n != 25 {
		t.Fatalf("InsertMany: %d Zeilen, erwartet 25", n)
	}
}

func TestUniqueCompositePerTenant(t *testing.T) {
	db, ctx := testDB(t)
	u := mustInsertUser(t, db, ctx, "a@example.org")
	docs := Repo[Document](db)
	if err := docs.Insert(ctx, &Document{Title: "Same", CreatedBy: u.ID}); err != nil {
		t.Fatalf("Insert 1: %v", err)
	}
	if err := docs.Insert(ctx, &Document{Title: "Same", CreatedBy: u.ID}); err == nil {
		t.Fatal("Composite-Unique (CreatedBy,Title) nicht durchgesetzt")
	}
}

func TestTxRollbackAndGetForUpdate(t *testing.T) {
	db, ctx := testDB(t)
	u := mustInsertUser(t, db, ctx, "a@example.org")
	users := Repo[User](db)

	// GetForUpdate außerhalb einer Tx.
	if _, err := users.GetForUpdate(ctx, u.ID); !errors.Is(err, ErrRequiresTx) {
		t.Fatalf("GetForUpdate ohne Tx: %v, erwartet ErrRequiresTx", err)
	}

	// Rollback bei Fehler.
	boom := errors.New("boom")
	err := db.Tx(ctx, func(tx Tx) error {
		if err := Repo[User](tx).Insert(ctx, &User{Email: "tx@example.org"}); err != nil {
			return err
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("Tx: %v", err)
	}
	if _, err := users.Query(ctx).Where(Eq("Email", "tx@example.org")).First(); !errors.Is(err, ErrNotFound) {
		t.Fatal("Rollback hat Insert nicht zurückgenommen")
	}

	// GetForUpdate + Update in Tx.
	err = db.Tx(ctx, func(tx Tx) error {
		got, err := Repo[User](tx).GetForUpdate(ctx, u.ID)
		if err != nil {
			return err
		}
		got.Name = "Locked"
		return Repo[User](tx).Update(ctx, got)
	})
	if err != nil {
		t.Fatalf("Tx mit GetForUpdate: %v", err)
	}
	got, _ := users.Get(ctx, u.ID)
	if got.Name != "Locked" {
		t.Fatal("Update in Tx nicht sichtbar")
	}
}

func TestTenantFreeModel(t *testing.T) {
	db, _ := testDB(t)
	noTenant := context.Background()
	cfg := Repo[SysConfig](db)

	c := &SysConfig{Key: "log_level", Value: "info"}
	if err := cfg.Insert(noTenant, c); err != nil {
		t.Fatalf("TenantFree-Insert ohne Tenant: %v", err)
	}
	got, err := cfg.Query(noTenant).Where(Eq("Key", "log_level")).First()
	if err != nil || got.Value != "info" {
		t.Fatalf("TenantFree-Query: %+v, %v", got, err)
	}
}

func TestSchemaDriftAndAdditiveDiff(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "drift.db")

	open := func() *DB {
		db, err := Open(SQLite(path))
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		return db
	}

	// v1: nur User.
	db1 := open()
	Register[User](db1, CRUD())
	if err := db1.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate v1: %v", err)
	}
	ctx := WithTenant(context.Background(), SingleTenant)
	u := mustInsertUser(t, db1, ctx, "a@example.org")
	db1.Close()

	// Drift: Model geändert (zusätzliches Model) ohne Versions-Erhöhung.
	db2 := open()
	Register[User](db2, CRUD())
	Register[SysConfig](db2, CRUD(), TenantFree())
	if err := db2.Migrate(context.Background()); !errors.Is(err, ErrSchemaDrift) {
		t.Fatalf("Drift: %v, erwartet ErrSchemaDrift", err)
	}
	db2.Close()

	// Mit Versions-Erhöhung: additiver Diff läuft durch, Bestandsdaten bleiben.
	db3 := open()
	Register[User](db3, CRUD())
	Register[SysConfig](db3, CRUD(), TenantFree())
	SchemaVersion(db3, 2)
	if err := db3.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate v2: %v", err)
	}
	if _, err := Repo[User](db3).Get(ctx, u.ID); err != nil {
		t.Fatalf("Bestandsdaten nach Migration: %v", err)
	}
	db3.Close()
}

func TestTopologyGeoValidation(t *testing.T) {
	db, err := Open(SQLite(filepath.Join(t.TempDir(), "geo.db")))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	Register[User](db, CRUD())
	Topology(db, Region("eu-central"), Region("us-east"))
	if err := db.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	ctx := WithTenant(context.Background(), SingleTenant)

	// Ohne Daten-Geo bei Mehr-Regionen-Topologie: ErrNoGeo.
	if err := Repo[User](db).Insert(ctx, &User{Email: "x@example.org"}); !errors.Is(err, ErrNoGeo) {
		t.Fatalf("Insert ohne Geo: %v, erwartet ErrNoGeo", err)
	}
	// Unbekannte Region: ErrRegionNotActive.
	bad := WithGeo(ctx, "mars")
	if err := Repo[User](db).Insert(bad, &User{Email: "x@example.org"}); !errors.Is(err, ErrRegionNotActive) {
		t.Fatalf("Insert mit unbekannter Region: %v, erwartet ErrRegionNotActive", err)
	}
	// Deklarierte Region: funktioniert (physisch kollabiert — Verhaltensgleichheit).
	eu := WithGeo(ctx, "eu-central")
	if err := Repo[User](db).Insert(eu, &User{Email: "x@example.org"}); err != nil {
		t.Fatalf("Insert mit gültiger Region: %v", err)
	}
}

func TestUpsert(t *testing.T) {
	db, ctx := testDB(t)
	u := mustInsertUser(t, db, ctx, "a@example.org")
	docs := Repo[Document](db)
	d := &Document{Title: "T", CreatedBy: u.ID}
	if err := docs.Upsert(ctx, d); err != nil {
		t.Fatalf("Upsert (insert): %v", err)
	}
	d.Status = "published"
	if err := docs.Upsert(ctx, d); err != nil {
		t.Fatalf("Upsert (update): %v", err)
	}
	got, _ := docs.Get(ctx, d.ID)
	if got.Status != "published" {
		t.Fatalf("Upsert-Update nicht angewandt: %q", got.Status)
	}
	if n, _ := docs.Query(ctx).Count(); n != 1 {
		t.Fatalf("Upsert hat dupliziert: %d Zeilen", n)
	}
}
