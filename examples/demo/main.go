// Die ORM++-Beispielanwendung: eine Mini-DNS-Verwaltung, die jede Fähigkeit
// der Library einmal zeigt und erklärt, WARUM sie so benutzt wird.
//
// Aufbau: Das Programm simuliert zwei App-Generationen gegen dieselbe
// Datenbank-Datei — genau so läuft ORM++ im echten Leben:
//
//	Generation 1 (SchemaVersion 1): Betrieb — Tenants, CRUD, Queries,
//	    Transaktionen, Event Sourcing, Reaktoren, Geo, Verschlüsselung,
//	    DSGVO-Export/-Purge, Observability.
//	Generation 2 (SchemaVersion 2): Schema-Evolution — ReplaceModel mit
//	    Dual-Write, BatchScript, Event-Upcaster, FinalizeMigration.
//
// Starten:  go run ./examples/demo
//
// Die Demo läuft auf SQLite. Derselbe Code läuft unverändert gegen
// orm.Postgres(dsn) oder orm.Yugabyte(dsn) — das ist das Grundprinzip
// von ORM++: Verhaltensgleichheit. App-Code verzweigt NIE nach dem Backend.
package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	orm "gitlab.techeve.de/orm-plus-plus/orm-plus-plus"
)

// ============================================================================
// 1. MODELLE — die App deklariert Go-Structs, kein SQL, keine DB-Details.
// ============================================================================

// Betreiber ist ein einfaches CRUD-Model und dient als Referenz-Ziel.
type Betreiber struct {
	ID   orm.ID `orm:"pk"`       // Primärschlüssel: immer orm.ID (UUIDv7 —
	Name string `orm:"required"` // zeitlich sortierbar, clientseitig erzeugt,
} // kollisionsfrei über verteilte Instanzen)

// ProviderAccount zeigt die ganze Tag-Familie eines CRUD-Models.
type ProviderAccount struct {
	ID orm.ID `orm:"pk"`

	// index/unique: Sekundär- bzw. Unique-Index. Unique gilt automatisch
	// PRO TENANT — zwei Kunden dürfen dieselbe Mail-Adresse führen.
	Name string `orm:"index"`
	Mail string `orm:"unique,required"`

	// enum: Wertemenge, nativ als CHECK-Constraint UND engine-seitig geprüft
	// (ErrInvalidValue). default greift, wenn das Feld beim Insert leer ist.
	Status string `orm:"enum=aktiv|pausiert,default=aktiv"`

	// encrypted: AES-256-GCM, die DB sieht nur Ciphertext. Deshalb ist das
	// Feld bewusst NICHT filter-/sortier-/indizierbar. Der Schlüssel kommt
	// über orm.Encryption(...) bei Open; Rotation läuft über die Key-ID im
	// Ciphertext (lazy beim nächsten Schreiben).
	APIKey string `orm:"encrypted,required"`

	// ref: Referenz auf ein anderes Model — engine-geprüft (existiert das
	// Ziel? gehört es demselben Tenant?) UND als FK, wo die DB das nativ kann.
	// immutable = write-once: die Engine nimmt das Feld in kein UPDATE auf.
	BetreiberID orm.ID `orm:"ref=Betreiber,immutable,required"`

	// Pointer ⇒ NULL erlaubt. ondelete=setnull: fällt der Prüfer weg, wird
	// das Feld geleert statt den Account zu blockieren.
	Pruefer *orm.ID `orm:"ref=Betreiber,ondelete=setnull"`

	// json: verschachtelte Werte als JSON-Spalte (JSONB auf PG/YB).
	Labels []string `orm:"json"`

	// version: optimistisches Locking — Update mit veralteter Version
	// schlägt mit ErrVersionConflict fehl. Nur die Engine zählt hoch.
	Version int64 `orm:"version"`

	// deprecated: Feld ist zur Entfernung markiert (Expand/Contract).
	// Die Spalte fällt erst, wenn das Feld auch aus dem Struct entfernt
	// und die Migration finalisiert wurde — nie still.
	AltesFeld string `orm:"deprecated"`

	// autocreate/autoupdate: Zeitstempel pflegt die Engine.
	Angelegt  time.Time `orm:"autocreate"`
	Geaendert time.Time `orm:"autoupdate"`

	// "-": wird nicht persistiert.
	Notizen string `orm:"-"`
}

// AppConfig ist TenantFree: technische Tabelle ohne Nutzerdaten — keine
// Tenant-Spalte, kein Tenant-Filter, nutzbar ohne Tenant im Context.
// Das ist die dokumentierte AUSNAHME; Default ist tenant-gebunden.
type AppConfig struct {
	ID   orm.ID `orm:"pk"`
	Key  string `orm:"unique,required"`
	Wert string
}

// SyncProfile ist GeoFlexible: Heimatregion + lesende Replikate sind PRO
// DATENSATZ wählbar (orm.WithGeo + ReplicateTo beim Insert, später SetGeo).
type SyncProfile struct {
	ID   orm.ID `orm:"pk"`
	Name string `orm:"required"`
}

// ----------------------------------------------------------------------------
// Event-Sourcing-Model: DNSZone. Der Zustand ist die Faltung seiner Events.
// ----------------------------------------------------------------------------

// Record ist ein DNS-Eintrag (verschachtelt, landet als JSON im Read-Model).
type Record struct {
	Host string
	IP   string
	TTL  int
}

// DNSZone bettet orm.Aggregate ein — das bringt ID/Version/Load/Append/
// History/AtVersion/… von Haus aus mit. Die Struct-Felder bilden das
// READ-MODEL (eine Tabelle, gegen die der Query-Builder läuft); die Wahrheit
// ist der Event-Log.
type DNSZone struct {
	orm.Aggregate

	Name    string   `orm:"index,unique"`
	Records []Record `orm:"json"`
	Status  string   `orm:"index"`
}

// Die Event-Payloads: reine Daten-Structs, nur das Delta — nie der volle
// Zustand (der wohnt im Read-Model und in Snapshots).
type ZoneCreated struct{ Name string }
type RecordAddedV1 struct{ Host, IP string } // Format der Generation 1
type RecordAdded struct{ Record Record }     // Format der Generation 2 (mit TTL)
type RecordRemoved struct{ Host string }
type ZoneDisabled struct{}

// Apply ist die EINZIGE Pflicht des Entwicklers: eine pure Funktion, die ein
// Event in den Zustand faltet. Sie ist zugleich Projektions-, Rebuild- UND
// Snapshot-Logik — eine zweite "Snapshot-Funktion" gäbe nur Divergenz.
//
// Der RecordAddedV1-Zweig existiert hier nur, weil beide Generationen in
// einer Binary stecken: Generation 1 kennt nur v1. Nach dem Upgrade hebt der
// Upcaster (siehe Generation 2) alte v1-Events beim Lesen auf v2 — Apply
// bekommt dann nur noch RecordAdded zu sehen.
func (z *DNSZone) Apply(e orm.Event) error {
	switch ev := e.Payload.(type) {
	case ZoneCreated:
		z.Name, z.Status = ev.Name, "aktiv"
	case RecordAddedV1:
		z.Records = append(z.Records, Record{Host: ev.Host, IP: ev.IP, TTL: 300})
	case RecordAdded:
		z.Records = append(z.Records, ev.Record)
	case RecordRemoved:
		out := z.Records[:0]
		for _, r := range z.Records {
			if r.Host != ev.Host {
				out = append(out, r)
			}
		}
		z.Records = out
	case ZoneDisabled:
		z.Status = "deaktiviert"
	default:
		return fmt.Errorf("unbekanntes Event %T", e.Payload)
	}
	return nil
}

// ZoneStats ist eine abgeleitete Read-View, die ein OnEvent-Reaktor pflegt —
// ein ganz normales CRUD-Model.
type ZoneStats struct {
	ID         orm.ID `orm:"pk"`
	Zone       string `orm:"unique"` // Aggregat-ID der Zone
	Ereignisse int64
}

// SeenEvent ist der Idempotenz-Merker des Reaktors: OnEvent liefert
// at-least-once — Handler MÜSSEN Duplikate erkennen. Muster: die Event-ID
// (selbst eine UUID) als Primärschlüssel mitschreiben.
type SeenEvent struct {
	ID orm.ID `orm:"pk"`
}

// ============================================================================
// Gemeinsame Registrierung — beide Generationen deklarieren dieselbe Basis.
// In echt ist das eine Funktion, die App-Server UND Worker-Prozesse teilen.
// ============================================================================

func registriereBasis(db *orm.DB) {
	// Topologie: welche Regionen der Cluster hat. Auf SQLite kollabieren
	// alle Regionen physisch auf eine — die Deklaration bleibt gültig,
	// Daten-Geos werden weiter validiert (Verhaltensgleichheit). Auf
	// YugabyteDB partitionieren die Event-Tabellen nativ nach Geo.
	orm.Topology(db,
		orm.Region("eu-central", orm.Placement("demo.eu-central-1")),
		orm.Region("us-east", orm.Placement("demo.us-east-1")),
	)

	orm.Register[Betreiber](db, orm.CRUD())
	orm.Register[ProviderAccount](db, orm.CRUD(),
		// Zusammengesetzter Unique-Constraint (Feldnamen = Go-Feldnamen).
		// tenant_id wird automatisch einbezogen: Eindeutigkeit pro Tenant.
		orm.Unique("BetreiberID", "Name"),
		orm.Index("Status", "Angelegt"),
	)
	orm.Register[AppConfig](db, orm.CRUD(), orm.TenantFree())
	orm.Register[SyncProfile](db, orm.CRUD(), orm.GeoFlexible(orm.WriteForwarding()))
	orm.Register[ZoneStats](db, orm.CRUD())
	orm.Register[SeenEvent](db, orm.CRUD())
}

func main() {
	dir, err := os.MkdirTemp("", "ormpp-demo")
	must(err)
	defer func() { _ = os.RemoveAll(dir) }()
	pfad := filepath.Join(dir, "demo.db")

	// 32-Byte-Schlüssel für die Feld-Verschlüsselung. In echt kommt der aus
	// einem KMS/Secret-Store; orm.KeyProvider ist rotationsfähig.
	schluessel := bytes.Repeat([]byte{0x42}, 32)

	generation1(pfad, schluessel)
	generation2(pfad, schluessel)

	fmt.Println("\n✔ Demo komplett — dieselbe Datei, zwei App-Generationen, kein SQL geschrieben.")
}

// ============================================================================
// GENERATION 1 — der normale Betrieb.
// ============================================================================

func generation1(pfad string, schluessel []byte) {
	banner("Generation 1: Betrieb (SchemaVersion 1)")

	// --- Open: Verbindung + Optionen. Open ändert NIE das Schema ---------
	db, err := orm.Open(orm.SQLite(pfad),
		orm.Encryption(orm.StaticKey(schluessel)), // Pflicht wg. encrypted-Feldern
		orm.AppVersion("demo-1.0"),                // landet im Instanzregister
		orm.DefaultSnapshotEvery(100),             // globaler Snapshot-Default
	)
	must(err)
	defer db.Close() // meldet die Instanz ab, gibt Leases frei, stoppt Worker

	registriereBasis(db)

	// ES-Model: Events werden bei der Registrierung BENANNT und versioniert.
	// Der volle CloudEvents-Typ wird daraus abgeleitet (…zone.created.v1).
	orm.Register[DNSZone](db, orm.EventSourced(),
		orm.Events(
			orm.E[ZoneCreated]("zone.created"),
			orm.E[RecordAddedV1]("zone.record_added"), // Generation 1 = v1
			orm.E[RecordRemoved]("zone.record_removed"),
			orm.E[ZoneDisabled]("zone.disabled"),
		),
		orm.SnapshotEvery(5),    // alle 5 Events pro Aggregat ein Snapshot
		orm.SnapshotKeepLast(2), // ältere Snapshots werden weggeräumt
	)

	// Diese App-Ausgabe erwartet Schema-Version 1. Ändert jemand Modelle,
	// OHNE die Version zu erhöhen ⇒ Startfehler (ErrSchemaDrift) statt
	// stiller Schema-Änderung.
	orm.SchemaVersion(db, 1)

	// Migrate: Systemtabellen, Tenant-Register, Schema (additiv), Instanz-
	// Registrierung. Idempotent — jede Instanz ruft es, eine Lease
	// entscheidet, wer arbeitet.
	must(db.Migrate(context.Background()))

	// StartWorkers: Projektionen, OnEvent-Reaktoren, Snapshots, Archivierung.
	// Lease-koordiniert: pro Aufgabe arbeitet clusterweit genau eine Instanz.
	must(db.StartWorkers(context.Background()))

	// --- OnEvent: der VERLÄSSLICHE Pfad für abgeleitete Views -------------
	// Persistent, checkpointed, at-least-once, rebuildfähig. Der Handler
	// läuft transaktional (tx) — View-Update und Checkpoint fallen zusammen.
	orm.OnEvent[DNSZone](db, "zone.*", zoneStatistik, orm.Named("zonen-statistik"))

	// --- Tenant & Geo: der Pflicht-Scope jeder Operation ------------------
	// Beides hängt am context.Context, nie an Funktionssignaturen. Ohne
	// Tenant ⇒ ErrNoTenant (fail-closed): nichts läuft versehentlich
	// tenant-los. Bei >1 Region braucht jeder Schreibzugriff ein Daten-Geo.
	bg := context.Background()
	kunde, err := db.Tenants().Create(bg, orm.TenantInfo{Name: "ACME GmbH"})
	must(err)
	ctx := orm.WithTenant(bg, kunde.ID)
	ctx = orm.WithGeo(ctx, "eu-central") // Daten-Geo: wohin Datensätze gehören

	// Fail-closed live: ohne Tenant lehnt jede Operation ab.
	if err := orm.Repo[Betreiber](db).Insert(bg, &Betreiber{Name: "x"}); errors.Is(err, orm.ErrNoTenant) {
		fmt.Println("• fail-closed: Insert ohne Tenant ⇒", err)
	}

	// --- CRUD-Basics -------------------------------------------------------
	konten := orm.Repo[ProviderAccount](db)
	chef := &Betreiber{Name: "Ops-Team"}
	must(orm.Repo[Betreiber](db).Insert(ctx, chef))

	acc := &ProviderAccount{
		Name: "Cloudflare Prod", Mail: "ops@acme.example",
		APIKey: "cf-geheim-123", BetreiberID: chef.ID,
		Labels: []string{"prod", "dns"},
	}
	must(konten.Insert(ctx, acc)) // füllt ID, Angelegt, Status="aktiv" (default)
	fmt.Printf("• Insert: id=%s status=%q angelegt=%s\n", acc.ID, acc.Status, acc.Angelegt.Format(time.RFC3339))

	// Get liest entschlüsselt — die DB-Spalte enthält nur Ciphertext.
	geladen, err := konten.Get(ctx, acc.ID)
	must(err)
	fmt.Printf("• encrypted-Roundtrip: APIKey=%q (in der DB: AES-256-GCM-Blob)\n", geladen.APIKey)

	// Filter auf ein encrypted-Feld ist bewusst ein Fehler:
	if _, err := konten.Query(ctx).Where(orm.Eq("APIKey", "x")).All(); err != nil {
		fmt.Println("• encrypted ist nicht filterbar ⇒", err)
	}

	// Optimistisches Locking: zwei Bearbeiter, der zweite verliert.
	a, _ := konten.Get(ctx, acc.ID)
	b, _ := konten.Get(ctx, acc.ID)
	a.Name = "Cloudflare Production"
	must(konten.Update(ctx, a))
	b.Name = "Cloudflare PROD"
	if err := konten.Update(ctx, b); errors.Is(err, orm.ErrVersionConflict) {
		fmt.Println("• optimistisches Locking ⇒", err)
	}

	// Referenz-Integrität: kaputte Referenz wird engine-seitig abgelehnt.
	if err := konten.Insert(ctx, &ProviderAccount{
		Name: "x", Mail: "y@z.example", APIKey: "k", BetreiberID: orm.NewID(),
	}); errors.Is(err, orm.ErrInvalidReference) {
		fmt.Println("• Referenzprüfung ⇒", err)
	}

	// InsertMany: Default atomar; orm.Chunked(n) für große Volumina —
	// die Einfüge-Strategie wählt der Dialekt (SQLite: Prepared Statements
	// in einer Tx, PG: Multi-Row/COPY, YB: tablet-gerecht).
	var batch []*ProviderAccount
	for i := 0; i < 5; i++ {
		batch = append(batch, &ProviderAccount{
			Name: fmt.Sprintf("Provider %d", i), Mail: fmt.Sprintf("p%d@acme.example", i),
			APIKey: "k", BetreiberID: chef.ID,
		})
	}
	must(konten.InsertMany(ctx, batch, orm.Chunked(2)))

	// --- Query-Builder ------------------------------------------------------
	// Feldnamen sind GO-Feldnamen (die Engine mappt auf Spalten); unbekannte
	// Felder scheitern beim Bauen, nicht erst in der DB. Der Tenant-Filter
	// wird IMMER automatisch injiziert — er ist nicht abschaltbar.
	treffer, err := konten.Query(ctx).
		Where(orm.And(
			orm.Like("Name", "Provider%"),
			orm.In("Status", "aktiv", "pausiert"),
		)).
		OrderBy("Name", orm.Desc).
		Limit(3).
		All()
	must(err)
	fmt.Printf("• Query: %d Treffer (Like+In, sortiert, limitiert)\n", len(treffer))

	n, err := konten.Query(ctx).Count()
	must(err)
	fmt.Printf("• Count: %d Konten im Tenant\n", n)

	// Iter: Cursor-Streaming statt alles in den Speicher (iter.Seq2).
	namen := []string{}
	for k, err := range konten.Query(ctx).OrderBy("Name", orm.Asc).Iter() {
		must(err)
		namen = append(namen, k.Name)
	}
	fmt.Printf("• Iter: %s …\n", strings.Join(namen[:3], ", "))

	// Mengenbasiert: EIN Statement statt N Roundtrips.
	geaendert, err := konten.Query(ctx).
		Where(orm.Like("Name", "Provider%")).
		UpdateSet(orm.Set("Status", "pausiert"))
	must(err)
	geloescht, err := konten.Query(ctx).Where(orm.Eq("Status", "pausiert")).Delete()
	must(err)
	fmt.Printf("• UpdateSet: %d Zeilen pausiert, Delete: %d Zeilen entfernt\n", geaendert, geloescht)

	// --- Transaktionen & pessimistisches Sperren ---------------------------
	// db.Tx über mehrere Modelle; Fehler oder Panic ⇒ Rollback. GetForUpdate
	// sperrt die Zeile (PG/YB: SELECT … FOR UPDATE; SQLite emuliert über die
	// serialisierte Schreib-Connection — verhaltensgleich).
	must(db.Tx(ctx, func(tx orm.Tx) error {
		k, err := orm.Repo[ProviderAccount](tx).GetForUpdate(ctx, acc.ID)
		if err != nil {
			return err
		}
		k.Labels = append(k.Labels, "gesperrt-bearbeitet")
		return orm.Repo[ProviderAccount](tx).Update(ctx, k)
	}))
	fmt.Println("• Tx + GetForUpdate: Read-Modify-Write unter Zeilensperre")

	// --- TenantFree: funktioniert ganz ohne Tenant im Context --------------
	must(orm.Repo[AppConfig](db).Insert(orm.WithGeo(bg, "eu-central"),
		&AppConfig{Key: "wartungsfenster", Wert: "So 03:00"}))
	fmt.Println("• TenantFree-Model: Insert ohne Tenant im Context")

	// --- Event Sourcing -----------------------------------------------------
	// Neues Aggregat: orm.New vergibt die ID; das erste Append persistiert.
	// Append ist atomar und optimistisch: kam jemand dazwischen ⇒
	// ErrVersionConflict, dann Refresh + erneut anhängen.
	zone := orm.New[DNSZone](db)
	_, err = zone.Append(ctx, ZoneCreated{Name: "acme.example"})
	must(err)

	// Watch VOR den nächsten Events abonnieren: der flüchtige Live-Pfad für
	// UIs (SSE/WebSocket). Wer nicht zuhört, verpasst nichts Dauerhaftes —
	// Verlässlichkeit kommt aus OnEvent.
	wctx, wstop := context.WithCancel(ctx)
	live := orm.Watch[DNSZone](wctx, db)

	vorher := time.Now() // Zeitmarke für die Zeitreise unten
	time.Sleep(5 * time.Millisecond)

	pos, err := zone.Append(ctx,
		RecordAddedV1{Host: "www", IP: "203.0.113.10"},
		RecordAddedV1{Host: "mail", IP: "203.0.113.25"},
	)
	must(err)

	select {
	case ce := <-live:
		fmt.Printf("• Watch (live): %s seq=%d\n", ce.Type, ce.AggregateSeq)
	case <-time.After(3 * time.Second):
		fmt.Println("• Watch: nichts empfangen (unerwartet)")
	}
	wstop()

	// Read-your-writes: Load wartet, bis die eingebaute Projektion die
	// eigene Schreibposition erreicht hat — dann sieht auch der
	// Query-Builder (der gegen das Read-Model läuft) den neuen Stand.
	zone2, err := orm.Load[DNSZone](ctx, db, zone.ID(), orm.WaitFor(pos, 5*time.Second))
	must(err)
	fmt.Printf("• Load: %s v%d, %d Records, Status=%s\n",
		zone2.Name, zone2.Version(), len(zone2.Records), zone2.Status)

	aktive, err := orm.Query[DNSZone](db, ctx).Where(orm.Eq("Status", "aktiv")).Count()
	must(err)
	fmt.Printf("• Query aufs Read-Model: %d aktive Zonen\n", aktive)

	// Historie: der Event-Strom als CloudEvents — Audit ohne Extra-Arbeit.
	fmt.Print("• History: ")
	for ce, err := range zone2.History(ctx) {
		must(err)
		fmt.Printf("[%d]%s ", ce.AggregateSeq, kurzTyp(ce.Type))
	}
	fmt.Println()

	// Zeitreisen: Zustand nach Event N bzw. zu einem Zeitpunkt — transparent
	// über Snapshots und Archiv. (Rückgabe ist any, weil Go keine generischen
	// Methoden kennt — auf den Model-Typ casten.)
	alt, err := zone2.AtVersion(ctx, 1)
	must(err)
	fmt.Printf("• AtVersion(1): %d Records (vor den A-Records)\n", len(alt.(*DNSZone).Records))
	beiZeit, err := zone2.AtTime(ctx, vorher)
	must(err)
	fmt.Printf("• AtTime(t): %d Records zum Zeitpunkt t\n", len(beiZeit.(*DNSZone).Records))

	// Der Reaktor läuft asynchron — kurz warten, dann die View lesen.
	time.Sleep(600 * time.Millisecond)
	stats, err := orm.Query[ZoneStats](db, ctx).First()
	if err == nil {
		fmt.Printf("• OnEvent-View: Zone %s… hat %d Ereignisse gezählt\n", stats.Zone[:8], stats.Ereignisse)
	}

	// --- Geo: GeoFlexible pro Datensatz ------------------------------------
	// Heimat + lesende Replikate beim Insert; Umzug engine-geführt per
	// SetGeo. Auf SQLite sind das Platzierungs-Metadaten (alles kollabiert);
	// auf YugabyteDB wird daraus echtes Placement.
	sp := &SyncProfile{Name: "Weltweit"}
	must(orm.Repo[SyncProfile](db).Insert(
		orm.WithGeo(ctx, "eu-central", orm.ReplicateTo("us-east")), sp))
	must(orm.Repo[SyncProfile](db).SetGeo(ctx, sp.ID, "us-east", orm.ReplicateAll()))
	fmt.Println("• GeoFlexible: Heimat eu-central→us-east verlegt, ReplicateAll gesetzt")

	// Daten-Geo wird gegen die Topologie validiert:
	if err := orm.Repo[Betreiber](db).Insert(orm.WithGeo(ctx, "mars"), &Betreiber{Name: "?"}); errors.Is(err, orm.ErrRegionNotActive) {
		fmt.Println("• Geo-Validierung ⇒", err)
	}

	// --- DSGVO: Export & Purge ---------------------------------------------
	// Export: vollständiger Auszug als JSON Lines — alle Modelle, Events
	// (als CloudEvents), Snapshots; encrypted-Felder entschlüsselt.
	var auszug bytes.Buffer
	must(db.Tenants().Export(ctx, kunde.ID, &auszug))
	fmt.Printf("• Export: %d JSON-Zeilen (Tenant, Zeilen, Events, Snapshots)\n",
		strings.Count(auszug.String(), "\n"))

	// Purge: Recht auf Vergessenwerden — physisches Löschen über ALLE
	// Tabellen/Events/Snapshots/Archive. Zweistufig: erst archivieren
	// (blockiert neue Schreibvorgänge), dann purgen. Auditiert.
	probe, err := db.Tenants().Create(bg, orm.TenantInfo{Name: "Probekunde"})
	must(err)
	probeCtx := orm.WithGeo(orm.WithTenant(bg, probe.ID), "eu-central")
	must(orm.Repo[Betreiber](db).Insert(probeCtx, &Betreiber{Name: "Wegwerf"}))
	if err := db.Tenants().Purge(bg, probe.ID); errors.Is(err, orm.ErrTenantNotArchived) {
		fmt.Println("• Purge verlangt Archivierung ⇒", err)
	}
	must(db.Tenants().Archive(bg, probe.ID))
	must(db.Tenants().Purge(bg, probe.ID))
	fmt.Println("• Purge: Probekunde vollständig und auditiert gelöscht")

	// --- Observability ------------------------------------------------------
	// Die einzige Stelle, an der Backends sich unterscheiden DÜRFEN: der
	// Betreiber sieht die physische Wahrheit (SQLite: eine Region "local"
	// wäre es ohne Topologie; hier die deklarierten Regionen).
	h, err := db.Health(bg)
	must(err)
	fmt.Printf("• Health: %d Instanz(en), %d Projektions-Cursor, Regionen: %s\n",
		len(h.Instances), len(h.Projections), regionNamen(h.Regions))
}

// zoneStatistik ist der OnEvent-Handler: baut eine abgeleitete View.
// Zwei Stolpersteine, die JEDER Reaktor beachten muss:
//  1. at-least-once ⇒ idempotent arbeiten (hier: Event-ID-Merker).
//  2. Der Worker-Context trägt weder Tenant noch Geo — beides kommt aus
//     dem Event selbst (ce.Tenant, ce.Geo). Ohne das schlüge jeder Insert
//     fail-closed mit ErrNoTenant/ErrNoGeo fehl.
func zoneStatistik(ctx context.Context, ce orm.CloudEvent, tx orm.Tx) error {
	ctx = orm.WithGeo(orm.WithTenant(ctx, ce.Tenant), ce.Geo)

	// Schon gesehen? Dann ist dieses Event ein Redelivery — fertig.
	eventID, err := orm.ParseID(ce.ID)
	if err != nil {
		return err
	}
	seen := orm.Repo[SeenEvent](tx)
	if _, err := seen.Get(ctx, eventID); err == nil {
		return nil
	}
	if err := seen.Insert(ctx, &SeenEvent{ID: eventID}); err != nil {
		return err
	}

	stats := orm.Repo[ZoneStats](tx)
	s, err := stats.Query(ctx).Where(orm.Eq("Zone", ce.Subject)).First()
	switch {
	case errors.Is(err, orm.ErrNotFound):
		return stats.Insert(ctx, &ZoneStats{Zone: ce.Subject, Ereignisse: 1})
	case err != nil:
		return err
	default:
		s.Ereignisse++
		return stats.Update(ctx, s)
	}
}

// ============================================================================
// GENERATION 2 — Schema-Evolution: dieselbe Datei, neue App-Version.
// ============================================================================

func generation2(pfad string, schluessel []byte) {
	banner("Generation 2: Schema-Evolution (SchemaVersion 2)")

	// Kontakt existierte nur in Generation 1 — hier die EINGEFRORENE Kopie
	// fürs Lesen der alten Tabelle. Konvention: Der Go-Name trägt ein
	// V-Suffix; "KontaktV1" liest die Tabelle des früheren Models "Kontakt".
	type KontaktV1 struct {
		ID      orm.ID `orm:"pk"`
		Name    string `orm:"required"`
		Strasse string
		Ort     string
	}
	// Das neue Model, das Kontakt ersetzt (Straße+Ort verschmolzen).
	type Adresskarte struct {
		ID      orm.ID `orm:"pk"`
		Name    string `orm:"required"`
		Adresse string
	}

	bg := context.Background()

	// Zwischenspiel: Generation 1 hätte "Kontakt" von Anfang an gehabt —
	// wir legen die Alt-Tabelle samt Daten nachträglich an, um den Umbau
	// ehrlich zu zeigen (eigene Mini-Generation 1b mit Version 1 → Drift-
	// Schutz verlangt sonst eine Versions-Erhöhung; hier: 1 → 2 unten).
	{
		type Kontakt struct {
			ID      orm.ID `orm:"pk"`
			Name    string `orm:"required"`
			Strasse string
			Ort     string
		}
		db1, err := orm.Open(orm.SQLite(pfad), orm.Encryption(orm.StaticKey(schluessel)))
		must(err)
		registriereBasis(db1)
		orm.Register[DNSZone](db1, orm.EventSourced(), orm.Events(
			orm.E[ZoneCreated]("zone.created"),
			orm.E[RecordAddedV1]("zone.record_added"),
			orm.E[RecordRemoved]("zone.record_removed"),
			orm.E[ZoneDisabled]("zone.disabled"),
		), orm.SnapshotEvery(5))
		orm.Register[Kontakt](db1, orm.CRUD())
		orm.SchemaVersion(db1, 2) // Model kam dazu ⇒ Version rauf (Drift-Schutz!)
		must(db1.Migrate(bg))
		must(db1.FinalizeMigration(bg, 2)) // rein additiv ⇒ sofort finalisierbar

		ctx := orm.WithGeo(orm.WithTenant(bg, ersterTenant(db1)), "eu-central")
		must(orm.Repo[Kontakt](db1).Insert(ctx, &Kontakt{Name: "Alba", Strasse: "Allee 1", Ort: "Aachen"}))
		must(orm.Repo[Kontakt](db1).Insert(ctx, &Kontakt{Name: "Bruno", Strasse: "Berg 2", Ort: "Bonn"}))
		db1.Close()
		fmt.Println("• Vorbereitung: Model 'Kontakt' mit 2 Zeilen (Version 2)")
	}

	// --- Die neue App-Version ----------------------------------------------
	db, err := orm.Open(orm.SQLite(pfad),
		orm.Encryption(orm.StaticKey(schluessel)),
		orm.AppVersion("demo-2.0"),
		orm.MigrationRole(orm.MigrationWorker), // darf Backfill-Shards übernehmen
	)
	must(err)
	defer db.Close()

	registriereBasis(db)

	// Event-Schema-Evolution: record_added ist jetzt v2 (Record mit TTL).
	// Alte v1-Events bleiben unveränderlich in der DB — der Upcaster hebt
	// sie BEIM LESEN auf v2. Fehlt er, schlägt Migrate fehl (Startfehler
	// statt Lesefehler).
	orm.Register[DNSZone](db, orm.EventSourced(), orm.Events(
		orm.E[ZoneCreated]("zone.created"),
		orm.E[RecordAdded]("zone.record_added", orm.V(2)),
		orm.E[RecordRemoved]("zone.record_removed"),
		orm.E[ZoneDisabled]("zone.disabled"),
	), orm.SnapshotEvery(5))
	orm.Upcast(db, "zone.record_added", 1, func(alt RecordAddedV1) (RecordAdded, error) {
		return RecordAdded{Record: Record{Host: alt.Host, IP: alt.IP, TTL: 300}}, nil
	})

	// Das neue Model + der Migrationsschritt von Version 2 nach 3.
	orm.Register[Adresskarte](db, orm.CRUD())
	orm.SchemaVersion(db, 3)
	orm.MigrationTo(db, 3,
		// ReplaceModel: Adresskarte ersetzt Kontakt, Zeile für Zeile
		// transformiert. Identität (ID), Tenant und Geo bleiben erhalten.
		// Während des Dual-Write ziehen Trigger die Schreibvorgänge ALTER
		// Instanzen laufend in die neue Tabelle nach.
		orm.ReplaceModel[KontaktV1, Adresskarte](func(_ context.Context, alt KontaktV1) (Adresskarte, error) {
			return Adresskarte{Name: alt.Name, Adresse: alt.Strasse + ", " + alt.Ort}, nil
		}),
		// BatchScript: freies, checkpointed Migrationsskript — bei Absturz
		// setzt es am gesicherten Fortschritt wieder auf.
		orm.BatchScript("statistik-neu", func(ctx context.Context, b orm.Batch) error {
			cp, err := b.Checkpoint(ctx)
			if err != nil {
				return err
			}
			if cp == "" {
				// … hier würden Daten häppchenweise umgebaut …
				return b.SaveCheckpoint(ctx, "fertig", 0)
			}
			return nil
		}),
	)

	// Migrate fährt die Online-Zustandsmaschine: expanding (additive DDL,
	// Trigger) → backfill (checkpointed) → dual-write. Alte Instanzen
	// laufen währenddessen unbeeinträchtigt weiter.
	must(db.Migrate(bg, orm.MigrationPlan{BatchSize: 500}))

	st, err := db.MigrationStatus(bg)
	must(err)
	fmt.Printf("• MigrationStatus: Phase=%s v%d→v%d, Fortschritt local=%.0f%%\n",
		st.Phase, st.CurrentVersion, st.TargetVersion, st.Geo["local"].Percent)

	// Finalize ist EXPLIZIT und verweigert, solange eine lebende Instanz
	// eine ältere Schema-Version meldet (Instanzregister). Dann: Dual-Write
	// beenden, Alt-Tabelle und entfernte deprecated-Spalten droppen.
	must(db.FinalizeMigration(bg, 3))
	st, _ = db.MigrationStatus(bg)
	fmt.Printf("• FinalizeMigration: Phase=%s, aktuelle Version=%d — Alt-Tabelle 'kontakt' ist weg\n",
		st.Phase, st.CurrentVersion)

	// Der Umbau ist angekommen:
	ctx := orm.WithGeo(orm.WithTenant(bg, ersterTenant(db)), "eu-central")
	karten, err := orm.Query[Adresskarte](db, ctx).OrderBy("Name", orm.Asc).All()
	must(err)
	for _, k := range karten {
		fmt.Printf("• Adresskarte: %-6s %s\n", k.Name, k.Adresse)
	}

	// Und der Upcaster hebt die alten v1-Events beim Laden transparent:
	zonen, err := orm.Query[DNSZone](db, ctx).All()
	must(err)
	if len(zonen) > 0 {
		zone, err := orm.Load[DNSZone](ctx, db, zonen[0].ID())
		must(err)
		fmt.Printf("• Upcaster: Zone %s geladen, Records (v1→v2 gehoben): %+v\n", zone.Name, zone.Records)
	}
}

// ============================================================================
// Kleinkram
// ============================================================================

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func banner(s string) {
	fmt.Printf("\n════ %s ════\n", s)
}

func kurzTyp(voll string) string {
	teile := strings.Split(voll, ".")
	if len(teile) < 3 {
		return voll
	}
	return strings.Join(teile[len(teile)-3:], ".")
}

func regionNamen(rs []orm.RegionInfo) string {
	var n []string
	for _, r := range rs {
		n = append(n, r.Name)
	}
	return strings.Join(n, ", ")
}

// ersterTenant liefert den in Generation 1 angelegten Kunden-Tenant.
// (orm.SingleTenant wäre der eingebaute Default für Single-Tenant-Apps.)
func ersterTenant(db *orm.DB) orm.ID {
	list, err := db.Tenants().List(context.Background())
	must(err)
	for _, t := range list {
		if t.ID != orm.SingleTenant && t.Status == "active" {
			return t.ID
		}
	}
	return orm.SingleTenant
}
