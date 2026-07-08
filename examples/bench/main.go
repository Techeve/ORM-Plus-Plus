// ORM++-Benchmark: dasselbe Szenario gegen SQLite, PostgreSQL und
// YugabyteDB — misst Latenzen (p50/p95/p99) und Durchsatz der wichtigsten
// Operationen und schreibt am Ende einen strukturierten Bericht.
//
// Starten (SQLite läuft immer; PG/YB nur mit DSN — siehe docker-compose.yml):
//
//	go run ./examples/bench
//	go run ./examples/bench \
//	    -postgres "postgres://orm:orm@localhost:5433/orm" \
//	    -yugabyte "postgres://yugabyte@localhost:5434/yugabyte" \
//	    -scale 300
//
// Berichte:
//   - Konsole: Tabelle pro Backend + Durchsatz-Vergleich
//   - report.json: strukturiert für Weiterverarbeitung/Archivierung
//   - bench.txt: Go-Benchmark-Format — DER etablierte Standard für
//     Performance-Daten im Go-Umfeld; Läufe vergleichen mit
//     `go install golang.org/x/perf/cmd/benchstat@latest && benchstat alt.txt neu.txt`
//
// Das Szenario deckt die ROADMAP-Lasttest-Punkte ab: Append-Durchsatz,
// Projektions-Lag, CRUD-Pfade. Gemessen wird SINGLE-THREADED — so bleiben
// die Latenzen vergleichbar und der Unterschied zwischen den Backends
// (SQLite: in-process; PG: 1 Netz-Roundtrip; YB: Konsens über den Cluster)
// wird nicht durch Parallelität verwischt.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	orm "gitlab.techeve.de/orm-plus-plus/orm-plus-plus"

	_ "github.com/jackc/pgx/v5/stdlib" // Admin-Verbindung für Schema-Isolation
)

// ============================================================================
// Bench-Modelle: ein CRUD-Konto und ein event-sourced Zähler.
// ============================================================================

type BenchKonto struct {
	ID       orm.ID    `orm:"pk"`
	Name     string    `orm:"index"`
	Mail     string    `orm:"unique,required"`
	Stand    int64     ``
	Labels   []string  `orm:"json"`
	Version  int64     `orm:"version"`
	Angelegt time.Time `orm:"autocreate"`
}

type BenchZaehler struct {
	orm.Aggregate
	Stand int64 `orm:"index"`
}

type Erhoeht struct{ Um int64 }

func (z *BenchZaehler) Apply(e orm.Event) error {
	if ev, ok := e.Payload.(Erhoeht); ok {
		z.Stand += ev.Um
	}
	return nil
}

// ============================================================================
// Messwerk
// ============================================================================

// Ergebnis ist eine Messreihe im JSON-Bericht.
type Ergebnis struct {
	Name      string  `json:"name"`
	Ops       int     `json:"ops"`
	TotalMs   float64 `json:"total_ms"`
	OpsProSek float64 `json:"ops_per_sec"`
	P50Us     float64 `json:"p50_us,omitempty"`
	P95Us     float64 `json:"p95_us,omitempty"`
	P99Us     float64 `json:"p99_us,omitempty"`
	MaxUs     float64 `json:"max_us,omitempty"`
}

// BackendBericht sammelt alle Messreihen eines Backends.
type BackendBericht struct {
	Backend    string     `json:"backend"`
	Ergebnisse []Ergebnis `json:"results"`
}

// Bericht ist die Wurzel von report.json.
type Bericht struct {
	Zeitpunkt  time.Time        `json:"timestamp"`
	Skalierung int              `json:"scale"`
	GoVersion  string           `json:"go_version"`
	Plattform  string           `json:"platform"`
	Backends   []BackendBericht `json:"backends"`
}

// messe führt fn ops-mal aus und sammelt die Einzel-Latenzen.
func messe(name string, ops int, fn func(i int) error) (Ergebnis, error) {
	lat := make([]time.Duration, 0, ops)
	start := time.Now()
	for i := 0; i < ops; i++ {
		t := time.Now()
		if err := fn(i); err != nil {
			return Ergebnis{}, fmt.Errorf("%s (op %d): %w", name, i, err)
		}
		lat = append(lat, time.Since(t))
	}
	return auswerten(name, ops, time.Since(start), lat), nil
}

// auswerten verdichtet Latenzen zu Kennzahlen.
func auswerten(name string, ops int, total time.Duration, lat []time.Duration) Ergebnis {
	e := Ergebnis{
		Name:      name,
		Ops:       ops,
		TotalMs:   float64(total.Microseconds()) / 1000,
		OpsProSek: float64(ops) / total.Seconds(),
	}
	if len(lat) > 0 {
		sort.Slice(lat, func(a, b int) bool { return lat[a] < lat[b] })
		p := func(q float64) float64 {
			idx := int(q * float64(len(lat)-1))
			return float64(lat[idx].Nanoseconds()) / 1000
		}
		e.P50Us, e.P95Us, e.P99Us = p(0.50), p(0.95), p(0.99)
		e.MaxUs = float64(lat[len(lat)-1].Nanoseconds()) / 1000
	}
	return e
}

// ============================================================================
// Das Szenario — identisch für jedes Backend (Verhaltensgleichheit!).
// ============================================================================

func szenario(backend string, treiber orm.Driver, scale int) (BackendBericht, error) {
	b := BackendBericht{Backend: backend}
	bg := context.Background()

	db, err := orm.Open(treiber)
	if err != nil {
		return b, err
	}
	defer db.Close()

	orm.Register[BenchKonto](db, orm.CRUD())
	orm.Register[BenchZaehler](db, orm.EventSourced(),
		orm.Events(orm.E[Erhoeht]("zaehler.erhoeht")),
		orm.SnapshotEvery(50),
	)
	if err := db.Migrate(bg); err != nil {
		return b, err
	}
	if err := db.StartWorkers(bg); err != nil {
		return b, err
	}
	ctx := orm.WithTenant(bg, orm.SingleTenant)
	konten := orm.Repo[BenchKonto](db)
	rnd := rand.New(rand.NewSource(1)) // fester Seed: reproduzierbare Zugriffe

	// Warmup: Verbindungen, Statement-Caches, Dateisystem — nicht mitmessen.
	for i := 0; i < 20; i++ {
		k := &BenchKonto{Name: "warmup", Mail: fmt.Sprintf("w%d@bench", i), Labels: []string{"w"}}
		if err := konten.Insert(ctx, k); err != nil {
			return b, err
		}
	}

	add := func(e Ergebnis, err error) error {
		if err != nil {
			return err
		}
		b.Ergebnisse = append(b.Ergebnisse, e)
		return nil
	}

	// --- 1. Insert einzeln: die Grundlatenz eines Schreibzugriffs ----------
	ids := make([]orm.ID, scale)
	if err := add(messe("crud/insert_einzeln", scale, func(i int) error {
		k := &BenchKonto{Name: fmt.Sprintf("Konto %04d", i), Mail: fmt.Sprintf("k%d@bench", i), Labels: []string{"a", "b"}}
		if err := konten.Insert(ctx, k); err != nil {
			return err
		}
		ids[i] = k.ID
		return nil
	})); err != nil {
		return b, err
	}

	// --- 2. InsertMany (Chunked): Durchsatz statt Einzellatenz -------------
	// Die Einfüge-Strategie wählt der Dialekt — genau das wird hier sichtbar.
	bulk := make([]*BenchKonto, scale*4)
	for i := range bulk {
		bulk[i] = &BenchKonto{Name: "Bulk", Mail: fmt.Sprintf("b%d@bench", i), Labels: []string{"bulk"}}
	}
	start := time.Now()
	if err := konten.InsertMany(ctx, bulk, orm.Chunked(250)); err != nil {
		return b, err
	}
	b.Ergebnisse = append(b.Ergebnisse, auswerten("crud/insert_many_chunked", len(bulk), time.Since(start), nil))

	// --- 3. Get per ID: der heißeste Lesepfad ------------------------------
	if err := add(messe("crud/get_per_id", scale, func(i int) error {
		_, err := konten.Get(ctx, ids[rnd.Intn(len(ids))])
		return err
	})); err != nil {
		return b, err
	}

	// --- 4. Query mit Index (Where + OrderBy + Limit) ----------------------
	if err := add(messe("crud/query_index_limit20", scale/2, func(i int) error {
		_, err := konten.Query(ctx).
			Where(orm.Like("Name", "Konto 00%")).
			OrderBy("Name", orm.Asc).Limit(20).All()
		return err
	})); err != nil {
		return b, err
	}

	// --- 5. Update mit optimistischem Locking ------------------------------
	if err := add(messe("crud/update_optimistisch", scale/2, func(i int) error {
		k, err := konten.Get(ctx, ids[i%len(ids)])
		if err != nil {
			return err
		}
		k.Stand++
		return konten.Update(ctx, k)
	})); err != nil {
		return b, err
	}

	// --- 6. Mengenbasiertes UpdateSet: 1 Statement über viele Zeilen -------
	start = time.Now()
	n, err := konten.Query(ctx).Where(orm.Eq("Name", "Bulk")).UpdateSet(orm.Set("Stand", int64(1)))
	if err != nil {
		return b, err
	}
	b.Ergebnisse = append(b.Ergebnisse, auswerten("crud/updateset_mengen", int(n), time.Since(start), nil))

	// --- 7. Transaktion mit Zeilensperre (Read-Modify-Write) ---------------
	if err := add(messe("crud/tx_getforupdate", scale/4, func(i int) error {
		return db.Tx(ctx, func(tx orm.Tx) error {
			k, err := orm.Repo[BenchKonto](tx).GetForUpdate(ctx, ids[i%len(ids)])
			if err != nil {
				return err
			}
			k.Stand++
			return orm.Repo[BenchKonto](tx).Update(ctx, k)
		})
	})); err != nil {
		return b, err
	}

	// --- 8. ES: Append-Durchsatz (1 Event pro Aufruf) ----------------------
	// Der ROADMAP-Lasttest: Append ist atomar (eigene Tx) und optimistisch.
	// Wir verteilen auf viele Aggregate — wie echte Last es täte.
	zaehler := make([]*BenchZaehler, max(1, scale/10))
	for i := range zaehler {
		zaehler[i] = orm.New[BenchZaehler](db)
	}
	var letztePos orm.Position
	if err := add(messe("es/append_1_event", scale, func(i int) error {
		pos, err := zaehler[i%len(zaehler)].Append(ctx, Erhoeht{Um: 1})
		letztePos = pos
		return err
	})); err != nil {
		return b, err
	}

	// --- 9. ES: Append im Batch (5 Events atomar) --------------------------
	if err := add(messe("es/append_5_events", scale/5, func(i int) error {
		pos, err := zaehler[i%len(zaehler)].Append(ctx,
			Erhoeht{Um: 1}, Erhoeht{Um: 1}, Erhoeht{Um: 1}, Erhoeht{Um: 1}, Erhoeht{Um: 1})
		letztePos = pos
		return err
	})); err != nil {
		return b, err
	}

	// --- 10. Projektions-Aufholzeit -----------------------------------------
	// Wie lange braucht die eingebaute Projektion nach einem Append-Burst,
	// bis das Read-Model die letzte Schreibposition erreicht hat?
	start = time.Now()
	if _, err := orm.Load[BenchZaehler](ctx, db, zaehler[0].ID(),
		orm.WaitFor(letztePos, 60*time.Second)); err != nil {
		return b, err
	}
	b.Ergebnisse = append(b.Ergebnisse, auswerten("es/projektions_aufholzeit", 1, time.Since(start), nil))

	// --- 11. ES: Load (Snapshot + Restevents falten) -----------------------
	if err := add(messe("es/load_aggregat", scale/2, func(i int) error {
		_, err := orm.Load[BenchZaehler](ctx, db, zaehler[i%len(zaehler)].ID())
		return err
	})); err != nil {
		return b, err
	}

	// --- 12. Query aufs Read-Model ------------------------------------------
	if err := add(messe("es/query_readmodel", scale/4, func(i int) error {
		_, err := orm.Query[BenchZaehler](db, ctx).Where(orm.Gt("Stand", int64(0))).Count()
		return err
	})); err != nil {
		return b, err
	}

	return b, nil
}

// ============================================================================
// Backend-Anbindung & Berichte
// ============================================================================

func main() {
	scale := flag.Int("scale", 300, "Skalierungsfaktor (Operationen pro Messreihe)")
	pgDSN := flag.String("postgres", os.Getenv("ORMPP_BENCH_POSTGRES"), "PostgreSQL-DSN (leer = überspringen)")
	ybDSN := flag.String("yugabyte", os.Getenv("ORMPP_BENCH_YUGABYTE"), "YugabyteDB-DSN (leer = überspringen)")
	jsonOut := flag.String("out", "report.json", "Pfad für den JSON-Bericht")
	benchOut := flag.String("bench", "bench.txt", "Pfad für das Go-Benchmark-Format (benchstat)")
	flag.Parse()

	bericht := Bericht{
		Zeitpunkt:  time.Now().UTC(),
		Skalierung: *scale,
		GoVersion:  runtime.Version(),
		Plattform:  runtime.GOOS + "/" + runtime.GOARCH,
	}

	// SQLite: immer — eingebettet, frische Datei.
	dir, err := os.MkdirTemp("", "ormpp-bench")
	must(err)
	defer func() { _ = os.RemoveAll(dir) }()
	lauf(&bericht, "sqlite", orm.SQLite(filepath.Join(dir, "bench.db")), *scale)

	// PostgreSQL/YugabyteDB: mit frischem Schema pro Lauf (saubere Messung,
	// keine Altdaten) — dieselbe Isolation wie in der Testsuite.
	if *pgDSN != "" {
		treiber, cleanup := isoliertesSchema(*pgDSN, false)
		lauf(&bericht, "postgres", treiber, *scale)
		cleanup()
	}
	if *ybDSN != "" {
		treiber, cleanup := isoliertesSchema(*ybDSN, true)
		lauf(&bericht, "yugabyte", treiber, *scale)
		cleanup()
	}

	druckeVergleich(bericht)

	// JSON-Bericht (strukturiert, archivierbar).
	j, err := json.MarshalIndent(bericht, "", "  ")
	must(err)
	must(os.WriteFile(*jsonOut, j, 0o644))

	// Go-Benchmark-Format — Läufe vergleichen mit benchstat.
	var sb strings.Builder
	for _, be := range bericht.Backends {
		for _, e := range be.Ergebnisse {
			nsProOp := e.TotalMs * 1e6 / float64(max(e.Ops, 1))
			fmt.Fprintf(&sb, "Benchmark%s/%s \t%d\t%.0f ns/op\t%.1f ops/s\n",
				benchName(e.Name), be.Backend, e.Ops, nsProOp, e.OpsProSek)
		}
	}
	must(os.WriteFile(*benchOut, []byte(sb.String()), 0o644))
	fmt.Printf("\nBerichte geschrieben: %s (JSON), %s (benchstat-Format)\n", *jsonOut, *benchOut)
}

func lauf(bericht *Bericht, name string, treiber orm.Driver, scale int) {
	fmt.Printf("\n════ %s (scale=%d) ════\n", name, scale)
	start := time.Now()
	b, err := szenario(name, treiber, scale)
	if err != nil {
		fmt.Printf("✗ %s übersprungen: %v\n", name, err)
		return
	}
	drucke(b)
	fmt.Printf("Gesamtlaufzeit %s: %s\n", name, time.Since(start).Round(time.Millisecond))
	bericht.Backends = append(bericht.Backends, b)
}

// isoliertesSchema legt ein frisches PG/YB-Schema an und hängt es als
// search_path an die DSN (identisch zur Testsuite-Isolation).
func isoliertesSchema(dsn string, yb bool) (orm.Driver, func()) {
	schema := fmt.Sprintf("bench_%d", time.Now().UnixNano())
	admin, err := sql.Open("pgx", dsn)
	must(err)
	_, err = admin.Exec(fmt.Sprintf("CREATE SCHEMA %q", schema))
	must(err)
	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	voll := dsn + sep + "search_path=" + schema
	cleanup := func() {
		_, _ = admin.Exec(fmt.Sprintf("DROP SCHEMA %q CASCADE", schema))
		_ = admin.Close()
	}
	if yb {
		return orm.Yugabyte(voll), cleanup
	}
	return orm.Postgres(voll), cleanup
}

// ============================================================================
// Ausgabe
// ============================================================================

func drucke(b BackendBericht) {
	fmt.Printf("%-28s %8s %10s %10s %10s %10s %10s\n",
		"Messreihe", "Ops", "ops/s", "p50", "p95", "p99", "max")
	for _, e := range b.Ergebnisse {
		lat := func(us float64) string {
			if us == 0 {
				return "—"
			}
			return time.Duration(us * float64(time.Microsecond)).Round(time.Microsecond).String()
		}
		fmt.Printf("%-28s %8d %10.0f %10s %10s %10s %10s\n",
			e.Name, e.Ops, e.OpsProSek, lat(e.P50Us), lat(e.P95Us), lat(e.P99Us), lat(e.MaxUs))
	}
}

func druckeVergleich(bericht Bericht) {
	if len(bericht.Backends) < 2 {
		return
	}
	fmt.Printf("\n════ Vergleich (ops/s) ════\n")
	fmt.Printf("%-28s", "Messreihe")
	for _, be := range bericht.Backends {
		fmt.Printf(" %12s", be.Backend)
	}
	fmt.Println()
	for i, e := range bericht.Backends[0].Ergebnisse {
		fmt.Printf("%-28s", e.Name)
		for _, be := range bericht.Backends {
			if i < len(be.Ergebnisse) {
				fmt.Printf(" %12.0f", be.Ergebnisse[i].OpsProSek)
			} else {
				fmt.Printf(" %12s", "—")
			}
		}
		fmt.Println()
	}
}

// benchName macht aus "crud/insert_einzeln" den Go-üblichen CamelCase-Namen.
func benchName(s string) string {
	var b strings.Builder
	up := true
	for _, r := range s {
		switch r {
		case '/', '_':
			up = true
			if r == '/' {
				b.WriteRune('/')
			}
		default:
			if up {
				b.WriteRune(toUpper(r))
				up = false
			} else {
				b.WriteRune(r)
			}
		}
	}
	return b.String()
}

func toUpper(r rune) rune {
	if r >= 'a' && r <= 'z' {
		return r - 32
	}
	return r
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
