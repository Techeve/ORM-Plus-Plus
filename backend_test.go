package orm

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// newTestStore liefert eine Treiber-Fabrik: Jeder Aufruf verbindet mit
// DEMSELBEN frischen Speicher (Datei bzw. Schema) — so laufen auch
// Restart-Szenarien (Drift, Upcaster, Migration) auf allen Backends.
//
// Backend-Auswahl über die Umgebung:
//
//	ORMPP_TEST_BACKEND=sqlite (Default) | postgres | yugabyte
//	ORMPP_TEST_DSN=postgres://… (für postgres/yugabyte, siehe docker-compose.yml)
//
// Auf Postgres/Yugabyte bekommt jeder Test ein eigenes Schema (search_path)
// und räumt es am Ende weg — die Verhaltenssuite ist backend-identisch.
func newTestStore(t *testing.T) func() Driver {
	t.Helper()
	backend := os.Getenv("ORMPP_TEST_BACKEND")
	switch backend {
	case "", "sqlite":
		file := filepath.Join(t.TempDir(), "test.db")
		return func() Driver { return SQLite(file) }
	case "postgres", "yugabyte":
		dsn := os.Getenv("ORMPP_TEST_DSN")
		if dsn == "" {
			t.Fatalf("ORMPP_TEST_DSN fehlt für Backend %q", backend)
		}
		if err := warteAufBackend(dsn); err != nil {
			t.Fatalf("Backend nicht bereit: %v", err)
		}
		var raw [8]byte
		if _, err := rand.Read(raw[:]); err != nil {
			t.Fatal(err)
		}
		schema := "t_" + hex.EncodeToString(raw[:])
		admin, err := sql.Open("pgx", dsn)
		if err != nil {
			t.Fatalf("Admin-Verbindung: %v", err)
		}
		// Wie in der Engine (execDDL): YugabyteDB serialisiert
		// Katalogaenderungen und meldet bei gleichzeitiger DDL
		// "Restart read required" (40001) — wiederholen, nicht scheitern.
		// Parallel laufende Tests treffen das sonst regelmaessig.
		for attempt := 0; attempt < 8; attempt++ {
			if _, err = admin.Exec(fmt.Sprintf("CREATE SCHEMA %q", schema)); err == nil || !isTransientConflict(err) {
				break
			}
			time.Sleep(time.Duration(attempt+1) * 50 * time.Millisecond)
		}
		if err != nil {
			t.Fatalf("Schema anlegen: %v", err)
		}
		t.Cleanup(func() {
			_, _ = admin.Exec(fmt.Sprintf("DROP SCHEMA %q CASCADE", schema))
			_ = admin.Close()
		})
		sep := "?"
		if strings.Contains(dsn, "?") {
			sep = "&"
		}
		testDSN := dsn + sep + "search_path=" + schema
		if backend == "yugabyte" {
			return func() Driver { return Yugabyte(testDSN) }
		}
		return func() Driver { return Postgres(testDSN) }
	default:
		t.Fatalf("unbekanntes ORMPP_TEST_BACKEND %q", backend)
		return nil
	}
}

// warteAufBackend blockiert, bis das Backend wirklich SQL annimmt.
//
// Ein offener TCP-Port genügt als Nachweis nicht: YugabyteDB nimmt auf
// 5433 schon Verbindungen entgegen, wenn die YSQL-Schicht noch startet —
// die Suite scheiterte dann reihenweise an "failed to connect". Der
// Runner-Health-Check gibt ohnehin nach 30 Sekunden auf, also wartet die
// Suite selbst, einmal für alle Tests.
var (
	backendBereit sync.Once
	backendFehler error
)

func warteAufBackend(dsn string) error {
	backendBereit.Do(func() {
		frist := time.Now().Add(3 * time.Minute)
		for {
			db, err := sql.Open("pgx", dsn)
			if err == nil {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				err = db.PingContext(ctx)
				cancel()
				_ = db.Close()
				if err == nil {
					return
				}
			}
			if time.Now().After(frist) {
				backendFehler = err
				return
			}
			time.Sleep(2 * time.Second)
		}
	})
	return backendFehler
}
