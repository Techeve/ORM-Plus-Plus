package orm

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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
