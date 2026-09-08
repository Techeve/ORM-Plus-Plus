package orm

import (
	"reflect"
	"strings"
	"testing"
)

// Eine nachtraeglich ergaenzte Pflichtspalte MIT deklarierter Vorgabe bekam
// zwei DEFAULT-Klauseln: die fuer die Bestandszeilen und die deklarierte.
// PostgreSQL und YugabyteDB lehnen das ab ("multiple default values
// specified for column"), SQLite nimmt es klaglos — der Fehler fiel
// deshalb erst beim Ausrollen auf (2026-09-08, DNS-Editor Account.Scope:
// jeder Knoten startete in eine Schleife).
//
// Der deklarierte Wert ist ohnehin der richtige: Bestandszeilen sollen
// nicht den Nullwert des Typs tragen, sondern das, was das Modell sagt.
func TestAlterColumnHasSingleDefault(t *testing.T) {
	t.Parallel()
	f := &field{
		column:     "scope",
		goType:     reflect.TypeFor[string](),
		defaultVal: "project",
		hasDefault: true,
		enum:       []string{"project", "organisation"},
	}
	ddl := columnDDL(sqliteDialect{}, f, false)
	if n := strings.Count(ddl, "DEFAULT"); n != 1 {
		t.Fatalf("%d DEFAULT-Klauseln in %q, erwartet genau eine", n, ddl)
	}
	if !strings.Contains(ddl, "DEFAULT 'project'") {
		t.Fatalf("die deklarierte Vorgabe fehlt: %q", ddl)
	}
	if !strings.Contains(ddl, "NOT NULL") {
		t.Fatalf("NOT NULL fehlt: %q", ddl)
	}

	// Ohne eigene Vorgabe bleibt es beim Nullwert des Typs — sonst
	// scheiterte ALTER ADD COLUMN an den Bestandszeilen.
	ohne := &field{column: "note", goType: reflect.TypeFor[string]()}
	ddl = columnDDL(sqliteDialect{}, ohne, false)
	if !strings.Contains(ddl, "NOT NULL DEFAULT ''") {
		t.Fatalf("Nullwert-Vorgabe fehlt: %q", ddl)
	}
}
