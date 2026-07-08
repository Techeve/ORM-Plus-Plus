package orm

import "context"

// replaceModel ist der Migrationsschritt "neues Model ersetzt altes"
// (Expand/Contract mit Dual-Write, Ausführung in Phase 3).
type replaceModel struct{}

func (replaceModel) migrationStep() {}

// ReplaceModel deklariert: New ersetzt Old, mit Transformation alt→neu.
// Die Ausführung (Expand/Backfill/Dual-Write/Finalize) kommt in Phase 3;
// die Deklaration ist bereits API-stabil.
func ReplaceModel[Old, New any](fn func(ctx context.Context, old Old) (New, error)) MigrationStep {
	return replaceModel{}
}
