package orm

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"
)

// MigrationPlan tunt die Ausführung einer Migration (optional bei Migrate).
type MigrationPlan struct {
	WorkersPerGeo map[string]int // regionale Worker-Zahl (Phase 4; SQLite: eine Region)
	BatchSize     int            // Zeilen pro Backfill-Batch (Default 1000)
	Throttle      Rate           // Obergrenze Zeilen/Sekunde (0 = ungedrosselt)
}

// Rate ist eine Durchsatz-Obergrenze.
type Rate int64

// RowsPerSecond deklariert eine Drossel in Zeilen pro Sekunde.
func RowsPerSecond(n int) Rate { return Rate(n) }

// MigrationStep ist ein Schritt einer versionierten Migration.
type MigrationStep interface{ migrationStep() }

// MigrationTo registriert die Schritte, die zur Schema-Version version
// führen. Ältere MigrationTo-Aufrufe bleiben im Code erhalten, damit auch
// Sprünge über mehrere Versionen in einem Zug möglich sind.
func MigrationTo(d *DB, version int, steps ...MigrationStep) {
	if d.migrations == nil {
		d.migrations = map[int][]MigrationStep{}
	}
	d.migrations[version] = append(d.migrations[version], steps...)
}

// --- ReplaceModel ---

// replaceStep: neues Model ersetzt altes, mit Zeilen-Transformation alt→neu.
type replaceStep struct {
	oldType reflect.Type
	newType reflect.Type
	fwd     func(ctx context.Context, oldPtr any) (newPtr any, err error)
}

func (*replaceStep) migrationStep() {}

// ReplaceModel deklariert: New ersetzt Old, mit Transformation alt→neu.
//
// Konvention: Der Go-Name von Old ist der alte Model-Name mit
// Versions-Suffix — "ZoneV2" liest die Tabelle des früheren Models "Zone".
// Ohne V-Suffix wird der Name unverändert auf die Tabelle abgebildet.
// Setzt die Transformation keine ID, übernimmt die Engine die alte
// (gleiche Identität über den Umbau hinweg).
func ReplaceModel[Old, New any](fn func(ctx context.Context, old Old) (New, error)) MigrationStep {
	return &replaceStep{
		oldType: reflect.TypeFor[Old](),
		newType: reflect.TypeFor[New](),
		fwd: func(ctx context.Context, oldPtr any) (any, error) {
			n, err := fn(ctx, *(oldPtr.(*Old)))
			if err != nil {
				return nil, err
			}
			return &n, nil
		},
	}
}

// oldTableName bildet den Go-Namen eines eingefrorenen Alt-Structs auf die
// Tabelle des früheren Models ab: "ZoneV2" → "zone", "CustomerV1" → "customer".
func oldTableName(typeName string) string {
	if i := strings.LastIndexByte(typeName, 'V'); i > 0 && i < len(typeName)-1 {
		digits := typeName[i+1:]
		allDigits := true
		for _, r := range digits {
			if r < '0' || r > '9' {
				allDigits = false
				break
			}
		}
		if allDigits {
			return snakeCase(typeName[:i])
		}
	}
	return snakeCase(typeName)
}

// compiledReplace ist ein ausführbarer ReplaceModel-Schritt.
type compiledReplace struct {
	step *replaceStep
	oldM *model // Scratch-Plan des Alt-Structs (Tabelle: V-Suffix-Konvention)
	newM *model // registriertes Ziel-Model
	name string // Schritt-Schlüssel für Checkpoints
}

// compileReplace validiert und kompiliert einen ReplaceModel-Schritt.
func (d *DB) compileReplace(rs *replaceStep) (*compiledReplace, error) {
	newM := d.reg.models[rs.newType]
	if newM == nil {
		return nil, fmt.Errorf("orm: ReplaceModel: Ziel-Model %s ist nicht registriert", rs.newType.Name())
	}
	if newM.kind != kindCRUD {
		return nil, fmt.Errorf("orm: ReplaceModel: %s ist event-sourced — ES-Umbauten laufen über Events/Upcaster", newM.name)
	}
	// Alt-Struct in einer Scratch-Registry kompilieren (nicht Teil des
	// Schemas). Die Tenant-Bindung erbt es vom Ziel-Model: ein Umbau ändert
	// die Bindung per Konvention nie, und die Alt-Tabelle eines
	// TenantFree-Models hat keine tenant_id-Spalte — sie tenant-gebunden zu
	// lesen würde den Backfill brechen.
	tmp := newRegistry()
	if newM.tenanted() {
		register(tmp, rs.oldType, CRUD())
	} else {
		register(tmp, rs.oldType, CRUD(), TenantFree())
	}
	if len(tmp.errs) > 0 {
		return nil, fmt.Errorf("orm: ReplaceModel: Alt-Struct %s: %w", rs.oldType.Name(), joinErrs(tmp.errs))
	}
	oldM := tmp.models[rs.oldType]
	oldM.table = oldTableName(rs.oldType.Name())
	if oldM.table == newM.table {
		return nil, fmt.Errorf("orm: ReplaceModel: %s und %s bilden auf dieselbe Tabelle %q ab", rs.oldType.Name(), newM.name, oldM.table)
	}
	return &compiledReplace{
		step: rs,
		oldM: oldM,
		newM: newM,
		name: "replace:" + rs.oldType.Name() + "->" + newM.name,
	}, nil
}

// --- BatchScript ---

// Batch ist die Checkpoint-Schnittstelle eines Batch-Migrationsskripts.
// Das Skript arbeitet mit den normalen ORM-APIs (Query/Iter/UpdateSet) und
// sichert seinen Fortschritt als opaken Schlüssel — bei Wiederaufnahme
// liest es ihn zurück und setzt dort fort.
type Batch struct {
	d       *DB
	version int
	step    string
}

// Checkpoint liest den zuletzt gesicherten Fortschritts-Schlüssel ("" am Anfang).
func (b Batch) Checkpoint(ctx context.Context) (string, error) {
	p, err := b.d.readProgress(ctx, b.version, b.step)
	if err != nil {
		return "", err
	}
	return p.lastKey, nil
}

// SaveCheckpoint sichert den Fortschritt (wiederaufnehmbar über Neustarts).
func (b Batch) SaveCheckpoint(ctx context.Context, key string, rowsDone int64) error {
	return b.d.writeProgress(ctx, b.d.q(), b.version, b.step, key, rowsDone, "running")
}

type batchScript struct {
	name string
	fn   func(ctx context.Context, b Batch) error
}

func (*batchScript) migrationStep() {}

// BatchScript deklariert ein freies Batch-Migrationsskript. Die Engine
// verwaltet den Checkpoint (Batch); das Skript muss idempotent aufsetzbar sein.
func BatchScript(name string, fn func(ctx context.Context, b Batch) error) MigrationStep {
	return &batchScript{name: name, fn: fn}
}

// --- Fortschritt (ormpp_migration_progress) ---

type progress struct {
	lastKey  string
	rowsDone int64
	state    string // "" | "running" | "done"
}

func (d *DB) readProgress(ctx context.Context, version int, step string) (progress, error) {
	var p progress
	err := d.q().QueryRowContext(ctx,
		`SELECT last_key, rows_done, state FROM ormpp_migration_progress WHERE version = ? AND step = ? AND geo = ?`,
		version, step, "local").Scan(&p.lastKey, &p.rowsDone, &p.state)
	switch err {
	case nil:
		return p, nil
	case sql.ErrNoRows:
		return progress{}, nil
	default:
		return progress{}, err
	}
}

func (d *DB) writeProgress(ctx context.Context, q queryer, version int, step, lastKey string, rowsDone int64, state string) error {
	_, err := q.ExecContext(ctx, `
		INSERT INTO ormpp_migration_progress (version, step, geo, last_key, rows_done, state, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (version, step, geo) DO UPDATE SET
			last_key = excluded.last_key, rows_done = excluded.rows_done,
			state = excluded.state, updated_at = excluded.updated_at`,
		version, step, "local", lastKey, rowsDone, state, nowUTC().Format(time.RFC3339Nano))
	return err
}
