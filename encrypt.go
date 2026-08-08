package orm

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"strings"
)

// EncryptFields ist der In-Place-Migrationsschritt für die
// Nachverschlüsselung: bestehende Klartext-Spalten eines CRUD-Models werden
// in die encrypted-Form überführt, ohne dass die Anwendung Doppel-Felder
// pflegen muss. Voraussetzung wie überall: das Model trägt zum Zeitpunkt
// der Migration bereits das encrypted-Tag auf den genannten Feldern.
//
//	orm.MigrationTo(db, 24,
//	    orm.EncryptFields[core.User]("PasswordHash"),
//	)
//
// Der Schritt liest die Spalten UNTER der Model-Schicht (die Zeilen sind
// noch Klartext — der normale Lesepfad würde am Ciphertext-Parser
// scheitern) und schreibt sie mit dem aktuellen Schlüssel als Ciphertext
// zurück; der Spaltentyp wandert vorab auf BYTEA/BLOB. Am
// Ciphertext-Versionsbyte ist erkennbar, welche Zeilen schon umgezogen
// sind — der Schritt ist idempotent, checkpointfähig (Abbruch setzt beim
// letzten gesicherten Primärschlüssel wieder auf) und läuft über die ganze
// Tabelle, also über alle Mandanten einschließlich SingleTenant.
// lookup-Felder werden mitgezogen: ihre Index-Spalte wird beim Umzug
// befüllt (auch für Zeilen, die schon Ciphertext tragen).
func EncryptFields[T any](fields ...string) MigrationStep {
	return &encryptFieldsStep{modelType: reflect.TypeFor[T](), fields: fields}
}

type encryptFieldsStep struct {
	modelType reflect.Type
	fields    []string
}

func (*encryptFieldsStep) migrationStep() {}

// compiledEncrypt ist ein ausführbarer EncryptFields-Schritt.
type compiledEncrypt struct {
	step   *encryptFieldsStep
	m      *model
	fields []*field
	name   string // Schritt-Schlüssel für Checkpoints
}

func (d *DB) compileEncrypt(es *encryptFieldsStep) (*compiledEncrypt, error) {
	m := d.reg.models[es.modelType]
	if m == nil {
		return nil, fmt.Errorf("orm: EncryptFields: Model %s ist nicht registriert", es.modelType.Name())
	}
	if m.kind != kindCRUD {
		return nil, fmt.Errorf("orm: EncryptFields: %s ist event-sourced — dort ist encrypted nicht verfügbar", m.name)
	}
	if len(es.fields) == 0 {
		return nil, fmt.Errorf("orm: EncryptFields[%s] ohne Felder", m.name)
	}
	if d.opts.keys == nil {
		return nil, fmt.Errorf("orm: EncryptFields[%s]: orm.Encryption(...) fehlt", m.name)
	}
	ce := &compiledEncrypt{step: es, m: m}
	for _, name := range es.fields {
		f := m.fieldByName(name)
		if f == nil {
			return nil, fmt.Errorf("orm: EncryptFields: %s.%s ist kein Feld des Models", m.name, name)
		}
		if !f.encrypted {
			return nil, fmt.Errorf("orm: EncryptFields: %s.%s trägt kein encrypted-Tag — erst das Model umstellen", m.name, name)
		}
		ce.fields = append(ce.fields, f)
	}
	ce.name = "encrypt:" + m.name + "." + strings.Join(es.fields, "+")
	return ce, nil
}

// convertEncryptColumns bringt die Spalten des Schritts auf den Binärtyp
// (expanding-Phase; idempotent — schon binäre Spalten sind ein No-op).
func (d *DB) convertEncryptColumns(ctx context.Context, ce *compiledEncrypt) error {
	for _, f := range ce.fields {
		stmts, err := d.dial.blobColumnSQL(d.q(), ce.m.table, f.column)
		if err != nil {
			return err
		}
		for _, stmt := range stmts {
			if err := d.execDDL(ctx, stmt); err != nil {
				return fmt.Errorf("orm: %s.%s auf Binärtyp bringen: %w", ce.m.table, f.column, err)
			}
		}
	}
	return nil
}

// backfillEncrypt verschlüsselt die Bestandszeilen in Batches — checkpointed
// über den Primärschlüssel (wiederaufnehmbar) und drosselbar wie jeder
// Backfill. Zeilen, die schon gültigen Ciphertext tragen, werden nicht neu
// geschrieben (kein Nonce-Wechsel); fehlt nur der Lookup-Index, wird er aus
// dem entschlüsselten Klartext ergänzt.
func (d *DB) backfillEncrypt(ctx context.Context, version int, ce *compiledEncrypt, plan MigrationPlan) error {
	p, err := d.readProgress(ctx, version, ce.name)
	if err != nil {
		return err
	}
	if p.state == "done" {
		return nil
	}
	m := ce.m
	keys := d.opts.keys

	sel := []string{m.pk.column}
	for _, f := range ce.fields {
		sel = append(sel, f.column)
		if f.lookup {
			sel = append(sel, f.lookupColumn())
		}
	}

	last, done := p.lastKey, p.rowsDone
	for {
		cond, args := "", []any{}
		if last != "" {
			cond = fmt.Sprintf("WHERE %q > ?", m.pk.column)
			args = append(args, last)
		}
		query := fmt.Sprintf("SELECT %s FROM %q %s ORDER BY %q LIMIT %d",
			quoteAll(sel...), m.table, cond, m.pk.column, plan.BatchSize)

		type rowUpdate struct {
			pk   string
			sets []string
			args []any
		}
		var updates []rowUpdate
		var batchLast string
		n := 0
		{
			rows, err := d.q().QueryContext(ctx, query, args...)
			if err != nil {
				return err
			}
			for rows.Next() {
				raws := make([]any, len(sel))
				ptrs := make([]any, len(sel))
				for i := range raws {
					ptrs[i] = &raws[i]
				}
				if err := rows.Scan(ptrs...); err != nil {
					rows.Close()
					return err
				}
				pk, _ := rawString(raws[0])
				batchLast = pk
				n++
				u := rowUpdate{pk: pk}
				i := 1
				for _, f := range ce.fields {
					raw := raws[i]
					i++
					var storedLookup []byte
					hasLookup := f.lookup
					if hasLookup {
						storedLookup, _ = rawBytes(raws[i])
						i++
					}
					if raw == nil {
						continue // NULL bleibt NULL, Index bleibt NULL
					}
					blob, ok := rawBytes(raw)
					if !ok {
						rows.Close()
						return fmt.Errorf("orm: EncryptFields: %s.%s: unerwarteter Typ %T", m.name, f.name, raw)
					}
					plain, already := blob, false
					if pt, err := decryptValue(keys, blob); err == nil {
						plain, already = pt, true
					}
					if !already {
						enc, err := encryptValue(keys, plain)
						if err != nil {
							rows.Close()
							return err
						}
						u.sets = append(u.sets, fmt.Sprintf("%q = ?", f.column))
						u.args = append(u.args, enc)
					}
					if hasLookup {
						lk, err := lookupHash(keys, string(plain))
						if err != nil {
							rows.Close()
							return err
						}
						lkBytes, _ := lk.([]byte)
						if !bytes.Equal(lkBytes, storedLookup) {
							u.sets = append(u.sets, fmt.Sprintf("%q = ?", f.lookupColumn()))
							u.args = append(u.args, lk)
						}
					}
				}
				if len(u.sets) > 0 {
					updates = append(updates, u)
				}
			}
			rows.Close()
			if err := rows.Err(); err != nil {
				return err
			}
		}
		if n == 0 {
			break
		}
		started := nowUTC()
		err = d.Tx(ctx, func(tx Tx) error {
			for _, u := range updates {
				stmt := fmt.Sprintf("UPDATE %q SET %s WHERE %q = ?",
					m.table, strings.Join(u.sets, ", "), m.pk.column)
				if _, err := tx.q().ExecContext(ctx, stmt, append(u.args, u.pk)...); err != nil {
					return err
				}
			}
			done += int64(n)
			return d.writeProgress(ctx, tx.q(), version, ce.name, batchLast, done, "running")
		})
		if err != nil {
			return fmt.Errorf("orm: EncryptFields %s: %w", ce.name, err)
		}
		last = batchLast
		throttleBatch(plan.Throttle, n, started)
	}
	return d.writeProgress(ctx, d.q(), version, ce.name, last, done, "done")
}
