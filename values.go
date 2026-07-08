package orm

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"time"
)

// decKind ist die beim Registrieren kompilierte Kodierungsart eines Feldes.
// Der Hot Path (encode/decode pro Zeile) macht damit nur noch einen
// Integer-Switch — kein Interface-Boxing, um den Typ zu erraten.
type decKind int

const (
	dOther decKind = iota // Fallback über database/sql-Konvertierung
	dJSON
	dEncrypted
	dID
	dTime
	dString
	dBool
	dInt
	dUint
	dFloat
	dBytes
)

func decKindOf(f *field) decKind {
	if f.json {
		return dJSON
	}
	if f.encrypted {
		return dEncrypted
	}
	t := f.goType
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	switch {
	case t == idType:
		return dID
	case t == timeType:
		return dTime
	case t.Kind() == reflect.String:
		return dString
	case t.Kind() == reflect.Bool:
		return dBool
	case t.Kind() >= reflect.Int && t.Kind() <= reflect.Int64:
		return dInt
	case t.Kind() >= reflect.Uint && t.Kind() <= reflect.Uint64:
		return dUint
	case t.Kind() == reflect.Float32 || t.Kind() == reflect.Float64:
		return dFloat
	case t.Kind() == reflect.Slice && t.Elem().Kind() == reflect.Uint8:
		return dBytes
	default:
		return dOther
	}
}

// encodeField wandelt einen Go-Feldwert in einen Treiberwert.
func encodeField(d *DB, f *field, v reflect.Value) (any, error) {
	if f.dk == dJSON {
		b, err := json.Marshal(v.Interface())
		if err != nil {
			return nil, fmt.Errorf("orm: %s als JSON: %w", f.name, err)
		}
		return string(b), nil
	}
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return nil, nil
		}
		v = v.Elem()
	}
	switch f.dk {
	case dEncrypted:
		var plain []byte
		if v.Kind() == reflect.String {
			plain = []byte(v.String())
		} else {
			plain = v.Bytes()
		}
		return encryptValue(d.opts.keys, plain)
	case dID:
		id := v.Interface().(ID)
		if id.IsZero() {
			return nil, nil
		}
		return id.String(), nil
	case dTime:
		t := v.Interface().(time.Time)
		if t.IsZero() {
			return nil, nil
		}
		return t.UTC().Format(time.RFC3339Nano), nil
	case dString:
		return v.String(), nil
	case dBool:
		if v.Bool() {
			return int64(1), nil
		}
		return int64(0), nil
	case dInt:
		return v.Int(), nil
	case dUint:
		return int64(v.Uint()), nil
	case dFloat:
		return v.Float(), nil
	case dBytes:
		return v.Bytes(), nil
	default:
		return v.Interface(), nil
	}
}

// decodeField schreibt einen Treiberwert zurück in ein Go-Feld.
func decodeField(d *DB, f *field, target reflect.Value, raw any) error {
	if f.dk == dJSON {
		if raw == nil {
			target.SetZero()
			return nil
		}
		var data []byte
		switch v := raw.(type) {
		case string:
			data = []byte(v)
		case []byte:
			data = v
		default:
			return fmt.Errorf("orm: JSON-Spalte %s: unerwarteter Typ %T", f.column, raw)
		}
		return json.Unmarshal(data, target.Addr().Interface())
	}

	if target.Kind() == reflect.Pointer {
		if raw == nil {
			target.SetZero()
			return nil
		}
		target.Set(reflect.New(target.Type().Elem()))
		target = target.Elem()
	} else if raw == nil {
		target.SetZero()
		return nil
	}

	switch f.dk {
	case dEncrypted:
		blob, ok := raw.([]byte)
		if !ok {
			return fmt.Errorf("orm: encrypted-Spalte %s: %T statt BLOB", f.column, raw)
		}
		plain, err := decryptValue(d.opts.keys, blob)
		if err != nil {
			return fmt.Errorf("orm: Spalte %s: %w", f.column, err)
		}
		if target.Kind() == reflect.String {
			target.SetString(string(plain))
		} else {
			target.SetBytes(plain)
		}
	case dID:
		id := target.Addr().Interface().(*ID)
		if err := id.Scan(raw); err != nil {
			return err
		}
	case dTime:
		s, ok := rawString(raw)
		if !ok {
			return fmt.Errorf("orm: Zeit-Spalte %s: unerwarteter Typ %T", f.column, raw)
		}
		t, err := time.Parse(time.RFC3339Nano, s)
		if err != nil {
			return err
		}
		*(target.Addr().Interface().(*time.Time)) = t
	case dString:
		s, ok := rawString(raw)
		if !ok {
			return fmt.Errorf("orm: Spalte %s: %T statt String", f.column, raw)
		}
		target.SetString(s)
	case dBool:
		n, ok := rawInt(raw)
		if !ok {
			return fmt.Errorf("orm: Spalte %s: %T statt Bool", f.column, raw)
		}
		target.SetBool(n != 0)
	case dInt:
		n, ok := rawInt(raw)
		if !ok {
			return fmt.Errorf("orm: Spalte %s: %T statt Integer", f.column, raw)
		}
		target.SetInt(n)
	case dUint:
		n, ok := rawInt(raw)
		if !ok {
			return fmt.Errorf("orm: Spalte %s: %T statt Integer", f.column, raw)
		}
		target.SetUint(uint64(n))
	case dFloat:
		switch v := raw.(type) {
		case float64:
			target.SetFloat(v)
		case int64:
			target.SetFloat(float64(v))
		default:
			return fmt.Errorf("orm: Spalte %s: %T statt Float", f.column, raw)
		}
	case dBytes:
		b, ok := raw.([]byte)
		if !ok {
			return fmt.Errorf("orm: Spalte %s: %T statt BLOB", f.column, raw)
		}
		target.SetBytes(append([]byte(nil), b...))
	default:
		return fmt.Errorf("orm: Spalte %s: nicht unterstützter Zieltyp %s", f.column, target.Type())
	}
	return nil
}

func rawString(raw any) (string, bool) {
	switch v := raw.(type) {
	case string:
		return v, true
	case []byte:
		return string(v), true
	}
	return "", false
}

func rawInt(raw any) (int64, bool) {
	switch v := raw.(type) {
	case int64:
		return v, true
	case int:
		return int64(v), true
	case bool:
		if v {
			return 1, true
		}
		return 0, true
	}
	return 0, false
}

// scanModelRows liest alle Zeilen eines *sql.Rows in Model-Instanzen.
// Die Scan-Puffer werden über die Zeilen hinweg wiederverwendet.
func scanModelRows[T any](h Handle, m *model, rows *sql.Rows) ([]*T, error) {
	defer rows.Close()
	n := len(m.fields)
	if m.kind == kindEventSourced {
		n += 2 // id, aggregate_seq (siehe selectList)
	}
	raws := make([]any, n)
	ptrs := make([]any, n)
	for i := range raws {
		ptrs[i] = &raws[i]
	}
	var out []*T
	for rows.Next() {
		e, err := scanModelRowInto[T](h, m, rows, raws, ptrs)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func scanModelRow[T any](h Handle, m *model, rows *sql.Rows) (*T, error) {
	n := len(m.fields)
	if m.kind == kindEventSourced {
		n += 2
	}
	raws := make([]any, n)
	ptrs := make([]any, n)
	for i := range raws {
		ptrs[i] = &raws[i]
	}
	return scanModelRowInto[T](h, m, rows, raws, ptrs)
}

func scanModelRowInto[T any](h Handle, m *model, rows *sql.Rows, raws, ptrs []any) (*T, error) {
	if err := rows.Scan(ptrs...); err != nil {
		return nil, err
	}
	e := new(T)
	rv := reflect.ValueOf(e).Elem()
	for i, f := range m.fields {
		if err := decodeField(h.db(), f, rv.FieldByIndex(f.index), raws[i]); err != nil {
			return nil, err
		}
	}
	if m.kind == kindEventSourced {
		var id ID
		if err := id.Scan(raws[len(m.fields)]); err != nil {
			return nil, err
		}
		seq, _ := rawInt(raws[len(m.fields)+1])
		agg := wireAggregate(m, e, h, id)
		agg.version = seq
	}
	return e, nil
}
