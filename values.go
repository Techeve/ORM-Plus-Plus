package orm

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"time"
)

// encodeField wandelt einen Go-Feldwert in einen Treiberwert.
func encodeField(d *DB, f *field, v reflect.Value) (any, error) {
	if f.json {
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
	if f.encrypted {
		var plain []byte
		switch val := v.Interface().(type) {
		case string:
			plain = []byte(val)
		case []byte:
			plain = val
		}
		return encryptValue(d.opts.keys, plain)
	}
	switch val := v.Interface().(type) {
	case ID:
		if val.IsZero() {
			return nil, nil
		}
		return val.String(), nil
	case time.Time:
		if val.IsZero() {
			return nil, nil
		}
		return val.UTC().Format(time.RFC3339Nano), nil
	case bool:
		if val {
			return int64(1), nil
		}
		return int64(0), nil
	default:
		return v.Interface(), nil
	}
}

// decodeField schreibt einen Treiberwert zurück in ein Go-Feld.
func decodeField(d *DB, f *field, target reflect.Value, raw any) error {
	if f.json {
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

	if f.encrypted {
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
		return nil
	}

	switch target.Interface().(type) {
	case ID:
		var id ID
		if err := id.Scan(raw); err != nil {
			return err
		}
		target.Set(reflect.ValueOf(id))
		return nil
	case time.Time:
		s, ok := rawString(raw)
		if !ok {
			return fmt.Errorf("orm: Zeit-Spalte %s: unerwarteter Typ %T", f.column, raw)
		}
		t, err := time.Parse(time.RFC3339Nano, s)
		if err != nil {
			return err
		}
		target.Set(reflect.ValueOf(t))
		return nil
	}

	switch target.Kind() {
	case reflect.String:
		s, ok := rawString(raw)
		if !ok {
			return fmt.Errorf("orm: Spalte %s: %T statt String", f.column, raw)
		}
		target.SetString(s)
	case reflect.Bool:
		n, ok := rawInt(raw)
		if !ok {
			return fmt.Errorf("orm: Spalte %s: %T statt Bool", f.column, raw)
		}
		target.SetBool(n != 0)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n, ok := rawInt(raw)
		if !ok {
			return fmt.Errorf("orm: Spalte %s: %T statt Integer", f.column, raw)
		}
		target.SetInt(n)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		n, ok := rawInt(raw)
		if !ok {
			return fmt.Errorf("orm: Spalte %s: %T statt Integer", f.column, raw)
		}
		target.SetUint(uint64(n))
	case reflect.Float32, reflect.Float64:
		switch v := raw.(type) {
		case float64:
			target.SetFloat(v)
		case int64:
			target.SetFloat(float64(v))
		default:
			return fmt.Errorf("orm: Spalte %s: %T statt Float", f.column, raw)
		}
	case reflect.Slice: // []byte
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
func scanModelRows[T any](h Handle, m *model, rows *sql.Rows) ([]*T, error) {
	defer rows.Close()
	var out []*T
	for rows.Next() {
		e, err := scanModelRow[T](h, m, rows)
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
		n += 2 // id, aggregate_seq (siehe selectList)
	}
	raws := make([]any, n)
	ptrs := make([]any, n)
	for i := range raws {
		ptrs[i] = &raws[i]
	}
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
