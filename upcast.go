package orm

import (
	"encoding/json"
	"fmt"
	"reflect"
)

// upcaster transformiert einen Event-Payload von Version from nach from+1.
type upcaster struct {
	inType  reflect.Type
	outType reflect.Type
	fn      func(any) (any, error)
}

// Upcast registriert eine Transformation für Events des Kurznamens name
// von Schema-Version fromVersion nach fromVersion+1. Ketten (v1→v2→v3)
// werden beim Lesen automatisch durchlaufen. Fehlt ein Upcaster für eine
// in der DB vorhandene alte Version, schlägt Migrate fehl.
func Upcast[Old, New any](d *DB, name string, fromVersion int, fn func(Old) (New, error)) {
	if d.upcasters == nil {
		d.upcasters = map[string]map[int]upcaster{}
	}
	byVersion := d.upcasters[name]
	if byVersion == nil {
		byVersion = map[int]upcaster{}
		d.upcasters[name] = byVersion
	}
	byVersion[fromVersion] = upcaster{
		inType:  reflect.TypeFor[Old](),
		outType: reflect.TypeFor[New](),
		fn: func(in any) (any, error) {
			out, err := fn(in.(Old))
			return out, err
		},
	}
}

// decodePayload dekodiert einen gespeicherten Payload; alte Versionen
// werden durch die Upcaster-Kette bis zur deklarierten Version gehoben.
func (d *DB) decodePayload(m *model, full string, data []byte) (any, *eventDecl, error) {
	name, v, ok := m.es.parseType(full)
	if !ok {
		return nil, nil, fmt.Errorf("orm: Event-Typ %q passt nicht zum Präfix von %s", full, m.name)
	}
	decl := m.es.byName[name]
	if decl == nil {
		return nil, nil, fmt.Errorf("orm: Event %q ist auf %s nicht deklariert", name, m.name)
	}
	if v > decl.version {
		return nil, nil, fmt.Errorf("orm: Event %q liegt in v%d vor, deklariert ist nur v%d", name, v, decl.version)
	}
	if v == decl.version {
		inst := reflect.New(decl.goType)
		if err := json.Unmarshal(data, inst.Interface()); err != nil {
			return nil, nil, fmt.Errorf("orm: Payload %s dekodieren: %w", name, err)
		}
		return inst.Elem().Interface(), decl, nil
	}
	first, ok := d.upcasters[name][v]
	if !ok {
		return nil, nil, fmt.Errorf("orm: Upcaster für %s v%d fehlt", name, v)
	}
	inst := reflect.New(first.inType)
	if err := json.Unmarshal(data, inst.Interface()); err != nil {
		return nil, nil, fmt.Errorf("orm: Payload %s v%d dekodieren: %w", name, v, err)
	}
	cur := inst.Elem().Interface()
	for k := v; k < decl.version; k++ {
		uc, ok := d.upcasters[name][k]
		if !ok {
			return nil, nil, fmt.Errorf("orm: Upcaster für %s v%d fehlt", name, k)
		}
		next, err := uc.fn(cur)
		if err != nil {
			return nil, nil, fmt.Errorf("orm: Upcast %s v%d→v%d: %w", name, k, k+1, err)
		}
		cur = next
	}
	return cur, decl, nil
}

// validateUpcasters prüft beim Migrate, dass für jede in der DB vorhandene
// alte Event-Version eine lückenlose, typkonsistente Upcaster-Kette bis zur
// deklarierten Version existiert — Startfehler statt Lesefehler.
func (d *DB) validateUpcasters() error {
	for _, m := range d.reg.ordered {
		if m.kind != kindEventSourced {
			continue
		}
		d.esTypes.mu.RLock()
		known := make([]string, 0, len(d.esTypes.byName))
		for full := range d.esTypes.byName {
			known = append(known, full)
		}
		d.esTypes.mu.RUnlock()

		for _, full := range known {
			name, v, ok := m.es.parseType(full)
			if !ok {
				continue
			}
			decl := m.es.byName[name]
			if decl == nil {
				continue // Event eines anderen Models mit gleichem Präfix
			}
			if v > decl.version {
				return fmt.Errorf("orm: %s: DB enthält %s in v%d, deklariert ist nur v%d — App-Version zu alt?", m.name, name, v, decl.version)
			}
			var prevOut reflect.Type
			for k := v; k < decl.version; k++ {
				uc, ok := d.upcasters[name][k]
				if !ok {
					return fmt.Errorf("orm: %s: Upcaster für Event %q v%d→v%d fehlt (orm.Upcast registrieren)", m.name, name, k, k+1)
				}
				if prevOut != nil && uc.inType != prevOut {
					return fmt.Errorf("orm: %s: Upcaster-Kette für %q bricht bei v%d — Eingangstyp %s, erwartet %s", m.name, name, k, uc.inType, prevOut)
				}
				prevOut = uc.outType
			}
			if prevOut != nil && prevOut != decl.goType {
				return fmt.Errorf("orm: %s: letzter Upcaster für %q liefert %s statt des deklarierten Payload-Typs %s", m.name, name, prevOut, decl.goType)
			}
		}
	}
	return nil
}
