package orm

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"
	"unicode"
)

// onDelete-Verhalten einer Referenz.
type onDelete int

const (
	odRestrict onDelete = iota
	odCascade
	odSetNull
)

func (o onDelete) sql() string {
	switch o {
	case odCascade:
		return "CASCADE"
	case odSetNull:
		return "SET NULL"
	default:
		return "RESTRICT"
	}
}

// field ist der kompilierte Plan für ein persistiertes Struct-Feld.
type field struct {
	name     string // Go-Feldname
	column   string // Spaltenname
	index    []int  // reflect-Indexpfad
	goType   reflect.Type
	nullable bool // Pointer-Typ ⇒ NULL erlaubt

	pk         bool
	indexed    bool
	unique     bool
	json       bool
	version    bool
	autoCreate bool
	autoUpdate bool
	immutable  bool
	required   bool
	deprecated bool
	encrypted  bool
	enum       []string
	defaultVal string
	hasDefault bool

	refModel string // Go-Struct-Name des Ziel-Models ("" = keine Referenz)
	refOn    onDelete
	ref      *model // aufgelöst in resolve()
}

// model ist der kompilierte Plan eines registrierten Models.
type model struct {
	goType  reflect.Type
	name    string // Go-Struct-Name
	table   string
	kind    modelKind
	opts    modelOptions
	fields  []*field
	pk      *field
	version *field
	es      *esInfo // nur bei kindEventSourced
	// referencedBy: Modelle, die per ref auf dieses Model zeigen (für restrict-Prüfung).
	referencedBy []*model
}

func (m *model) tenanted() bool { return !m.opts.tenantFree }

// pkColumn liefert die Primärschlüssel-Spalte — bei ES-Modellen die
// implizite "id" des Read-Models.
func (m *model) pkColumn() string {
	if m.kind == kindEventSourced {
		return "id"
	}
	return m.pk.column
}

func (m *model) fieldByName(name string) *field {
	for _, f := range m.fields {
		if f.name == name {
			return f
		}
	}
	return nil
}

func (m *model) fieldByColumn(column string) *field {
	for _, f := range m.fields {
		if f.column == column {
			return f
		}
	}
	return nil
}

type registry struct {
	models  map[reflect.Type]*model
	byName  map[string]*model
	ordered []*model // Registrierungsreihenfolge
	errs    []error
}

func newRegistry() *registry {
	return &registry{
		models: map[reflect.Type]*model{},
		byName: map[string]*model{},
	}
}

func (r *registry) errf(format string, args ...any) {
	r.errs = append(r.errs, fmt.Errorf("orm: "+format, args...))
}

var (
	timeType = reflect.TypeOf(time.Time{})
	idType   = reflect.TypeOf(ID{})
)

// register kompiliert das Struct T zu einem Model-Plan.
func register(r *registry, t reflect.Type, mode ModelMode, opts ...ModelOption) {
	if t.Kind() != reflect.Struct {
		r.errf("Register[%s]: Model muss ein Struct sein", t)
		return
	}
	if _, dup := r.models[t]; dup {
		r.errf("Register[%s]: doppelt registriert", t.Name())
		return
	}

	mo := modelOptions{snapshotKeepLast: 2}
	for _, o := range opts {
		o(&mo)
	}

	m := &model{
		goType: t,
		name:   t.Name(),
		table:  snakeCase(t.Name()),
		kind:   mode.kind,
		opts:   mo,
	}

	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		if !sf.IsExported() || sf.Anonymous {
			continue
		}
		tag := sf.Tag.Get("orm")
		if tag == "-" {
			continue
		}
		f, err := parseField(sf, tag)
		if err != nil {
			r.errf("%s.%s: %v", m.name, sf.Name, err)
			continue
		}
		m.fields = append(m.fields, f)
		if f.pk {
			if m.pk != nil {
				r.errf("%s: mehr als ein pk-Feld", m.name)
			}
			m.pk = f
		}
		if f.version {
			m.version = f
		}
	}

	if mode.kind == kindEventSourced {
		if err := compileES(m, t, mo.events); err != nil {
			r.errf("%s: %v", m.name, err)
		}
	} else if m.pk == nil {
		r.errf("%s: kein pk-Feld deklariert", m.name)
	} else if m.pk.goType != idType {
		r.errf("%s: pk-Feld muss orm.ID sein", m.name)
	}

	for _, set := range append(mo.uniques, mo.indexes...) {
		for _, fn := range set {
			if m.fieldByName(fn) == nil {
				r.errf("%s: Unique/Index nennt unbekanntes Feld %q", m.name, fn)
			}
		}
	}

	r.models[t] = m
	r.byName[m.name] = m
	r.ordered = append(r.ordered, m)
}

var (
	aggregateType = reflect.TypeOf(Aggregate{})
	applierType   = reflect.TypeOf((*applier)(nil)).Elem()
)

// compileES validiert ein EventSourced-Model und kompiliert die Event-Deklarationen.
func compileES(m *model, t reflect.Type, decls []EventDecl) error {
	var aggIdx []int
	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		if sf.Anonymous && sf.Type == aggregateType {
			aggIdx = sf.Index
			break
		}
	}
	if aggIdx == nil {
		return fmt.Errorf("EventSourced-Model muss orm.Aggregate einbetten")
	}
	if !reflect.PointerTo(t).Implements(applierType) {
		return fmt.Errorf("Apply(orm.Event) error fehlt (Pointer-Receiver)")
	}
	if m.pk != nil {
		return fmt.Errorf("pk-Tag nicht erlaubt — die ID kommt aus orm.Aggregate")
	}
	if len(decls) == 0 {
		return fmt.Errorf("orm.Events(orm.E[...](...), …) fehlt")
	}
	for _, f := range m.fields {
		if f.column == "id" || f.column == "aggregate_seq" {
			return fmt.Errorf("das Feld %s kollidiert mit der impliziten Read-Model-Spalte %q", f.name, f.column)
		}
	}
	es := &esInfo{
		aggIdx: aggIdx,
		byType: map[reflect.Type]*eventDecl{},
		byName: map[string]*eventDecl{},
	}
	for _, d := range decls {
		if d.name == "" {
			return fmt.Errorf("Event ohne Namen deklariert")
		}
		if d.payload.Kind() != reflect.Struct {
			return fmt.Errorf("Event %q: Payload %s muss ein Struct sein", d.name, d.payload)
		}
		if d.version < 1 {
			return fmt.Errorf("Event %q: Version muss ≥ 1 sein", d.name)
		}
		if _, dup := es.byName[d.name]; dup {
			return fmt.Errorf("Event %q doppelt deklariert", d.name)
		}
		if _, dup := es.byType[d.payload]; dup {
			return fmt.Errorf("Payload-Typ %s doppelt deklariert", d.payload)
		}
		dd := &eventDecl{name: d.name, version: d.version, goType: d.payload}
		es.decls = append(es.decls, dd)
		es.byName[d.name] = dd
		es.byType[d.payload] = dd
	}
	m.es = es
	return nil
}

func parseField(sf reflect.StructField, tag string) (*field, error) {
	f := &field{
		name:   sf.Name,
		column: snakeCase(sf.Name),
		index:  sf.Index,
		goType: sf.Type,
	}
	if sf.Type.Kind() == reflect.Pointer {
		f.nullable = true
	}

	if tag != "" {
		for _, part := range strings.Split(tag, ",") {
			key, val, hasVal := strings.Cut(part, "=")
			key = strings.TrimSpace(key)
			switch key {
			case "pk":
				f.pk, f.immutable = true, true
			case "index":
				f.indexed = true
			case "unique":
				f.unique = true
			case "json":
				f.json = true
			case "version":
				f.version = true
			case "autocreate":
				f.autoCreate = true
			case "autoupdate":
				f.autoUpdate = true
			case "immutable":
				f.immutable = true
			case "required":
				f.required = true
			case "deprecated":
				f.deprecated = true
			case "enum":
				if !hasVal || val == "" {
					return nil, fmt.Errorf("enum ohne Wertemenge")
				}
				f.enum = strings.Split(val, "|")
			case "default":
				if !hasVal {
					return nil, fmt.Errorf("default ohne Wert")
				}
				f.defaultVal, f.hasDefault = val, true
			case "ref":
				if !hasVal || val == "" {
					return nil, fmt.Errorf("ref ohne Ziel-Model")
				}
				f.refModel = val
			case "ondelete":
				switch val {
				case "restrict":
					f.refOn = odRestrict
				case "cascade":
					f.refOn = odCascade
				case "setnull":
					f.refOn = odSetNull
				default:
					return nil, fmt.Errorf("unbekanntes ondelete %q", val)
				}
			case "encrypted":
				f.encrypted = true
			default:
				return nil, fmt.Errorf("unbekanntes Tag %q", key)
			}
		}
	}

	if f.required && f.hasDefault {
		return nil, fmt.Errorf("required und default sind nicht kombinierbar")
	}
	if f.enum != nil {
		base := f.goType
		if base.Kind() == reflect.Pointer {
			base = base.Elem()
		}
		if base.Kind() != reflect.String {
			return nil, fmt.Errorf("enum nur auf String-Feldern")
		}
	}
	if f.version {
		switch f.goType.Kind() {
		case reflect.Int, reflect.Int32, reflect.Int64:
		default:
			return nil, fmt.Errorf("version-Feld muss ein Integer sein")
		}
	}
	if (f.autoCreate || f.autoUpdate) && f.goType != timeType {
		return nil, fmt.Errorf("autocreate/autoupdate nur auf time.Time")
	}
	if f.refModel != "" {
		base := f.goType
		if base.Kind() == reflect.Pointer {
			base = base.Elem()
		}
		if base != idType {
			return nil, fmt.Errorf("ref-Feld muss orm.ID oder *orm.ID sein")
		}
	}
	if f.encrypted {
		if f.pk || f.indexed || f.unique || f.json || f.version || f.hasDefault || len(f.enum) > 0 || f.refModel != "" {
			return nil, fmt.Errorf("encrypted ist nicht kombinierbar mit pk/index/unique/json/version/default/enum/ref (Ciphertext ist nicht vergleichbar)")
		}
		base := f.goType
		if base.Kind() == reflect.Pointer {
			base = base.Elem()
		}
		isBytes := base.Kind() == reflect.Slice && base.Elem().Kind() == reflect.Uint8
		if base.Kind() != reflect.String && !isBytes {
			return nil, fmt.Errorf("encrypted nur auf string- oder []byte-Feldern")
		}
	}
	return f, nil
}

// resolve löst Referenzen auf und validiert modellübergreifende Regeln.
// Läuft bei Migrate, nachdem alle Register-Aufrufe erfolgt sind.
func (r *registry) resolve() error {
	if len(r.errs) > 0 {
		return joinErrs(r.errs)
	}
	var errs []error
	for _, m := range r.ordered {
		for _, f := range m.fields {
			if f.refModel == "" {
				continue
			}
			target, ok := r.byName[f.refModel]
			if !ok {
				errs = append(errs, fmt.Errorf("orm: %s.%s: ref-Ziel %q ist nicht registriert", m.name, f.name, f.refModel))
				continue
			}
			if !m.tenanted() && target.tenanted() {
				errs = append(errs, fmt.Errorf("orm: %s.%s: TenantFree-Model darf nicht auf tenant-gebundenes Model %q verweisen", m.name, f.name, f.refModel))
				continue
			}
			if f.refOn == odSetNull && !f.nullable {
				errs = append(errs, fmt.Errorf("orm: %s.%s: ondelete=setnull erfordert ein Pointer-Feld", m.name, f.name))
				continue
			}
			f.ref = target
			target.referencedBy = append(target.referencedBy, m)
		}
	}
	if len(errs) > 0 {
		return joinErrs(errs)
	}
	return nil
}

// sortedByDeps liefert die Modelle topologisch sortiert (Referenzziele zuerst),
// damit FK-DDL in gültiger Reihenfolge ausgeführt werden kann.
func (r *registry) sortedByDeps() ([]*model, error) {
	state := map[*model]int{} // 0 offen, 1 in Arbeit, 2 fertig
	var out []*model
	var visit func(m *model) error
	visit = func(m *model) error {
		switch state[m] {
		case 1:
			return fmt.Errorf("orm: zyklische Referenzen um Model %s", m.name)
		case 2:
			return nil
		}
		state[m] = 1
		for _, f := range m.fields {
			if f.ref != nil && f.ref != m {
				if err := visit(f.ref); err != nil {
					return err
				}
			}
		}
		state[m] = 2
		out = append(out, m)
		return nil
	}
	for _, m := range r.ordered {
		if err := visit(m); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// checksum liefert einen stabilen Hash über alle Modell-Deklarationen
// (Drift-Erkennung laut doc/ROADMAP.md).
func (r *registry) checksum() string {
	var lines []string
	for _, m := range r.ordered {
		var b strings.Builder
		fmt.Fprintf(&b, "%s|table=%s|kind=%d|tenantfree=%t|geo=%d", m.name, m.table, m.kind, m.opts.tenantFree, m.opts.geoMode)
		for _, u := range m.opts.uniques {
			fmt.Fprintf(&b, "|unique=%s", strings.Join(u, "+"))
		}
		for _, ix := range m.opts.indexes {
			fmt.Fprintf(&b, "|index=%s", strings.Join(ix, "+"))
		}
		for _, f := range m.fields {
			fmt.Fprintf(&b, "|f:%s:%s:%s:pk=%t:null=%t:json=%t:enum=%s:def=%s:ref=%s:od=%d:dep=%t:enc=%t",
				f.name, f.column, f.goType.String(), f.pk, f.nullable, f.json,
				strings.Join(f.enum, "|"), f.defaultVal, f.refModel, f.refOn, f.deprecated, f.encrypted)
		}
		lines = append(lines, b.String())
	}
	sort.Strings(lines)
	sum := sha256.Sum256([]byte(strings.Join(lines, "\n")))
	return hex.EncodeToString(sum[:])
}

func joinErrs(errs []error) error {
	msgs := make([]string, len(errs))
	for i, e := range errs {
		msgs[i] = e.Error()
	}
	return fmt.Errorf("%s", strings.Join(msgs, "; "))
}

// snakeCase wandelt Go-Namen in Spalten-/Tabellennamen um und behandelt
// Akronym-Läufe: "APIKey" → "api_key", "ProjectID" → "project_id".
func snakeCase(s string) string {
	runes := []rune(s)
	var b strings.Builder
	for i, r := range runes {
		if unicode.IsUpper(r) {
			prevLower := i > 0 && (unicode.IsLower(runes[i-1]) || unicode.IsDigit(runes[i-1]))
			nextLower := i+1 < len(runes) && unicode.IsLower(runes[i+1])
			if i > 0 && (prevLower || (unicode.IsUpper(runes[i-1]) && nextLower)) {
				b.WriteByte('_')
			}
			b.WriteRune(unicode.ToLower(r))
		} else {
			b.WriteRune(r)
		}
	}
	return b.String()
}
