package orm

import (
	"crypto/hmac"
	"crypto/sha256"
	"fmt"
	"reflect"
	"strings"
)

// Blind-Index für encrypted,lookup-Felder: neben der Ciphertext-Spalte
// pflegt die Engine eine deterministische Index-Spalte (HMAC-SHA-256 über
// den normalisierten Klartext). Gleichheits-Queries und unique laufen über
// diese Spalte — der Klartext bleibt der DB verborgen, gleiche Werte
// erzeugen aber gleiche Indizes (das ist der bewusste Trade-off eines
// Blind-Index: Gleichheits-Muster sind sichtbar, Werte nicht).
//
// Der Index-Schlüssel wird per HKDF-SHA-256 mit fester Ableitungs-Kennung
// aus dem AKTUELLEN Hauptschlüssel abgeleitet. Bei Schlüsselrotation gilt
// die Lazy-Strategie der Ciphertexte: Bestandszeilen behalten ihren alten
// Index, bis sie neu geschrieben werden — bis dahin findet eine
// Gleichheits-Query (die mit dem aktuellen Schlüssel hasht) sie nicht.
// tenants.Import berechnet Indizes grundsätzlich neu (abgeleitete Daten).

const lookupSuffix = "_lookup"

// lookupInfo ist die feste HKDF-Ableitungs-Kennung des Index-Schlüssels.
const lookupInfo = "ormpp-lookup-index-v1"

func (f *field) lookupColumn() string { return f.column + lookupSuffix }

// normalizeLookup ist die dokumentierte Vorgabe für string-lookup-Felder:
// Gleichheit gilt case-insensitiv und ohne Randleerraum.
func normalizeLookup(s string) string { return strings.ToLower(strings.TrimSpace(s)) }

// lookupIndexKey leitet den Index-Schlüssel aus dem aktuellen
// Hauptschlüssel ab: HKDF-SHA-256 (RFC 5869, ein Block) mit leerem Salt
// und lookupInfo als Info — Extract: PRK = HMAC(0^32, key),
// Expand: OKM = HMAC(PRK, info || 0x01).
func lookupIndexKey(p KeyProvider) ([]byte, error) {
	if p == nil {
		return nil, fmt.Errorf("orm: lookup-Feld ohne Encryption-Provider")
	}
	_, key, err := p.CurrentKey()
	if err != nil {
		return nil, err
	}
	extract := hmac.New(sha256.New, make([]byte, sha256.Size))
	extract.Write(key)
	prk := extract.Sum(nil)
	expand := hmac.New(sha256.New, prk)
	expand.Write([]byte(lookupInfo))
	expand.Write([]byte{0x01})
	return expand.Sum(nil), nil
}

// lookupHash berechnet den Index-Wert eines Klartexts. Leere (normalisierte)
// Werte liefern nil — die Index-Spalte wird NULL und kollidiert per
// SQL-Semantik nie (wie ein leeres unique-Feld).
func lookupHash(p KeyProvider, plain string) (any, error) {
	norm := normalizeLookup(plain)
	if norm == "" {
		return nil, nil
	}
	key, err := lookupIndexKey(p)
	if err != nil {
		return nil, err
	}
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(norm))
	return mac.Sum(nil), nil
}

// encodeLookup erzeugt den Index-Spaltenwert aus dem Struct-Feldwert
// (Pointer-nil → NULL, sonst Hash des Klartexts).
func encodeLookup(d *DB, f *field, v reflect.Value) (any, error) {
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return nil, nil
		}
		v = v.Elem()
	}
	return lookupHash(d.opts.keys, v.String())
}

// lookupQueryValue hasht einen Query-Parameter für die Index-Spalte.
func lookupQueryValue(d *DB, m *model, f *field, v any) (any, error) {
	s, ok := v.(string)
	if !ok {
		return nil, fmt.Errorf("orm: %s.%s: lookup-Vergleich braucht einen string, nicht %T", m.name, f.name, v)
	}
	return lookupHash(d.opts.keys, s)
}
