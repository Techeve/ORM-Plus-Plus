package orm

import (
	"crypto/rand"
	"database/sql/driver"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"time"
)

// ID ist der Primärschlüssel-Typ von ORM++ (UUIDv7: zeitlich sortierbar,
// clientseitig erzeugbar, kollisionfrei über verteilte Instanzen).
type ID [16]byte

// SingleTenant ist der beim Bootstrap automatisch angelegte Default-Tenant
// für Single-Tenant-Anwendungen (feste, wohlbekannte UUID).
var SingleTenant = ID{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x70, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01}

// NewID erzeugt eine neue UUIDv7.
func NewID() ID {
	var id ID
	ms := uint64(time.Now().UnixMilli())
	binary.BigEndian.PutUint64(id[:8], ms<<16)
	if _, err := rand.Read(id[6:]); err != nil {
		panic("orm: crypto/rand nicht verfügbar: " + err.Error())
	}
	id[6] = (id[6] & 0x0f) | 0x70 // Version 7
	id[8] = (id[8] & 0x3f) | 0x80 // Variant RFC 4122
	return id
}

// IsZero meldet, ob die ID ungesetzt ist.
func (id ID) IsZero() bool { return id == ID{} }

// String liefert die kanonische UUID-Darstellung.
func (id ID) String() string {
	var b [36]byte
	hex.Encode(b[:8], id[:4])
	b[8] = '-'
	hex.Encode(b[9:13], id[4:6])
	b[13] = '-'
	hex.Encode(b[14:18], id[6:8])
	b[18] = '-'
	hex.Encode(b[19:23], id[8:10])
	b[23] = '-'
	hex.Encode(b[24:], id[10:])
	return string(b[:])
}

// ParseID liest eine kanonische UUID-Darstellung.
func ParseID(s string) (ID, error) {
	var id ID
	if len(s) != 36 || s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
		return id, fmt.Errorf("orm: ungültige ID %q", s)
	}
	hexed := make([]byte, 0, 32)
	for _, part := range []string{s[:8], s[9:13], s[14:18], s[19:23], s[24:]} {
		hexed = append(hexed, part...)
	}
	if _, err := hex.Decode(id[:], hexed); err != nil {
		return id, fmt.Errorf("orm: ungültige ID %q: %w", s, err)
	}
	return id, nil
}

// Value implementiert driver.Valuer — IDs werden als TEXT gespeichert.
func (id ID) Value() (driver.Value, error) {
	if id.IsZero() {
		return nil, nil
	}
	return id.String(), nil
}

// Scan implementiert sql.Scanner.
func (id *ID) Scan(src any) error {
	switch v := src.(type) {
	case nil:
		*id = ID{}
		return nil
	case string:
		parsed, err := ParseID(v)
		if err != nil {
			return err
		}
		*id = parsed
		return nil
	case []byte:
		return id.Scan(string(v))
	default:
		return fmt.Errorf("orm: kann %T nicht in orm.ID lesen", src)
	}
}
