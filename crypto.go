package orm

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"fmt"
)

// KeyProvider liefert den aktuellen Schreib-Schlüssel und löst Key-IDs beim
// Lesen auf — rotationsfähig von Tag 1: jeder Ciphertext trägt die ID des
// benutzten Schlüssels, Rotation erfolgt lazy beim nächsten Schreiben.
type KeyProvider interface {
	CurrentKey() (id string, key []byte, err error)
	Key(id string) ([]byte, error)
}

// StaticKey ist der einfachste Provider: genau ein Schlüssel (32 Bytes, AES-256).
func StaticKey(key []byte) KeyProvider { return staticKey{key: key} }

type staticKey struct{ key []byte }

func (s staticKey) CurrentKey() (string, []byte, error) {
	if len(s.key) != 32 {
		return "", nil, fmt.Errorf("orm: StaticKey braucht 32 Bytes (AES-256), hat %d", len(s.key))
	}
	return "static", s.key, nil
}

func (s staticKey) Key(id string) ([]byte, error) {
	if id != "static" {
		return nil, fmt.Errorf("orm: unbekannte Key-ID %q", id)
	}
	if len(s.key) != 32 {
		return nil, fmt.Errorf("orm: StaticKey braucht 32 Bytes (AES-256), hat %d", len(s.key))
	}
	return s.key, nil
}

// Encryption aktiviert die Feld-Verschlüsselung. Pflicht, sobald ein Model
// encrypted-Felder deklariert (sonst schlägt Migrate fehl).
func Encryption(p KeyProvider) OpenOption {
	return func(o *openOptions) { o.keys = p }
}

// Ciphertext-Layout: 0x01 | len(keyID) | keyID | Nonce(12) | AES-256-GCM(Plaintext).
const cipherVersion = 0x01

func encryptValue(p KeyProvider, plain []byte) ([]byte, error) {
	id, key, err := p.CurrentKey()
	if err != nil {
		return nil, err
	}
	if len(id) == 0 || len(id) > 255 {
		return nil, fmt.Errorf("orm: Key-ID %q muss 1–255 Bytes lang sein", id)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("orm: Verschlüsselung: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	out := make([]byte, 0, 2+len(id)+len(nonce)+len(plain)+gcm.Overhead())
	out = append(out, cipherVersion, byte(len(id)))
	out = append(out, id...)
	out = append(out, nonce...)
	return gcm.Seal(out, nonce, plain, nil), nil
}

func decryptValue(p KeyProvider, blob []byte) ([]byte, error) {
	if len(blob) < 2 || blob[0] != cipherVersion {
		return nil, fmt.Errorf("orm: unbekanntes Ciphertext-Format")
	}
	idLen := int(blob[1])
	if len(blob) < 2+idLen {
		return nil, fmt.Errorf("orm: Ciphertext beschädigt")
	}
	id := string(blob[2 : 2+idLen])
	key, err := p.Key(id)
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	rest := blob[2+idLen:]
	if len(rest) < gcm.NonceSize() {
		return nil, fmt.Errorf("orm: Ciphertext beschädigt")
	}
	plain, err := gcm.Open(nil, rest[:gcm.NonceSize()], rest[gcm.NonceSize():], nil)
	if err != nil {
		return nil, fmt.Errorf("orm: Entschlüsselung fehlgeschlagen (Key %q): %w", id, err)
	}
	return plain, nil
}
