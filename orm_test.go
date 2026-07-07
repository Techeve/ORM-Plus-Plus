package orm

import "testing"

func TestVersion(t *testing.T) {
	if Version == "" {
		t.Fatal("Version darf nicht leer sein")
	}
}
