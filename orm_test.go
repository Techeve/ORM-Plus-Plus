package orm

import "testing"

func TestVersion(t *testing.T) {
	t.Parallel()
	if Version == "" {
		t.Fatal("Version darf nicht leer sein")
	}
}
