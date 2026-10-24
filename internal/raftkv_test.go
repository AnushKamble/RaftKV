package raftkv

import "testing"

func TestVersion(t *testing.T) {
	if Version == "" {
		t.Fatal("expected non-empty version")
	}
}
