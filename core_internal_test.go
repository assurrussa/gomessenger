package messenger

import (
	"testing"
)

func TestSmallHelpers(t *testing.T) {
	if boolInt(false) != 0 || boolInt(true) != 1 || Kind("invalid").valid() || DataEncoding(99).valid() {
		t.Fatal("small helper contract changed")
	}
	if newObserverSet(noopLogger{}, nil) != nil {
		t.Fatal("empty observer set is not disabled")
	}
}
