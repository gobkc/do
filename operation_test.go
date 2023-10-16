package do

import (
	"testing"
)

func TestOneOr(t *testing.T) {
	v1 := OneOr(222, 111)
	if v1 != 222 {
		t.Errorf(`value error`)
	}
	v2 := OneOr(``, `bbb`)
	if v2 != `bbb` {
		t.Errorf(`value error`)
	}
}
