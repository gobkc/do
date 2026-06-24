package postgres

import "testing"

func TestDialectPlaceholder(t *testing.T) {
	d := Dialect{}

	tests := []struct {
		n      int
		expect string
	}{
		{1, "$1"},
		{2, "$2"},
		{10, "$10"},
	}

	for _, tt := range tests {
		got := d.Placeholder(tt.n)
		if got != tt.expect {
			t.Errorf("Placeholder(%d) = %q, want %q", tt.n, got, tt.expect)
		}
	}
}
