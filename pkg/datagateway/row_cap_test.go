package datagateway

import "testing"

func TestRowCap_Take(t *testing.T) {
	c := newRowCap(10)
	steps := []struct {
		table   string
		n       int64
		allowed int64
		reached bool
	}{
		{"t", 4, 4, false},
		{"t", 8, 6, true},
		{"t", 1, 0, true},
		{"t2", 3, 3, false},
	}
	for i, s := range steps {
		a, r := c.take("b", s.table, s.n)
		if a != s.allowed || r != s.reached {
			t.Fatalf("step %d: take(%s,%d) = (%d,%v), want (%d,%v)", i, s.table, s.n, a, r, s.allowed, s.reached)
		}
	}
	if a, r := (*rowCap)(nil).take("b", "t", 5); a != 5 || r {
		t.Fatalf("nil cap = (%d,%v), want (5,false)", a, r)
	}
}
