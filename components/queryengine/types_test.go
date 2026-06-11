package queryengine

import "testing"

func TestRequestDefaults(t *testing.T) {
	r := Request{SQL: "select 1"}
	if r.MaxRows != 0 {
		t.Fatalf("zero value MaxRows should be 0, caller sets it")
	}
}

func TestColumnRoundsTrip(t *testing.T) {
	c := Column{Name: "x", Type: "BIGINT"}
	if c.Name != "x" || c.Type != "BIGINT" {
		t.Fatal("field mismatch")
	}
}
