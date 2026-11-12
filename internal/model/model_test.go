package model

import (
	"testing"
	"time"
)

func TestIdentityStableForSameInput(t *testing.T) {
	r := Record{SiteID: "supplier", SourceID: "SUP-00001", PriceCents: 1234, Stock: 7}
	if r.Identity() != r.Identity() {
		t.Fatal("Identity must be deterministic")
	}
}

func TestDuplicateRatio(t *testing.T) {
	cases := []struct {
		rows, distinct int
		want           float64
	}{
		{100, 100, 0},
		{100, 80, 0.2},
		{0, 0, 0},
		{10, 0, 1},
	}
	for _, c := range cases {
		got := SiteStats{Rows: c.rows, DistinctSource: c.distinct}.DuplicateRatio()
		if diff := got - c.want; diff > 1e-9 || diff < -1e-9 {
			t.Fatalf("rows=%d distinct=%d want %.2f got %.6f", c.rows, c.distinct, c.want, got)
		}
	}
}

func TestRecordUpdatedAtZero(t *testing.T) {
	r := Record{}
	if !r.UpdatedAt.IsZero() {
		t.Fatal("zero value should have zero time")
	}
	_ = time.Now()
}
