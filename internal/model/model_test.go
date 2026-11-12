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

// TestIdentityChangesWithPrice 记录当前行为：价格变化会产生不同的 ItemKey。
//
// 这个测试断言的是现有实现，改动该行为时需同步更新。
func TestIdentityChangesWithPrice(t *testing.T) {
	a := Record{SiteID: "supplier", SourceID: "SUP-00001", PriceCents: 1234, Stock: 7}
	b := a
	b.PriceCents = 1235
	if a.Identity() == b.Identity() {
		t.Fatal("current implementation folds price into the key; update this test when fixing #2")
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
