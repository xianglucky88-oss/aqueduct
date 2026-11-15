package model

import (
	"testing"
)

func TestIdentityStableForSameInput(t *testing.T) {
	r := Record{SiteID: "supplier", SourceID: "SUP-00001", PriceCents: 1234, Stock: 7}
	if r.Identity() != r.Identity() {
		t.Fatal("Identity must be deterministic")
	}
}

// Identity 只绑定 (site_id, source_id)，价格/库存/标题变化不得改变键。
// 这是内容就地更新与多副本安全接管的前提。
func TestIdentityIgnoresMutableFields(t *testing.T) {
	a := Record{SiteID: "supplier", SourceID: "SUP-00001", PriceCents: 1234, Stock: 7, Title: "x"}
	b := a
	b.PriceCents = 1235
	b.Stock = 0
	b.Title = "y"
	if a.Identity() != b.Identity() {
		t.Fatal("key must be stable across mutable field changes")
	}
}

func TestIdentityDistinguishesSites(t *testing.T) {
	a := Record{SiteID: "supplier", SourceID: "S-1"}
	b := Record{SiteID: "catalog", SourceID: "S-1"}
	if a.Identity() == b.Identity() {
		t.Fatal("same source id on different sites must not collide")
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
