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

// 身份只由 (site_id, source_id) 决定，调价/改库存不得产生新对象。
func TestIdentityIgnoresMutableFields(t *testing.T) {
	a := Record{SiteID: "supplier", SourceID: "SUP-00001", PriceCents: 1234, Stock: 7, Title: "x"}
	b := a
	b.PriceCents = 9999
	b.Stock = 0
	b.Title = "y"
	b.Payload = map[string]string{"region": "west"}
	if a.Identity() != b.Identity() {
		t.Fatal("mutable fields must not affect identity")
	}
}

func TestIdentityIncludesSite(t *testing.T) {
	if (Record{SiteID: "a", SourceID: "S"}).Identity() ==
		(Record{SiteID: "b", SourceID: "S"}).Identity() {
		t.Fatal("site must be part of identity")
	}
}

func TestContentHashStable(t *testing.T) {
	r := Record{Title: "t", PriceCents: 10, Stock: 2, Payload: map[string]string{"b": "2", "a": "1"}}
	if r.ContentHash() != r.ContentHash() {
		t.Fatal("ContentHash must be deterministic")
	}
	// map 顺序不得影响哈希。
	r2 := Record{Title: "t", PriceCents: 10, Stock: 2, Payload: map[string]string{"a": "1", "b": "2"}}
	if r.ContentHash() != r2.ContentHash() {
		t.Fatal("payload key order must not affect ContentHash")
	}
	// 时间不属于内容。
	r3 := r2
	r3.UpdatedAt = time.Now()
	if r.ContentHash() != r3.ContentHash() {
		t.Fatal("updated_at must not affect ContentHash")
	}
}

func TestContentHashChangesWithContent(t *testing.T) {
	base := Record{Title: "t", PriceCents: 10, Stock: 2}
	cases := []Record{
		{Title: "u", PriceCents: 10, Stock: 2},
		{Title: "t", PriceCents: 11, Stock: 2},
		{Title: "t", PriceCents: 10, Stock: 3},
		{Title: "t", PriceCents: 10, Stock: 2, Payload: map[string]string{"region": "east"}},
	}
	seen := map[string]bool{base.ContentHash(): true}
	for _, c := range cases {
		if seen[c.ContentHash()] {
			t.Fatalf("content change must change hash: %+v", c)
		}
		seen[c.ContentHash()] = true
	}
}

func TestDecideUpdate(t *testing.T) {
	t0 := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Hour)
	zero := time.Time{}

	cases := []struct {
		name     string
		cur, obs time.Time
		want     Decision
	}{
		{"newer wins", t0, t1, Accept},
		{"same timestamp conflicts", t0, t0, RejectStale},
		{"older loses", t1, t0, RejectStale},
		{"observed known, current missing", zero, t0, Accept},
		{"observed missing, current known", t0, zero, RejectMissingTime},
		{"both missing", zero, zero, Accept},
	}
	for _, c := range cases {
		if got := DecideUpdate(c.cur, c.obs); got != c.want {
			t.Fatalf("%s: got %v want %v", c.name, got, c.want)
		}
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
	if r.HasUpdatedAt() {
		t.Fatal("zero value must report missing updated_at")
	}
}
