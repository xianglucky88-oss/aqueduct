package store

import (
	"testing"
	"time"

	"github.com/internal-platform/aqueduct/internal/model"
)

func makeRec(source string, price int64, at time.Time) model.Record {
	r := model.Record{SiteID: "supplier", SourceID: source, PriceCents: price, UpdatedAt: at}
	r.ItemKey = r.Identity()
	return r
}

func TestFoldRecordsOrderIndependent(t *testing.T) {
	t0 := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	a := makeRec("S", 100, t0)
	b := makeRec("S", 200, t0.Add(time.Hour))
	c := makeRec("S", 150, t0.Add(30*time.Minute))

	orders := [][]model.Record{{a, b, c}, {c, b, a}, {b, a, c}, {b, c, a}}
	for _, in := range orders {
		got, stale := foldRecords(in)
		if len(got) != 1 {
			t.Fatalf("want 1 folded record, got %d", len(got))
		}
		if got[0].PriceCents != 200 {
			t.Fatalf("must keep newest fact, got %d", got[0].PriceCents)
		}
		if stale != 2 {
			t.Fatalf("want 2 superseded observations, got %d", stale)
		}
	}
}

func TestFoldRecordsIdenticalDuplicatesNotStale(t *testing.T) {
	t0 := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	a := makeRec("S", 100, t0)
	got, stale := foldRecords([]model.Record{a, a, a})
	if len(got) != 1 {
		t.Fatalf("want 1, got %d", len(got))
	}
	if stale != 0 {
		t.Fatalf("identical duplicates must not count as stale, got %d", stale)
	}
}

func TestFoldRecordsMissingTimeLosesToKnown(t *testing.T) {
	t0 := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	known := makeRec("S", 100, t0)
	missing := makeRec("S", 999, time.Time{})

	for _, in := range [][]model.Record{{known, missing}, {missing, known}} {
		got, _ := foldRecords(in)
		if len(got) != 1 || got[0].PriceCents != 100 {
			t.Fatalf("known time must win regardless of order, got %+v", got)
		}
	}
}

func TestFoldRecordsMultipleObjects(t *testing.T) {
	t0 := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	in := []model.Record{
		makeRec("A", 1, t0), makeRec("B", 2, t0), makeRec("A", 3, t0.Add(time.Hour)),
	}
	got, stale := foldRecords(in)
	if len(got) != 2 || stale != 1 {
		t.Fatalf("want 2 folded / 1 stale, got %d / %d", len(got), stale)
	}
	if got[0].SourceID != "A" || got[0].PriceCents != 3 {
		t.Fatalf("object A must fold to newest, got %+v", got[0])
	}
}
