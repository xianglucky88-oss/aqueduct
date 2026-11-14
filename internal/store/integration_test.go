package store

// 这些测试需要一个真实的 PostgreSQL。
//
// 默认跳过；设置 AQUEDUCT_TEST_DSN 指向一个已存在的空库即可运行：
//
//	AQUEDUCT_TEST_DSN='postgres://user@127.0.0.1:5432/aqueduct_test?sslmode=disable' \
//	  go test ./internal/store/ -run TestStore -count=1
//
// 每个测试使用独立 schema 前缀，互不干扰，可并行重复执行。

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/internal-platform/aqueduct/internal/model"
)

func testStore(t *testing.T) (*Store, context.Context) {
	t.Helper()
	dsn := os.Getenv("AQUEDUCT_TEST_DSN")
	if dsn == "" {
		t.Skip("AQUEDUCT_TEST_DSN not set; skipping real-Postgres test")
	}
	ctx := context.Background()
	// 每个测试用独立 database，避免并行测试互相清表。
	admin, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open admin: %v", err)
	}
	defer admin.Close()
	name := fmt.Sprintf("aq_test_%d", time.Now().UnixNano())
	if _, err := admin.ExecContext(ctx, `CREATE DATABASE `+name); err != nil {
		t.Fatalf("create database: %v", err)
	}
	t.Cleanup(func() {
		a, err := sql.Open("postgres", dsn)
		if err != nil {
			return
		}
		defer a.Close()
		_, _ = a.ExecContext(context.Background(), `DROP DATABASE IF EXISTS `+name)
	})

	testDSN := replaceDBName(dsn, name)
	st, err := Open(ctx, testDSN)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	if err := st.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return st, ctx
}

func replaceDBName(dsn, name string) string {
	// 支持 path 形式：.../dbname?params
	if i := strings.Index(dsn, "?"); i >= 0 {
		prefix := dsn[:i]
		slash := strings.LastIndex(prefix, "/")
		return prefix[:slash+1] + name + dsn[i:]
	}
	slash := strings.LastIndex(dsn, "/")
	return dsn[:slash+1] + name
}

func rec(site, source string, price int64, stock int, updated time.Time) model.Record {
	r := model.Record{
		SiteID: site, SourceID: source, Title: "t-" + source,
		PriceCents: price, Stock: stock, UpdatedAt: updated,
	}
	r.ItemKey = r.Identity()
	return r
}

func mustApply(t *testing.T, st *Store, ctx context.Context, recs ...model.Record) model.ApplyResult {
	t.Helper()
	res, err := st.Apply(ctx, recs)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	return res
}

func currentPrice(t *testing.T, st *Store, ctx context.Context, site, source string) (int64, int64) {
	t.Helper()
	v, err := st.Current(ctx, site, source)
	if err != nil {
		t.Fatalf("current: %v", err)
	}
	if v == nil {
		t.Fatalf("no current for %s/%s", site, source)
	}
	return v.PriceCents, v.Revision
}

func TestStoreIdentityStableAcrossPriceChange(t *testing.T) {
	st, ctx := testStore(t)
	t0 := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)

	mustApply(t, st, ctx, rec("supplier", "SUP-1", 100, 5, t0))
	mustApply(t, st, ctx, rec("supplier", "SUP-1", 250, 5, t0.Add(time.Hour)))

	price, rev := currentPrice(t, st, ctx, "supplier", "SUP-1")
	if price != 250 || rev != 2 {
		t.Fatalf("want price=250 rev=2, got price=%d rev=%d", price, rev)
	}
	if n := rowCount(t, st, `SELECT count(*) FROM records`); n != 1 {
		t.Fatalf("one source must have exactly one current row, got %d", n)
	}
}

func TestStoreIdempotentRepeat(t *testing.T) {
	st, ctx := testStore(t)
	t0 := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)

	for i := 0; i < 5; i++ {
		res := mustApply(t, st, ctx, rec("supplier", "SUP-1", 100, 5, t0))
		if i == 0 && res.Written != 1 {
			t.Fatalf("first apply must write, got %+v", res)
		}
		if i > 0 && res.Written != 0 {
			t.Fatalf("repeat must not write, got %+v", res)
		}
	}
	if n := rowCount(t, st, `SELECT count(*) FROM record_versions`); n != 1 {
		t.Fatalf("duplicate apply must not create versions, got %d", n)
	}
}

func TestStoreRejectsOlderUpdatedAt(t *testing.T) {
	st, ctx := testStore(t)
	t2 := time.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC)
	t1 := t2.Add(-time.Hour)

	mustApply(t, st, ctx, rec("supplier", "SUP-1", 200, 1, t2))
	res := mustApply(t, st, ctx, rec("supplier", "SUP-1", 50, 99, t1))
	if res.Written != 0 || res.Stale != 1 {
		t.Fatalf("older fact must be rejected, got %+v", res)
	}
	price, rev := currentPrice(t, st, ctx, "supplier", "SUP-1")
	if price != 200 || rev != 1 {
		t.Fatalf("newer fact must survive, got price=%d rev=%d", price, rev)
	}
}

func TestStoreRejectsMissingTimeOverKnown(t *testing.T) {
	st, ctx := testStore(t)
	known := time.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC)

	mustApply(t, st, ctx, rec("supplier", "SUP-1", 200, 1, known))
	res := mustApply(t, st, ctx, rec("supplier", "SUP-1", 1, 1, time.Time{}))
	if res.Written != 0 || res.Stale != 1 {
		t.Fatalf("missing time must not overwrite known fact, got %+v", res)
	}
	price, _ := currentPrice(t, st, ctx, "supplier", "SUP-1")
	if price != 200 {
		t.Fatalf("known fact must survive, got %d", price)
	}
}

func TestStoreAcceptsKnownTimeOverMissingCurrent(t *testing.T) {
	st, ctx := testStore(t)
	known := time.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC)

	mustApply(t, st, ctx, rec("supplier", "SUP-1", 10, 1, time.Time{}))
	res := mustApply(t, st, ctx, rec("supplier", "SUP-1", 20, 2, known))
	if res.Written != 1 {
		t.Fatalf("known time must win over missing, got %+v", res)
	}
	price, rev := currentPrice(t, st, ctx, "supplier", "SUP-1")
	if price != 20 || rev != 2 {
		t.Fatalf("got price=%d rev=%d", price, rev)
	}
}

// 同批次内乱序观测：结果必须与到达顺序无关，且以最新业务时间为准。
func TestStoreBatchOrderIndependent(t *testing.T) {
	t0 := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	obs := []model.Record{
		rec("supplier", "SUP-1", 300, 1, t0.Add(3*time.Hour)),
		rec("supplier", "SUP-1", 100, 1, t0.Add(1*time.Hour)),
		rec("supplier", "SUP-1", 200, 1, t0.Add(2*time.Hour)),
	}

	var prices []int64
	for _, order := range [][]int{{0, 1, 2}, {2, 1, 0}, {1, 2, 0}} {
		st, ctx := testStore(t)
		batch := []model.Record{obs[order[0]], obs[order[1]], obs[order[2]]}
		mustApply(t, st, ctx, batch...)
		p, _ := currentPrice(t, st, ctx, "supplier", "SUP-1")
		prices = append(prices, p)
	}
	for _, p := range prices {
		if p != 300 {
			t.Fatalf("batch must converge on newest fact, got %v", prices)
		}
	}
}

func TestStoreHistoryAndAsOf(t *testing.T) {
	st, ctx := testStore(t)
	t1 := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	t2 := t1.Add(24 * time.Hour)
	t3 := t2.Add(24 * time.Hour)

	mustApply(t, st, ctx, rec("supplier", "SUP-1", 100, 1, t1))
	mustApply(t, st, ctx, rec("supplier", "SUP-1", 200, 2, t2))
	mustApply(t, st, ctx, rec("supplier", "SUP-1", 300, 3, t3))

	hist, err := st.History(ctx, "supplier", "SUP-1", 10)
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	if len(hist) != 3 {
		t.Fatalf("want 3 versions, got %d", len(hist))
	}
	if hist[0].Revision != 3 || hist[0].PriceCents != 300 || hist[2].Revision != 1 {
		t.Fatalf("history order wrong: %+v", hist)
	}
	// 区间必须首尾相接、半开。
	for i := 0; i < len(hist)-1; i++ {
		newer, older := hist[i], hist[i+1]
		if older.ValidTo == nil || !older.ValidTo.Equal(newer.ValidFrom) {
			t.Fatalf("versions must tile: older.valid_to=%v newer.valid_from=%v",
				older.ValidTo, newer.ValidFrom)
		}
	}
	if hist[0].ValidTo != nil {
		t.Fatalf("current version must have open valid_to, got %v", hist[0].ValidTo)
	}

	cases := []struct {
		at    time.Time
		price int64
	}{
		{t1.Add(-time.Hour), -1}, // 尚不可见
		{t1, 100},
		{t2.Add(-time.Second), 100},
		{t2, 200},
		{t3, 300},
		{t3.Add(365 * 24 * time.Hour), 300},
	}
	for _, c := range cases {
		v, err := st.AsOf(ctx, "supplier", "SUP-1", c.at)
		if err != nil {
			t.Fatalf("as-of %v: %v", c.at, err)
		}
		if c.price == -1 {
			if v != nil {
				t.Fatalf("expected no version at %v, got %+v", c.at, v)
			}
			continue
		}
		if v == nil || v.PriceCents != c.price {
			t.Fatalf("as-of %v want %d, got %+v", c.at, c.price, v)
		}
	}
}

// 上游时间戳相同时，revision 必须给出确定顺序。
func TestStoreSameTimestampDeterministic(t *testing.T) {
	st, ctx := testStore(t)
	ts := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)

	mustApply(t, st, ctx, rec("supplier", "SUP-1", 100, 1, ts))
	res := mustApply(t, st, ctx, rec("supplier", "SUP-1", 200, 2, ts))
	if res.Written != 0 || res.Stale != 1 {
		t.Fatalf("same-timestamp conflict must be rejected deterministically, got %+v", res)
	}
	hist, _ := st.History(ctx, "supplier", "SUP-1", 10)
	if len(hist) != 1 || hist[0].PriceCents != 100 {
		t.Fatalf("same timestamp must not create a second version: %+v", hist)
	}
}

func rowCount(t *testing.T, st *Store, query string) int {
	t.Helper()
	var n int
	if err := st.db.QueryRow(query).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	return n
}
