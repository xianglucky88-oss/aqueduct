package store

// 并发写入测试。需要 AQUEDUCT_TEST_DSN，见 integration_test.go。

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/internal-platform/aqueduct/internal/model"
)

// 同一来源对象被并发写入不同业务时间的观测：最终当前状态必须收敛到
// 业务时间最新的那次，历史链不得出现空洞或重复 revision。
func TestConcurrentApplyConvergesOnNewestFact(t *testing.T) {
	st, ctx := testStore(t)
	t0 := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)

	const workers = 8
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r := rec("supplier", "SUP-1", int64(100+i), i, t0.Add(time.Duration(i)*time.Minute))
			if _, err := st.Apply(ctx, []model.Record{r}); err != nil {
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent apply: %v", err)
	}

	v, err := st.Current(ctx, "supplier", "SUP-1")
	if err != nil {
		t.Fatal(err)
	}
	// 最新业务时间对应 worker 7（价格 107）。
	if v.PriceCents != 107 {
		t.Fatalf("concurrent writes must converge on newest fact, got price=%d rev=%d",
			v.PriceCents, v.Revision)
	}
	if n := rowCount(t, st, `SELECT count(*) FROM records`); n != 1 {
		t.Fatalf("one object must have one current row, got %d", n)
	}
	// revision 必须从 1 连续到当前 revision，无空洞。
	hist, err := st.History(ctx, "supplier", "SUP-1", 100)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < len(hist); i++ {
		wantRev := int64(len(hist) - i)
		if hist[i].Revision != wantRev {
			t.Fatalf("revision gap: got %d want %d", hist[i].Revision, wantRev)
		}
	}
	// 版本链区间必须闭合：较旧版本的 valid_to 等于相邻较新版本的 valid_from。
	for i := 0; i < len(hist)-1; i++ {
		newer, older := hist[i], hist[i+1]
		if older.ValidTo == nil || !older.ValidTo.Equal(newer.ValidFrom) {
			t.Fatalf("chain torn between revision %d and %d: newer.from=%v older.to=%v",
				newer.Revision, older.Revision, newer.ValidFrom, older.ValidTo)
		}
	}
	if hist[0].ValidTo != nil {
		t.Fatalf("head version must have open valid_to, got %v", hist[0].ValidTo)
	}
}

// 多站点并发：互不阻塞，各自收敛。
func TestConcurrentApplyAcrossSites(t *testing.T) {
	st, ctx := testStore(t)
	t0 := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	sites := []string{"supplier", "catalog", "stock", "notice"}

	var wg sync.WaitGroup
	for _, site := range sites {
		wg.Add(1)
		go func(site string) {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				r := rec(site, fmt.Sprintf("S-%02d", i), int64(i), i, t0)
				if _, err := st.Apply(ctx, []model.Record{r}); err != nil {
					t.Errorf("%s: %v", site, err)
					return
				}
			}
		}(site)
	}
	wg.Wait()

	for _, site := range sites {
		var n int
		if err := st.db.QueryRowContext(ctx,
			`SELECT count(*) FROM records WHERE site_id=$1`, site).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 20 {
			t.Fatalf("%s: want 20 rows, got %d", site, n)
		}
	}
}
