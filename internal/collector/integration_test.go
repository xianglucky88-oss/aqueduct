package collector

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/internal-platform/aqueduct/internal/config"
	"github.com/internal-platform/aqueduct/internal/model"
	"github.com/internal-platform/aqueduct/internal/store"
	"github.com/internal-platform/aqueduct/internal/testdb"
)

// 采集流程的集成测试需要真实 PostgreSQL；未配置时跳过。
func testCollector(t *testing.T) (*Collector, *store.Store, *config.Config) {
	t.Helper()
	dsn := testdb.DSN(t, "itest_collector")
	ctx := context.Background()
	st, err := store.Open(ctx, dsn, 8, 2)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	if err := st.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	for _, q := range []string{
		`TRUNCATE records, runs, cursors, leases, site_runtime`,
		`DELETE FROM schema_steps`,
	} {
		if _, err := st.DB().ExecContext(ctx, q); err != nil {
			t.Fatalf("reset: %v", err)
		}
	}
	if err := st.Migrate(ctx); err != nil {
		t.Fatalf("re-migrate: %v", err)
	}
	cfg := &config.Config{
		WorkerID:           "test-worker",
		LeaseTTL:           15 * time.Second,
		LeaseRenewInterval: 5 * time.Second,
		ShutdownGrace:      5 * time.Second,
		SchedulerPoll:      time.Second,
		MaxConcurrentSites: 2,
		MaxPages:           50,
	}
	return New(cfg, st, nil), st, cfg
}

// pagedUpstream 提供一个 cursor 分页的上游，可控制页数与延迟。
func pagedUpstream(t *testing.T, pages int, delay time.Duration, failAt int32) *httptest.Server {
	t.Helper()
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&hits, 1)
		if failAt > 0 && n >= failAt {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("boom"))
			return
		}
		if delay > 0 {
			select {
			case <-time.After(delay):
			case <-r.Context().Done():
				return
			}
		}
		cur := r.URL.Query().Get("cursor")
		page := 0
		if cur != "" {
			page, _ = strconv.Atoi(cur)
		}
		next := page + 1
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"items":[{"source_id":"S-%d","title":"t","price_cents":%d,"stock":1,"updated_at":"2026-11-01T00:00:00Z"}],"next_cursor":"%d","has_more":%t}`,
			page, 100+page, next, next < pages)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func siteFor(srv *httptest.Server, mode string) config.Site {
	return config.Site{
		ID: "supplier", BaseURL: srv.URL, Path: "/supplier/items",
		Mode: mode, PageSize: 1, Interval: time.Minute, RPS: 1000,
	}
}

func leaseFor(t *testing.T, st *store.Store, site string) store.Lease {
	t.Helper()
	ctx := context.Background()
	if err := st.EnsureSites(ctx, []string{site}); err != nil {
		t.Fatal(err)
	}
	l, err := st.AcquireLease(ctx, site, "test-worker", 15*time.Second, true)
	if err != nil || l == nil {
		t.Fatalf("acquire: lease=%v err=%v", l, err)
	}
	return *l
}

func TestRunSiteCommitsAllPagesInOrder(t *testing.T) {
	col, st, _ := testCollector(t)
	srv := pagedUpstream(t, 3, 0, 0)
	site := siteFor(srv, "cursor")
	lease := leaseFor(t, st, site.ID)

	res := col.RunSite(context.Background(), lease, site)
	if res.Err != nil {
		t.Fatalf("run: %v", res.Err)
	}
	if res.Status != model.RunStatusSucceeded {
		t.Fatalf("want succeeded, got %s", res.Status)
	}
	// 3 页 → 3 条记录；游标收敛为空（本轮完成）。
	var rows int
	if err := st.DB().QueryRow(`SELECT count(*) FROM records WHERE site_id='supplier'`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 3 {
		t.Fatalf("want 3 rows, got %d", rows)
	}
	cur, _ := st.Cursor(context.Background(), site.ID)
	if cur != "" {
		t.Fatalf("completed round must clear cursor, got %q", cur)
	}
	if res.Run.ItemsSeen != 3 || res.Run.ItemsWrote != 3 {
		t.Fatalf("counters: seen=%d wrote=%d", res.Run.ItemsSeen, res.Run.ItemsWrote)
	}
}

// 上游在第 2 页失败：第 1 页已提交，游标停在页 1 的 next_cursor，
// 下一轮从检查点继续。
func TestRunSiteStopsAtFailingPageWithCheckpoint(t *testing.T) {
	col, st, _ := testCollector(t)
	srv := pagedUpstream(t, 5, 0, 2) // 第 2 次请求返回 500
	site := siteFor(srv, "cursor")
	lease := leaseFor(t, st, site.ID)

	res := col.RunSite(context.Background(), lease, site)
	if res.Err == nil {
		t.Fatal("expected failure")
	}
	if res.Status != model.RunStatusFailed {
		t.Fatalf("want failed, got %s", res.Status)
	}
	var rows int
	if err := st.DB().QueryRow(`SELECT count(*) FROM records WHERE site_id='supplier'`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("page 1 must be committed, got %d rows", rows)
	}
	cur, _ := st.Cursor(context.Background(), site.ID)
	if cur != "1" {
		t.Fatalf("checkpoint must be page 1 cursor, got %q", cur)
	}
}

// ctx 取消发生在页与页之间：已提交页保留，本轮记为 abandoned。
func TestRunSiteCancelKeepsCommittedPages(t *testing.T) {
	col, st, _ := testCollector(t)
	srv := pagedUpstream(t, 10, 300*time.Millisecond, 0)
	site := siteFor(srv, "cursor")
	lease := leaseFor(t, st, site.ID)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(500 * time.Millisecond)
		cancel()
	}()
	res := col.RunSite(ctx, lease, site)
	if res.Status != model.RunStatusAbandoned {
		t.Fatalf("want abandoned, got %s (err=%v)", res.Status, res.Err)
	}
	cur, _ := st.Cursor(context.Background(), site.ID)
	if cur == "" {
		t.Fatal("committed pages must leave a non-empty checkpoint")
	}
	var rows int
	if err := st.DB().QueryRow(`SELECT count(*) FROM records WHERE site_id='supplier'`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows == 0 {
		t.Fatal("committed pages must survive cancellation")
	}
}

// 租约被抢占后，本轮的写入必须被围栏拒绝并被记为 abandoned。
func TestRunSiteLosesLeaseMidRun(t *testing.T) {
	col, st, _ := testCollector(t)
	srv := pagedUpstream(t, 10, 300*time.Millisecond, 0)
	site := siteFor(srv, "cursor")
	lease := leaseFor(t, st, site.ID)

	go func() {
		time.Sleep(400 * time.Millisecond)
		// 模拟另一个副本接管：强制过期并由 worker-other 获取新围栏。
		_, _ = st.DB().Exec(`UPDATE leases SET expires_at = now() - interval '1 second'`)
		_, _ = st.DB().Exec(`UPDATE site_runtime SET next_run_at = now()`)
		_, _ = st.AcquireLease(context.Background(), site.ID, "worker-other", 15*time.Second, true)
	}()

	res := col.RunSite(context.Background(), lease, site)
	if res.Status != model.RunStatusAbandoned {
		t.Fatalf("want abandoned after lease loss, got %s (err=%v)", res.Status, res.Err)
	}
	// 接管方之后不应看到原持有者的新写入。
	var worker string
	if err := st.DB().QueryRow(`SELECT worker_id FROM leases WHERE site_id='supplier'`).Scan(&worker); err != nil {
		t.Fatal(err)
	}
	if worker != "worker-other" {
		t.Fatalf("lease should be held by worker-other, got %s", worker)
	}
}

// 内容变化不应产生新行，即使分多页重复采集。
func TestRunSiteIsIdempotentAcrossRounds(t *testing.T) {
	col, st, _ := testCollector(t)
	srv := pagedUpstream(t, 3, 0, 0)
	site := siteFor(srv, "cursor")

	for i := 0; i < 2; i++ {
		lease := leaseFor(t, st, site.ID)
		if res := col.RunSite(context.Background(), lease, site); res.Err != nil {
			t.Fatalf("round %d: %v", i, res.Err)
		}
		if _, err := st.FinishSite(context.Background(), lease, store.FinishOptions{
			Status: model.RunStatusSucceeded, Interval: time.Millisecond,
			BackoffBase: time.Second, BackoffMax: time.Minute,
		}); err != nil {
			t.Fatalf("finish round %d: %v", i, err)
		}
	}
	var rows, distinct int
	if err := st.DB().QueryRow(
		`SELECT count(*), count(DISTINCT source_id) FROM records WHERE site_id='supplier'`).Scan(&rows, &distinct); err != nil {
		t.Fatal(err)
	}
	if rows != 3 || distinct != 3 {
		t.Fatalf("repeat rounds must not duplicate: rows=%d distinct=%d", rows, distinct)
	}
}
