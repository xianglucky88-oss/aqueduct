package store

import (
	"context"
	"testing"
	"time"

	"github.com/internal-platform/aqueduct/internal/model"
	"github.com/internal-platform/aqueduct/internal/testdb"
)

// 集成测试需要真实 PostgreSQL。未设置 AQUEDUCT_TEST_DATABASE_URL 时跳过，
// 保证 `go test ./...` 在无数据库环境下仍然可运行。
// 每个测试包使用独立 schema，避免与其他包并行时互相 TRUNCATE。
func testStore(t *testing.T) *Store {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	st, err := Open(ctx, testdb.DSN(t, "itest_store"), 8, 2)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := st.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	reset(t, st)
	return st
}

func reset(t *testing.T, st *Store) {
	t.Helper()
	ctx := context.Background()
	for _, q := range []string{
		`TRUNCATE records, runs, cursors, leases, site_runtime`,
		`DELETE FROM schema_steps`,
	} {
		if _, err := st.DB().ExecContext(ctx, q); err != nil {
			t.Fatalf("reset: %v", err)
		}
	}
	// 重新应用一次性迁移，保证 schema_steps 与索引状态一致。
	if err := st.Migrate(ctx); err != nil {
		t.Fatalf("re-migrate: %v", err)
	}
}

func mustAcquire(t *testing.T, st *Store, site, worker string, ttl time.Duration) *Lease {
	t.Helper()
	lease, err := st.AcquireLease(context.Background(), site, worker, ttl, false)
	if err != nil {
		t.Fatalf("acquire %s for %s: %v", site, worker, err)
	}
	if lease == nil {
		t.Fatalf("expected %s to acquire lease on %s", worker, site)
	}
	return lease
}

// makeDue 把站点推进到「下一轮已到期」，用于复现下一周期开始的竞争。
func makeDue(t *testing.T, st *Store, site string) {
	t.Helper()
	if _, err := st.DB().ExecContext(context.Background(),
		`UPDATE site_runtime SET next_run_at = now() WHERE site_id = $1`, site); err != nil {
		t.Fatalf("make %s due: %v", site, err)
	}
}

func TestLeaseIsExclusiveAndFenced(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	if err := st.EnsureSites(ctx, []string{"supplier"}); err != nil {
		t.Fatal(err)
	}

	a := mustAcquire(t, st, "supplier", "worker-a", 30*time.Second)

	// B 在 A 持有期间不得取得租约。
	got, err := st.AcquireLease(ctx, "supplier", "worker-b", 30*time.Second, false)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("worker-b must not acquire while worker-a holds the lease: %+v", got)
	}

	// A 续租成功，B 无法续租 A 的租约。
	if ok, err := st.RenewLease(ctx, *a, 30*time.Second); err != nil || !ok {
		t.Fatalf("owner renew: ok=%v err=%v", ok, err)
	}
	if ok, _ := st.RenewLease(ctx, Lease{SiteID: "supplier", WorkerID: "worker-b", Fence: a.Fence}, 30*time.Second); ok {
		t.Fatal("non-owner must not renew")
	}

	// A 释放租约并把站点置为到期（正常释放后站点仍需等到下一周期，
	// 因此这里显式让 next_run_at 到期，模拟 B 在下一轮开始时的竞争）。
	if err := st.ReleaseLease(ctx, *a); err != nil {
		t.Fatal(err)
	}
	makeDue(t, st, "supplier")
	b := mustAcquire(t, st, "supplier", "worker-b", 30*time.Second)
	if b.Fence <= a.Fence {
		t.Fatalf("fence must increase across takeovers: a=%d b=%d", a.Fence, b.Fence)
	}

	// 旧持有者 A 的写入必须被拒绝——这是防「僵尸写入」的核心。
	_, err = st.CommitPage(ctx, *a, "run-a", []model.Record{{SiteID: "supplier", SourceID: "S1", ItemKey: "k1", UpdatedAt: time.Now()}}, "")
	if err != ErrLeaseLost {
		t.Fatalf("stale fence must be rejected with ErrLeaseLost, got %v", err)
	}
}

func TestExpiredLeaseIsTakeoverable(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	if err := st.EnsureSites(ctx, []string{"supplier"}); err != nil {
		t.Fatal(err)
	}
	a := mustAcquire(t, st, "supplier", "worker-a", 30*time.Second)

	// 模拟进程崩溃：租约到期但未释放，且站点已到下一周期。
	if _, err := st.DB().ExecContext(ctx,
		`UPDATE leases SET expires_at = now() - interval '1 second' WHERE site_id = 'supplier'`); err != nil {
		t.Fatal(err)
	}
	makeDue(t, st, "supplier")

	b, err := st.AcquireLease(ctx, "supplier", "worker-b", 30*time.Second, false)
	if err != nil || b == nil {
		t.Fatalf("expired lease must be takeoverable: lease=%v err=%v", b, err)
	}
	if b.Fence <= a.Fence {
		t.Fatalf("takeover must bump fence: a=%d b=%d", a.Fence, b.Fence)
	}
}

func TestCommitPageIsAtomicCheckpoint(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	if err := st.EnsureSites(ctx, []string{"supplier"}); err != nil {
		t.Fatal(err)
	}
	lease := mustAcquire(t, st, "supplier", "worker-a", 30*time.Second)
	run := model.Run{ID: "run-1", SiteID: "supplier", WorkerID: "worker-a", StartedAt: time.Now().UTC()}
	if err := st.BeginSiteRun(ctx, *lease, run); err != nil {
		t.Fatal(err)
	}

	page1 := []model.Record{{SiteID: "supplier", SourceID: "S1", ItemKey: "k1", UpdatedAt: time.Now()}}
	if _, err := st.CommitPage(ctx, *lease, "run-1", page1, "cursor-1"); err != nil {
		t.Fatal(err)
	}

	// 数据、游标、计数三者必须同时可见。
	cur, err := st.Cursor(ctx, "supplier")
	if err != nil || cur != "cursor-1" {
		t.Fatalf("cursor after page commit: %q err=%v", cur, err)
	}
	var seen, wrote int
	if err := st.DB().QueryRowContext(ctx,
		`SELECT items_seen, items_wrote FROM runs WHERE run_id = 'run-1'`).Scan(&seen, &wrote); err != nil {
		t.Fatal(err)
	}
	if seen != 1 || wrote != 1 {
		t.Fatalf("run counters: seen=%d wrote=%d", seen, wrote)
	}

	// 模拟进程中途退出：不调用 FinishRun，租约到期、站点到期。
	if _, err := st.DB().ExecContext(ctx,
		`UPDATE leases SET expires_at = now() - interval '1 second'`); err != nil {
		t.Fatal(err)
	}
	makeDue(t, st, "supplier")

	// 新副本接管。BeginSiteRun 必须把遗留 running 轮次标记为 abandoned，
	// 且新轮次能从 package cursor 继续。
	leaseB := mustAcquire(t, st, "supplier", "worker-b", 30*time.Second)
	runB := model.Run{ID: "run-2", SiteID: "supplier", WorkerID: "worker-b", StartedAt: time.Now().UTC()}
	if err := st.BeginSiteRun(ctx, *leaseB, runB); err != nil {
		t.Fatal(err)
	}
	curB, err := st.Cursor(ctx, "supplier")
	if err != nil || curB != "cursor-1" {
		t.Fatalf("takeover must resume from checkpoint: %q err=%v", curB, err)
	}
	var status string
	if err := st.DB().QueryRowContext(ctx,
		`SELECT status FROM runs WHERE run_id = 'run-1'`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != model.RunStatusAbandoned {
		t.Fatalf("stale run must be abandoned on takeover, got %q", status)
	}
}

func TestUpsertUpdatesInPlace(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	first := model.Record{SiteID: "supplier", SourceID: "S1", PriceCents: 100, Stock: 5, UpdatedAt: now}
	first.ItemKey = first.Identity()
	if _, err := st.Upsert(ctx, []model.Record{first}); err != nil {
		t.Fatal(err)
	}

	// 同一业务主键、内容变化：键不变，应原地更新而非新增行。
	second := model.Record{SiteID: "supplier", SourceID: "S1", PriceCents: 999, Stock: 0, UpdatedAt: now.Add(time.Minute)}
	second.ItemKey = second.Identity()
	if _, err := st.Upsert(ctx, []model.Record{second}); err != nil {
		t.Fatal(err)
	}

	var rows int
	var price int64
	if err := st.DB().QueryRowContext(ctx,
		`SELECT count(*), max(price_cents) FROM records WHERE site_id='supplier' AND source_id='S1'`).Scan(&rows, &price); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("content change must not create rows: got %d rows", rows)
	}
	if price != 999 {
		t.Fatalf("content change must update in place: price=%d", price)
	}
}

func TestFinishSiteSchedulesNextRun(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	if err := st.EnsureSites(ctx, []string{"supplier"}); err != nil {
		t.Fatal(err)
	}

	// 成功：下一轮在 interval 之后，失败计数清零。
	lease := mustAcquire(t, st, "supplier", "worker-a", 30*time.Second)
	delay, err := st.FinishSite(ctx, *lease, FinishOptions{
		Status: model.RunStatusSucceeded, Interval: 30 * time.Second,
		BackoffBase: 5 * time.Second, BackoffMax: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if delay < 29*time.Second || delay > 31*time.Second {
		t.Fatalf("success must schedule next run at interval, got %s", delay)
	}

	// 失败：退避并在 site_runtime 记连续失败数。
	var failures int
	if err := st.DB().QueryRowContext(ctx,
		`SELECT consecutive_failures FROM site_runtime WHERE site_id='supplier'`).Scan(&failures); err != nil {
		t.Fatal(err)
	}
	if failures != 0 {
		t.Fatalf("success must clear failure count, got %d", failures)
	}

	// 让站点立即到期再取租约，模拟失败轮次。
	if _, err := st.DB().ExecContext(ctx, `UPDATE site_runtime SET next_run_at = now()`); err != nil {
		t.Fatal(err)
	}
	lease = mustAcquire(t, st, "supplier", "worker-a", 30*time.Second)
	delay, err = st.FinishSite(ctx, *lease, FinishOptions{
		Status: model.RunStatusFailed, Interval: 30 * time.Second,
		BackoffBase: 5 * time.Second, BackoffMax: time.Minute, LastError: "boom",
	})
	if err != nil {
		t.Fatal(err)
	}
	if delay < 4*time.Second {
		t.Fatalf("failure must back off, got %s", delay)
	}
	if err := st.DB().QueryRowContext(ctx,
		`SELECT consecutive_failures FROM site_runtime WHERE site_id='supplier'`).Scan(&failures); err != nil {
		t.Fatal(err)
	}
	if failures != 1 {
		t.Fatalf("failure count must increment, got %d", failures)
	}

	// 放弃（终止）：必须立即到期，让其他副本接管，避免遗漏。
	if _, err := st.DB().ExecContext(ctx, `UPDATE site_runtime SET next_run_at = now()`); err != nil {
		t.Fatal(err)
	}
	lease = mustAcquire(t, st, "supplier", "worker-a", 30*time.Second)
	delay, err = st.FinishSite(ctx, *lease, FinishOptions{
		Status: model.RunStatusAbandoned, Interval: 30 * time.Second,
		BackoffBase: 5 * time.Second, BackoffMax: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if delay > 100*time.Millisecond {
		t.Fatalf("abandoned site must become due immediately, got %s", delay)
	}
}

func TestDueSitesSkipsHeldLease(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	if err := st.EnsureSites(ctx, []string{"supplier", "catalog"}); err != nil {
		t.Fatal(err)
	}
	_ = mustAcquire(t, st, "supplier", "worker-a", 30*time.Second)

	due, err := st.DueSites(ctx, []string{"supplier", "catalog"})
	if err != nil {
		t.Fatal(err)
	}
	if len(due) != 1 || due[0] != "catalog" {
		t.Fatalf("only unheld sites may be due, got %v", due)
	}
}
