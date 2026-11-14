package store

// 迁移路径的集成测试。需要 AQUEDUCT_TEST_DSN，见 integration_test.go。

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/internal-platform/aqueduct/internal/model"
)

// legacyStore 建一个迁移前结构的库，并灌入旧实现的重复行。
func legacyStore(t *testing.T) (*Store, context.Context) {
	t.Helper()
	st := emptyDB(t)
	ctx := context.Background()
	legacySchema := []string{
		`CREATE TABLE records (
			item_key     TEXT PRIMARY KEY,
			site_id      TEXT NOT NULL,
			source_id    TEXT NOT NULL,
			title        TEXT NOT NULL DEFAULT '',
			price_cents  BIGINT NOT NULL DEFAULT 0,
			stock        INTEGER NOT NULL DEFAULT 0,
			updated_at   TIMESTAMPTZ NOT NULL,
			payload      JSONB NOT NULL DEFAULT '{}'::jsonb,
			collected_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
		`CREATE TABLE runs (
			run_id      TEXT PRIMARY KEY,
			site_id     TEXT NOT NULL,
			started_at  TIMESTAMPTZ NOT NULL,
			finished_at TIMESTAMPTZ,
			status      TEXT NOT NULL,
			items_seen  INTEGER NOT NULL DEFAULT 0,
			items_wrote INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE TABLE cursors (
			site_id TEXT PRIMARY KEY, cursor TEXT NOT NULL DEFAULT '',
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
		`CREATE TABLE leases (
			site_id TEXT PRIMARY KEY, worker_id TEXT NOT NULL,
			expires_at TIMESTAMPTZ NOT NULL
		)`,
	}
	for _, s := range legacySchema {
		if _, err := st.db.ExecContext(ctx, s); err != nil {
			t.Fatalf("legacy schema: %v", err)
		}
	}
	return st, ctx
}

func emptyDB(t *testing.T) *Store {
	t.Helper()
	dsn := testDSN(t)
	admin, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open admin: %v", err)
	}
	name := fmt.Sprintf("aq_mig_%d", time.Now().UnixNano())
	if _, err := admin.ExecContext(context.Background(), `CREATE DATABASE `+name); err != nil {
		t.Fatalf("create db: %v", err)
	}
	admin.Close()
	t.Cleanup(func() {
		a, err := sql.Open("postgres", dsn)
		if err != nil {
			return
		}
		defer a.Close()
		_, _ = a.ExecContext(context.Background(), `DROP DATABASE IF EXISTS `+name)
	})
	st, err := Open(context.Background(), replaceDBName(dsn, name))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

// legacyInsert 按旧实现的键计算方式插入一行（价格/库存入哈希）。
func legacyInsert(t *testing.T, st *Store, ctx context.Context, r model.Record) string {
	t.Helper()
	legacy := model.Record{
		SiteID: r.SiteID, SourceID: r.SourceID,
		PriceCents: r.PriceCents, Stock: r.Stock,
	}
	// 复刻旧 Identity：含可变字段。
	sum := legacyIdentity(legacy)
	_, err := st.db.ExecContext(ctx, `
		INSERT INTO records (item_key, site_id, source_id, title, price_cents, stock,
		                     updated_at, payload)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
		sum, r.SiteID, r.SourceID, r.Title, r.PriceCents, r.Stock,
		r.UpdatedAt, toJSON(r.Payload))
	if err != nil {
		t.Fatalf("legacy insert: %v", err)
	}
	return sum
}

// legacyIdentity 复刻迁移前把可变字段纳入哈希的键算法。
func legacyIdentity(r model.Record) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s|%s|%d|%d",
		r.SiteID, r.SourceID, r.PriceCents, r.Stock)))
	return hex.EncodeToString(sum[:8])
}

func TestMigrateBuildsHistoryFromDuplicates(t *testing.T) {
	st, ctx := legacyStore(t)
	t0 := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)

	// 同一来源对象三次改价，旧实现留下三行。
	for i, price := range []int64{100, 200, 300} {
		legacyInsert(t, st, ctx, rec("supplier", "SUP-1", price, i, t0.Add(time.Duration(i)*time.Hour)))
	}
	// 另一个来源对象只有一行。
	legacyInsert(t, st, ctx, rec("supplier", "SUP-2", 999, 1, t0))
	// 别的站点。
	legacyInsert(t, st, ctx, rec("catalog", "SPU-1", 50, 1, t0))

	if n := rowCount(t, st, `SELECT count(*) FROM records`); n != 5 {
		t.Fatalf("legacy rows: want 5 got %d", n)
	}

	if err := st.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	// 当前状态：每个来源对象恰好一行。
	if n := rowCount(t, st, `SELECT count(*) FROM records`); n != 3 {
		t.Fatalf("after migrate: want 3 current rows got %d", n)
	}
	var dup int
	if err := st.db.QueryRowContext(ctx, `
		SELECT count(*) FROM (
			SELECT site_id, source_id FROM records GROUP BY 1,2 HAVING count(*) > 1
		) x`).Scan(&dup); err != nil {
		t.Fatal(err)
	}
	if dup != 0 {
		t.Fatalf("duplicate current objects remain: %d", dup)
	}

	// 历史链 3 版，最新价格成为当前状态。
	hist, err := st.History(ctx, "supplier", "SUP-1", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) != 3 {
		t.Fatalf("want 3 versions, got %d", len(hist))
	}
	if hist[0].PriceCents != 300 || hist[0].Revision != 3 {
		t.Fatalf("newest must be current, got %+v", hist[0])
	}
	if !hist[0].FromLegacy || !hist[2].FromLegacy {
		t.Fatal("migrated versions must be flagged from_legacy")
	}
	price, rev := currentPrice(t, st, ctx, "supplier", "SUP-1")
	if price != 300 || rev != 3 {
		t.Fatalf("current price/rev = %d/%d", price, rev)
	}

	// as-of 能还原每个历史时点。
	for i, want := range []int64{100, 200, 300} {
		at := t0.Add(time.Duration(i) * time.Hour)
		v, err := st.AsOf(ctx, "supplier", "SUP-1", at)
		if err != nil {
			t.Fatal(err)
		}
		if v == nil || v.PriceCents != want {
			t.Fatalf("as-of #%d want %d got %+v", i, want, v)
		}
	}
}

func TestMigrateIsIdempotent(t *testing.T) {
	st, ctx := legacyStore(t)
	t0 := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	for i, price := range []int64{100, 200, 300} {
		legacyInsert(t, st, ctx, rec("supplier", "SUP-1", price, i, t0.Add(time.Duration(i)*time.Hour)))
	}

	for round := 0; round < 3; round++ {
		if err := st.Migrate(ctx); err != nil {
			t.Fatalf("migrate round %d: %v", round, err)
		}
		if n := rowCount(t, st, `SELECT count(*) FROM records`); n != 1 {
			t.Fatalf("round %d: current rows %d", round, n)
		}
		if n := rowCount(t, st, `SELECT count(*) FROM record_versions`); n != 3 {
			t.Fatalf("round %d: versions %d (must not duplicate)", round, n)
		}
	}
}

// 中断恢复：删掉进度标记模拟崩溃，重跑必须收敛到同一结果。
func TestMigrateResumesAfterInterruption(t *testing.T) {
	st, ctx := legacyStore(t)
	t0 := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 5; i++ {
		legacyInsert(t, st, ctx, rec("supplier", fmt.Sprintf("SUP-%d", i), int64(100+i), i, t0))
	}
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	before, _ := st.History(ctx, "supplier", "SUP-2", 10)

	// 模拟中途崩溃：进度被清空，但历史已写入。
	if _, err := st.db.ExecContext(ctx, `DELETE FROM data_migrations`); err != nil {
		t.Fatal(err)
	}
	if err := st.Migrate(ctx); err != nil {
		t.Fatalf("resume migrate: %v", err)
	}
	after, _ := st.History(ctx, "supplier", "SUP-2", 10)
	if len(before) != len(after) {
		t.Fatalf("resume changed history length: %d -> %d", len(before), len(after))
	}
	if before[0].Revision != after[0].Revision || before[0].ContentHash != after[0].ContentHash {
		t.Fatalf("resume changed head: %+v -> %+v", before[0], after[0])
	}
}

// 迁移期间旧进程继续写入重复行：这些行必须被并入，且不覆盖更新的业务事实。
func TestMigrateMergesResidualLegacyDuplicates(t *testing.T) {
	st, ctx := legacyStore(t)
	t0 := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	legacyInsert(t, st, ctx, rec("supplier", "SUP-1", 100, 1, t0))
	legacyInsert(t, st, ctx, rec("supplier", "SUP-1", 200, 2, t0.Add(time.Hour)))
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}

	// 模拟迁移窗口内旧进程继续写入：此时唯一约束尚未建立
	// （Migrate 在全部回填完成后才建它）。测试里显式移除约束以复现该窗口。
	if _, err := st.db.ExecContext(ctx, `DROP INDEX IF EXISTS records_site_source_uidx`); err != nil {
		t.Fatal(err)
	}
	legacyInsert(t, st, ctx, rec("supplier", "SUP-1", 250, 3, t0.Add(2*time.Hour)))
	legacyInsert(t, st, ctx, rec("supplier", "SUP-1", 50, 9, t0.Add(-time.Hour)))

	if err := st.Migrate(ctx); err != nil {
		t.Fatalf("migrate residual: %v", err)
	}
	if n := rowCount(t, st, `SELECT count(*) FROM records`); n != 1 {
		t.Fatalf("residual duplicates not merged, current rows=%d", n)
	}
	price, _ := currentPrice(t, st, ctx, "supplier", "SUP-1")
	if price != 250 {
		t.Fatalf("newer residual fact must win, got %d", price)
	}
	hist, _ := st.History(ctx, "supplier", "SUP-1", 10)
	if len(hist) != 3 {
		t.Fatalf("want 3 versions (100,200,250), got %d", len(hist))
	}
	for _, v := range hist {
		if v.PriceCents == 50 {
			t.Fatal("older residual fact must not become a version")
		}
	}
}

// 迁移不依赖重新采集：迁移后无需任何新观测，历史查询立即可用。
func TestMigrateNoRecollectionNeeded(t *testing.T) {
	st, ctx := legacyStore(t)
	t0 := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	legacyInsert(t, st, ctx, rec("supplier", "SUP-1", 100, 1, t0))
	legacyInsert(t, st, ctx, rec("supplier", "SUP-1", 200, 2, t0.Add(time.Hour)))
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	// 不做任何 Apply。
	v, err := st.AsOf(ctx, "supplier", "SUP-1", t0.Add(30*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if v == nil || v.PriceCents != 100 {
		t.Fatalf("history must be queryable after migrate alone, got %+v", v)
	}
}

// 迁移后现有消费者的读取路径（按 item_key 读 records）必须仍然可用，
// 且被保留的键就是旧行里最新的那个。
func TestMigrateKeepsLegacyItemKeyForCurrentRow(t *testing.T) {
	st, ctx := legacyStore(t)
	t0 := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	var lastKey string
	for i, price := range []int64{100, 200, 300} {
		lastKey = legacyInsert(t, st, ctx, rec("supplier", "SUP-1", price, i, t0.Add(time.Duration(i)*time.Hour)))
	}
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	var got string
	var gotPrice int64
	if err := st.db.QueryRowContext(ctx,
		`SELECT item_key, price_cents FROM records WHERE site_id='supplier' AND source_id='SUP-1'`).
		Scan(&got, &gotPrice); err != nil {
		t.Fatal(err)
	}
	if got != lastKey {
		t.Fatalf("current row must keep the newest legacy item_key: want %s got %s", lastKey, got)
	}
	if gotPrice != 300 {
		t.Fatalf("current row price = %d", gotPrice)
	}
	// 旧键仍然命中（下游可能持有它）。
	var n int
	if err := st.db.QueryRowContext(ctx,
		`SELECT count(*) FROM records WHERE item_key = $1`, lastKey).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("legacy item_key must remain resolvable, got %d", n)
	}
}

// 新写入沿用当前行既有的 item_key，不会因改价换键。
func TestApplyKeepsItemKeyStableAfterMigration(t *testing.T) {
	st, ctx := legacyStore(t)
	t0 := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	key := legacyInsert(t, st, ctx, rec("supplier", "SUP-1", 100, 1, t0))
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	updated := rec("supplier", "SUP-1", 500, 1, t0.Add(time.Hour))
	updated.ItemKey = updated.Identity() // 新算法给出的键
	if _, err := st.Apply(ctx, []model.Record{updated}); err != nil {
		t.Fatal(err)
	}
	var got string
	if err := st.db.QueryRowContext(ctx,
		`SELECT item_key FROM records WHERE site_id='supplier' AND source_id='SUP-1'`).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != key {
		t.Fatalf("item_key changed on update: want %s got %s", key, got)
	}
}

func TestMigrateStatsReportsCompletion(t *testing.T) {
	st, ctx := legacyStore(t)
	t0 := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	legacyInsert(t, st, ctx, rec("supplier", "SUP-1", 100, 1, t0))
	legacyInsert(t, st, ctx, rec("catalog", "SPU-1", 100, 1, t0))
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	stats, err := st.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !stats.Migration.TemporalReady || len(stats.Migration.PendingSites) != 0 {
		t.Fatalf("migration should be complete: %+v", stats.Migration)
	}
	if stats.TotalRows != 2 {
		t.Fatalf("total rows = %d", stats.TotalRows)
	}
	for _, s := range stats.Sites {
		if s.Rows != s.DistinctSource {
			t.Fatalf("%s: rows %d != distinct %d", s.SiteID, s.Rows, s.DistinctSource)
		}
	}
}

func testDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("AQUEDUCT_TEST_DSN")
	if dsn == "" {
		t.Skip("AQUEDUCT_TEST_DSN not set; skipping real-Postgres test")
	}
	return dsn
}
