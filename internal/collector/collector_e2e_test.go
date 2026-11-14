package collector

// 端到端测试：真实 HTTP 上游 + 真实 Postgres + 真实采集流程。
//
// 需设置 AQUEDUCT_TEST_DSN，见 internal/store/integration_test.go。

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/internal-platform/aqueduct/internal/config"
	"github.com/internal-platform/aqueduct/internal/model"
	"github.com/internal-platform/aqueduct/internal/store"
)

// fakeUpstream 是一个可改数据、可回放的上游。
type fakeUpstream struct {
	mu    sync.Mutex
	items map[string]upItem
	order []string
}

type upItem struct {
	Title      string            `json:"title"`
	PriceCents int64             `json:"price_cents"`
	Stock      int               `json:"stock"`
	UpdatedAt  string            `json:"updated_at"`
	Extra      map[string]string `json:"extra,omitempty"`
}

func (f *fakeUpstream) set(id string, it upItem) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.items[id]; !ok {
		f.order = append(f.order, id)
	}
	f.items[id] = it
}

func (f *fakeUpstream) handler(pageSize int) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		offset := 0
		if cur := r.URL.Query().Get("cursor"); cur != "" {
			offset, _ = strconv.Atoi(cur)
		}
		end := offset + pageSize
		if end > len(f.order) {
			end = len(f.order)
		}
		if offset > len(f.order) {
			offset = len(f.order)
		}
		type wireItem struct {
			SourceID   string            `json:"source_id"`
			Title      string            `json:"title"`
			PriceCents int64             `json:"price_cents"`
			Stock      int               `json:"stock"`
			UpdatedAt  string            `json:"updated_at"`
			Extra      map[string]string `json:"extra"`
		}
		items := []wireItem{}
		for _, id := range f.order[offset:end] {
			it := f.items[id]
			items = append(items, wireItem{
				SourceID: id, Title: it.Title, PriceCents: it.PriceCents,
				Stock: it.Stock, UpdatedAt: it.UpdatedAt, Extra: it.Extra,
			})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"items": items, "next_cursor": strconv.Itoa(end), "has_more": end < len(f.order),
		})
	}
}

func e2eStore(t *testing.T) (*store.Store, context.Context) {
	t.Helper()
	dsn := os.Getenv("AQUEDUCT_TEST_DSN")
	if dsn == "" {
		t.Skip("AQUEDUCT_TEST_DSN not set; skipping end-to-end test")
	}
	ctx := context.Background()
	admin, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open admin: %v", err)
	}
	defer admin.Close()
	name := fmt.Sprintf("aq_e2e_%d", time.Now().UnixNano())
	if _, err := admin.ExecContext(ctx, `CREATE DATABASE `+name); err != nil {
		t.Fatalf("create db: %v", err)
	}
	t.Cleanup(func() {
		a, err := sql.Open("postgres", dsn)
		if err != nil {
			return
		}
		defer a.Close()
		_, _ = a.ExecContext(context.Background(), `DROP DATABASE IF EXISTS `+name)
	})
	slash := lastSlashBeforeQuery(dsn)
	testDSN := dsn[:slash+1] + name + dsn[slashAfterDB(dsn):]
	st, err := store.Open(ctx, testDSN)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	if err := st.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return st, ctx
}

func lastSlashBeforeQuery(dsn string) int {
	if i := indexByte(dsn, '?'); i >= 0 {
		if s := lastIndexByte(dsn[:i], '/'); s >= 0 {
			return s
		}
		return i - 1
	}
	return lastIndexByte(dsn, '/')
}

func slashAfterDB(dsn string) int {
	if i := indexByte(dsn, '?'); i >= 0 {
		return i
	}
	return len(dsn)
}

func indexByte(s string, c byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == c {
			return i
		}
	}
	return -1
}

func lastIndexByte(s string, c byte) int {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == c {
			return i
		}
	}
	return -1
}

func TestRunSiteTemporalEndToEnd(t *testing.T) {
	st, ctx := e2eStore(t)
	t0 := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	up := &fakeUpstream{items: map[string]upItem{}}
	up.set("SUP-1", upItem{Title: "a", PriceCents: 100, Stock: 5, UpdatedAt: t0.Format(time.RFC3339)})
	up.set("SUP-2", upItem{Title: "b", PriceCents: 200, Stock: 3, UpdatedAt: t0.Format(time.RFC3339)})
	up.set("SUP-3", upItem{Title: "c", PriceCents: 300, Stock: 1, UpdatedAt: t0.Format(time.RFC3339)})

	srv := httptest.NewServer(up.handler(2))
	defer srv.Close()
	site := config.Site{
		ID: "supplier", BaseURL: srv.URL, Path: "/items",
		Mode: "cursor", PageSize: 2, RPS: 1000,
	}
	col := New(&config.Config{}, st, nil)
	runAll := func(label string) model.Run {
		t.Helper()
		// 游标模式的成功一轮会把游标推到末尾；为了重复观测同批数据，
		// 这里在每轮前重置游标（上游本身是静态的回放源）。
		if err := st.SaveCursor(ctx, site.ID, ""); err != nil {
			t.Fatalf("%s: reset cursor: %v", label, err)
		}
		run, err := col.RunSite(ctx, site)
		if err != nil {
			t.Fatalf("%s: %v", label, err)
		}
		return run
	}

	run := runAll("run 1")
	if run.ItemsSeen != 3 || run.ItemsWrote != 3 {
		t.Fatalf("run 1: seen=%d wrote=%d", run.ItemsSeen, run.ItemsWrote)
	}

	// 重跑：内容未变，不应再写版本。
	run = runAll("run 2")
	if run.ItemsWrote != 0 || run.ItemsUnchanged != 3 {
		t.Fatalf("run 2: want wrote=0 unchanged=3, got %+v", run)
	}

	// 上游改价（业务时间前进）。
	up.set("SUP-1", upItem{Title: "a", PriceCents: 150, Stock: 5, UpdatedAt: t0.Add(time.Hour).Format(time.RFC3339)})
	run = runAll("run 3")
	if run.ItemsWrote != 1 {
		t.Fatalf("run 3: want wrote=1, got %+v", run)
	}

	// 每站点行数保持 3（身份稳定）。
	stats, err := st.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range stats.Sites {
		if s.SiteID != "supplier" {
			continue
		}
		if s.Rows != 3 || s.DistinctSource != 3 {
			t.Fatalf("rows=%d distinct=%d", s.Rows, s.DistinctSource)
		}
		if s.DuplicateRatio() != 0 {
			t.Fatalf("duplicate ratio = %v", s.DuplicateRatio())
		}
		if s.Versions != 4 { // 3 初版 + 1 改价
			t.Fatalf("versions = %d", s.Versions)
		}
	}
	if !stats.Migration.TemporalReady {
		t.Fatal("temporal reads should be ready")
	}

	// 历史还原。
	v, err := st.AsOf(ctx, "supplier", "SUP-1", t0.Add(30*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if v == nil || v.PriceCents != 100 {
		t.Fatalf("as-of before change must be 100, got %+v", v)
	}
	v, err = st.AsOf(ctx, "supplier", "SUP-1", t0.Add(2*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if v == nil || v.PriceCents != 150 {
		t.Fatalf("as-of after change must be 150, got %+v", v)
	}

	// 上游时间倒退：不得覆盖。
	up.set("SUP-1", upItem{Title: "a", PriceCents: 90, Stock: 5, UpdatedAt: t0.Add(-time.Hour).Format(time.RFC3339)})
	run = runAll("run 4")
	if run.ItemsWrote != 0 || run.ItemsStale != 1 {
		t.Fatalf("regression must be rejected, got %+v", run)
	}
	cur, err := st.Current(ctx, "supplier", "SUP-1")
	if err != nil {
		t.Fatal(err)
	}
	if cur.PriceCents != 150 {
		t.Fatalf("current price must stay 150, got %d", cur.PriceCents)
	}

	// 上游缺失时间：不得覆盖已知时间。
	up.set("SUP-1", upItem{Title: "a", PriceCents: 70, Stock: 5, UpdatedAt: ""})
	run = runAll("run 5")
	if run.ItemsWrote != 0 || run.ItemsStale != 1 {
		t.Fatalf("missing time must be rejected, got %+v", run)
	}

}
