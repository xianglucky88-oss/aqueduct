package api

// 接口层的集成测试。需要 AQUEDUCT_TEST_DSN，见 internal/store/integration_test.go。

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/internal-platform/aqueduct/internal/config"
	"github.com/internal-platform/aqueduct/internal/model"
	"github.com/internal-platform/aqueduct/internal/store"
)

func apiFixture(t *testing.T) (*Server, *store.Store, context.Context) {
	t.Helper()
	dsn := os.Getenv("AQUEDUCT_TEST_DSN")
	if dsn == "" {
		t.Skip("AQUEDUCT_TEST_DSN not set; skipping API integration test")
	}
	ctx := context.Background()
	admin, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	name := fmt.Sprintf("aq_api_%d", time.Now().UnixNano())
	if _, err := admin.ExecContext(ctx, `CREATE DATABASE `+name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		a, _ := sql.Open("postgres", dsn)
		if a == nil {
			return
		}
		defer a.Close()
		_, _ = a.ExecContext(context.Background(), `DROP DATABASE IF EXISTS `+name)
	})
	q := ""
	for i := 0; i < len(dsn); i++ {
		if dsn[i] == '?' {
			q = dsn[i:]
			break
		}
	}
	slash := len(dsn) - len(q)
	for slash > 0 && dsn[slash-1] != '/' {
		slash--
	}
	st, err := store.Open(ctx, dsn[:slash]+name+q)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	return New(&config.Config{}, st, nil, nil), st, ctx
}

func doGet(t *testing.T, s *Server, path string) (int, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	s.http.Handler.ServeHTTP(rec, req)
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("%s: decode %q: %v", path, rec.Body.String(), err)
	}
	return rec.Code, body
}

func TestAPIHistoryAndAsOf(t *testing.T) {
	srv, st, ctx := apiFixture(t)
	t1 := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	t2 := t1.Add(24 * time.Hour)

	seed := []model.Record{
		{SiteID: "supplier", SourceID: "SUP-1", Title: "a", PriceCents: 100, Stock: 1, UpdatedAt: t1},
		{SiteID: "supplier", SourceID: "SUP-1", Title: "a", PriceCents: 200, Stock: 2, UpdatedAt: t2},
	}
	for _, r := range seed {
		r.ItemKey = r.Identity()
		if _, err := st.Apply(ctx, []model.Record{r}); err != nil {
			t.Fatal(err)
		}
	}

	code, body := doGet(t, srv, "/items/supplier/SUP-1")
	if code != http.StatusOK {
		t.Fatalf("current: %d %v", code, body)
	}
	if p, _ := body["price_cents"].(float64); p != 200 {
		t.Fatalf("current price = %v", body["price_cents"])
	}

	code, body = doGet(t, srv, "/items/supplier/SUP-1/history")
	if code != http.StatusOK {
		t.Fatalf("history: %d", code)
	}
	vs, _ := body["versions"].([]any)
	if len(vs) != 2 {
		t.Fatalf("want 2 versions, got %d", len(vs))
	}

	code, body = doGet(t, srv, "/items/supplier/SUP-1/as-of?at="+t1.Format(time.RFC3339))
	if code != http.StatusOK {
		t.Fatalf("as-of: %d %v", code, body)
	}
	v, _ := body["version"].(map[string]any)
	if p, _ := v["price_cents"].(float64); p != 100 {
		t.Fatalf("as-of price = %v", v["price_cents"])
	}

	// 早于任何版本 -> 404。
	code, _ = doGet(t, srv, "/items/supplier/SUP-1/as-of?at="+t1.Add(-time.Hour).Format(time.RFC3339))
	if code != http.StatusNotFound {
		t.Fatalf("pre-history as-of should be 404, got %d", code)
	}

	// 非法时间 -> 400。
	code, _ = doGet(t, srv, "/items/supplier/SUP-1/as-of?at=yesterday")
	if code != http.StatusBadRequest {
		t.Fatalf("bad at should be 400, got %d", code)
	}

	// 未知对象 -> 404。
	code, _ = doGet(t, srv, "/items/supplier/NOPE")
	if code != http.StatusNotFound {
		t.Fatalf("unknown item should be 404, got %d", code)
	}
}

func TestAPISnapshotAsOf(t *testing.T) {
	srv, st, ctx := apiFixture(t)
	t1 := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	t2 := t1.Add(time.Hour)

	items := []model.Record{
		{SiteID: "supplier", SourceID: "SUP-1", PriceCents: 100, UpdatedAt: t1},
		{SiteID: "supplier", SourceID: "SUP-2", PriceCents: 200, UpdatedAt: t1},
		{SiteID: "supplier", SourceID: "SUP-1", PriceCents: 150, UpdatedAt: t2},
	}
	for _, r := range items {
		r.ItemKey = r.Identity()
		if _, err := st.Apply(ctx, []model.Record{r}); err != nil {
			t.Fatal(err)
		}
	}

	code, body := doGet(t, srv, "/snapshot/supplier?at="+t1.Format(time.RFC3339))
	if code != http.StatusOK {
		t.Fatalf("snapshot: %d %v", code, body)
	}
	got, _ := body["items"].([]any)
	if len(got) != 2 {
		t.Fatalf("snapshot at t1: want 2 items, got %d", len(got))
	}
	for _, it := range got {
		m := it.(map[string]any)
		if m["source_id"] == "SUP-1" && m["price_cents"].(float64) != 100 {
			t.Fatalf("snapshot must show old price at t1: %v", m)
		}
	}

	code, body = doGet(t, srv, "/snapshot/supplier?at="+t2.Format(time.RFC3339))
	if code != http.StatusOK {
		t.Fatal("snapshot t2 failed")
	}
	got, _ = body["items"].([]any)
	if len(got) != 2 {
		t.Fatalf("snapshot at t2: want 2 items, got %d", len(got))
	}
	for _, it := range got {
		m := it.(map[string]any)
		if m["source_id"] == "SUP-1" && m["price_cents"].(float64) != 150 {
			t.Fatalf("snapshot must show new price at t2: %v", m)
		}
	}
}

func TestAPIStatsMigrationAndDuplicateRatio(t *testing.T) {
	srv, st, ctx := apiFixture(t)
	t0 := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 3; i++ {
		r := model.Record{SiteID: "supplier", SourceID: fmt.Sprintf("SUP-%d", i),
			PriceCents: int64(100 + i), UpdatedAt: t0}
		r.ItemKey = r.Identity()
		if _, err := st.Apply(ctx, []model.Record{r}); err != nil {
			t.Fatal(err)
		}
	}
	code, body := doGet(t, srv, "/stats")
	if code != http.StatusOK {
		t.Fatal("stats failed")
	}
	sites, _ := body["sites"].([]any)
	if len(sites) != 1 {
		t.Fatalf("want 1 site, got %d", len(sites))
	}
	s := sites[0].(map[string]any)
	if s["rows"].(float64) != 3 || s["distinct_source_ids"].(float64) != 3 {
		t.Fatalf("stats rows/distinct wrong: %v", s)
	}
	if s["duplicate_ratio"].(float64) != 0 {
		t.Fatalf("duplicate ratio = %v", s["duplicate_ratio"])
	}
	mi, _ := body["migration"].(map[string]any)
	if mi["temporal_reads_ready"] != true {
		t.Fatalf("migration readiness wrong: %v", mi)
	}
}
