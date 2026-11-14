package fetcher

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/internal-platform/aqueduct/internal/config"
)

func testSite(baseURL string) config.Site {
	return config.Site{
		ID:       "supplier",
		BaseURL:  baseURL,
		Path:     "/supplier/items",
		Mode:     "cursor",
		PageSize: 2,
		RPS:      1000, // 测试里不等待
	}
}

func TestPageParsesCursor(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("cursor"); got != "7" {
			t.Errorf("cursor not forwarded, got %q", got)
		}
		if got := r.URL.Query().Get("page_size"); got != "2" {
			t.Errorf("page_size not forwarded, got %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"items":[{"source_id":"SUP-1","title":"x","price_cents":100,"stock":3,
			          "updated_at":"2026-11-01T00:00:00Z","extra":{"region":"east"}}],
			"next_cursor":"8","has_more":true}`))
	}))
	defer srv.Close()

	c := New(testSite(srv.URL))
	p, err := c.Page(context.Background(), "7")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(p.Items) != 1 {
		t.Fatalf("want 1 item, got %d", len(p.Items))
	}
	if p.NextCursor != "8" || !p.HasMore {
		t.Fatalf("pagination lost: %+v", p)
	}
	if p.Items[0].ItemKey == "" {
		t.Fatal("ItemKey must be non-empty")
	}
	if !p.Items[0].UpdatedAt.Equal(time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("updated_at parsed wrong: %v", p.Items[0].UpdatedAt)
	}
}

func TestPageClassifiesRateLimit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	_, err := New(testSite(srv.URL)).Page(context.Background(), "")
	if !errors.Is(err, ErrRateLimited) {
		t.Fatalf("want ErrRateLimited, got %v", err)
	}
}

func TestPageClassifiesServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("boom"))
	}))
	defer srv.Close()

	_, err := New(testSite(srv.URL)).Page(context.Background(), "")
	if !errors.Is(err, ErrServerError) {
		t.Fatalf("want ErrServerError, got %v", err)
	}
}

func TestPageModeUsesPageParam(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("page"); got != "1" {
			t.Errorf("page param missing on first request, got %q", got)
		}
		_, _ = w.Write([]byte(`{"items":[],"next_cursor":"2","has_more":false}`))
	}))
	defer srv.Close()

	site := testSite(srv.URL)
	site.Mode = "page"
	if _, err := New(site).Page(context.Background(), ""); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// 非法时间必须保持零值（上游未提供业务时间），不能用本机时间伪造。
func TestBadUpdatedAtStaysMissing(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"items":[{"source_id":"S1","updated_at":"not-a-time"}],"has_more":false}`))
	}))
	defer srv.Close()

	p, err := New(testSite(srv.URL)).Page(context.Background(), "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !p.Items[0].UpdatedAt.IsZero() {
		t.Fatalf("bad timestamp must stay zero, got %v", p.Items[0].UpdatedAt)
	}
}

func TestPageEmptyUpdatedAtStaysMissing(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"items":[{"source_id":"S1","updated_at":""}],"has_more":false}`))
	}))
	defer srv.Close()

	p, err := New(testSite(srv.URL)).Page(context.Background(), "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !p.Items[0].UpdatedAt.IsZero() {
		t.Fatalf("absent timestamp must stay zero, got %v", p.Items[0].UpdatedAt)
	}
}
