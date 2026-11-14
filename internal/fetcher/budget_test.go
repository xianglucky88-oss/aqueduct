package fetcher

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/internal-platform/aqueduct/internal/budget"
)

// fakeGov 是测试用预算治理器：记录申请/释放/反馈，并按需阻塞。
type fakeGov struct {
	mu        sync.Mutex
	spec      budget.Spec
	acquires  atomic.Int64
	releases  atomic.Int64
	limited   atomic.Int64
	success   atomic.Int64
	lastRetry time.Duration
	block     chan struct{}
}

func newFakeGov(maxAttempts int) *fakeGov {
	return &fakeGov{
		spec:  budget.Spec{Rate: 100, Burst: 100, MaxInflight: 5, MaxAttempts: maxAttempts},
		block: make(chan struct{}),
	}
}

func (f *fakeGov) SiteID() string { return "supplier" }
func (f *fakeGov) Spec() budget.Spec {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.spec
}
func (f *fakeGov) Acquire(ctx context.Context, attempt int) (budget.Grant, error) {
	f.acquires.Add(1)
	select {
	case <-f.block:
		return budget.Grant{}, errors.New("blocked")
	default:
	}
	return budget.Grant{SiteID: "supplier", Attempt: attempt, Status: budget.StatusGranted}, nil
}
func (f *fakeGov) Release(context.Context, budget.Grant) { f.releases.Add(1) }
func (f *fakeGov) MarkLimited(_ context.Context, ra time.Duration) {
	f.limited.Add(1)
	f.mu.Lock()
	f.lastRetry = ra
	f.mu.Unlock()
}
func (f *fakeGov) MarkSuccess(context.Context) { f.success.Add(1) }
func (f *fakeGov) Snapshot(context.Context) budget.Snapshot {
	return budget.Snapshot{SiteID: "supplier", Mode: budget.ModeLocal}
}
func (f *fakeGov) Close() error { return nil }

func TestRetries429UntilSuccessAndConsumesBudgetPerAttempt(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) <= 2 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		_, _ = w.Write([]byte(`{"items":[],"has_more":false}`))
	}))
	defer srv.Close()

	gov := newFakeGov(4)
	site := testSite(srv.URL)
	c := New(site, gov)

	if _, err := c.Page(context.Background(), ""); err != nil {
		t.Fatalf("page after retries: %v", err)
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("upstream calls = %d, want 3 (2 limited + 1 success)", got)
	}
	// 每次尝试都独立申请并释放预算。
	if got := gov.acquires.Load(); got != 3 {
		t.Fatalf("budget acquires = %d, want 3", got)
	}
	if got := gov.releases.Load(); got != 3 {
		t.Fatalf("budget releases = %d, want 3", got)
	}
	if got := gov.limited.Load(); got != 2 {
		t.Fatalf("MarkLimited = %d, want 2", got)
	}
	if got := gov.success.Load(); got != 1 {
		t.Fatalf("MarkSuccess = %d, want 1", got)
	}
}

func Test429ExhaustedReturnsRateLimited(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	gov := newFakeGov(2)
	_, err := New(testSite(srv.URL), gov).Page(context.Background(), "")
	if !errors.Is(err, ErrRateLimited) {
		t.Fatalf("want ErrRateLimited after attempts exhausted, got %v", err)
	}
	if got := gov.acquires.Load(); got != 2 {
		t.Fatalf("acquires = %d, want maxAttempts=2", got)
	}
}

func TestFatal4xxNotRetried(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	gov := newFakeGov(5)
	_, err := New(testSite(srv.URL), gov).Page(context.Background(), "")
	if err == nil {
		t.Fatal("expected 404 error")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("4xx must not be retried, calls=%d", got)
	}
}

func TestServerErrorRetried(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		_, _ = w.Write([]byte(`{"items":[],"has_more":false}`))
	}))
	defer srv.Close()

	gov := newFakeGov(3)
	if _, err := New(testSite(srv.URL), gov).Page(context.Background(), ""); err != nil {
		t.Fatalf("page should succeed after 502 retry: %v", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("calls = %d, want 2", got)
	}
}

func TestRetryAfterParsedAndReportedToGovernor(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "2")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	gov := newFakeGov(1)
	_, _ = New(testSite(srv.URL), gov).Page(context.Background(), "")
	gov.mu.Lock()
	ra := gov.lastRetry
	gov.mu.Unlock()
	if ra != 2*time.Second {
		t.Fatalf("Retry-After not propagated, got %v", ra)
	}
}

func TestParseRetryAfterVariants(t *testing.T) {
	if d := parseRetryAfter(""); d != 0 {
		t.Fatalf("empty = %v", d)
	}
	if d := parseRetryAfter("5"); d != 5*time.Second {
		t.Fatalf("seconds = %v", d)
	}
	future := time.Now().Add(3 * time.Second).UTC().Format(http.TimeFormat)
	d := parseRetryAfter(future)
	if d < 2*time.Second || d > 4*time.Second {
		t.Fatalf("http-date parse = %v", d)
	}
	if d := parseRetryAfter("garbage"); d != 0 {
		t.Fatalf("garbage = %v", d)
	}
}

func TestBudgetAcquireFailureAbortsWithoutHTTP(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("HTTP must not be called when budget acquisition fails")
	}))
	defer srv.Close()

	gov := newFakeGov(1)
	close(gov.block) // Acquire 立即失败
	_, err := New(testSite(srv.URL), gov).Page(context.Background(), "")
	if err == nil {
		t.Fatal("expected acquisition error to abort the page")
	}
}

func TestObserverSeesEveryAttempt(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		_, _ = w.Write([]byte(`{"items":[],"has_more":false}`))
	}))
	defer srv.Close()

	var observed atomic.Int32
	gov := newFakeGov(3)
	c := New(testSite(srv.URL), gov, WithObserver(func(siteID string, attempt, max int,
		waited time.Duration, status budget.AcquireStatus, outcome AttemptOutcome, err error) {
		observed.Add(1)
	}))
	if _, err := c.Page(context.Background(), ""); err != nil {
		t.Fatal(err)
	}
	if got := observed.Load(); got != 2 {
		t.Fatalf("observer invocations = %d, want 2", got)
	}
}
