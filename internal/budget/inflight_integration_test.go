package budget

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestRedisInflightCapHeldDuringSlowRequests 用慢上游让请求保持在途，
// 直接观测集群在途数被压在 MaxInflight 以内。
func TestRedisInflightCapHeldDuringSlowRequests(t *testing.T) {
	rdb := testRedis(t)
	var inFlight atomic.Int32
	var maxObserved atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cur := inFlight.Add(1)
		for {
			m := maxObserved.Load()
			if cur <= m || maxObserved.CompareAndSwap(m, cur) {
				break
			}
		}
		time.Sleep(300 * time.Millisecond)
		inFlight.Add(-1)
		_, _ = w.Write([]byte(`{"items":[],"has_more":false}`))
	}))
	defer srv.Close()

	spec := Spec{Rate: 1000, Burst: 1000, MaxInflight: 2, MaxAttempts: 1,
		FallbackFraction: 0.5, Version: 1}
	// 同一站点、两个治理器（两个实例）共享 Redis 配额。
	g1 := newTestGovernor(t, rdb, "t-slow-inflight", spec)
	g2 := newTestGovernor(t, rdb, "t-slow-inflight", spec)
	ctx := context.Background()

	// 用真实 HTTP 客户端 + 治理器各发 4 个并发请求。
	doRequests := func(gov Governor) {
		var wg sync.WaitGroup
		for i := 0; i < 4; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				grant, err := gov.Acquire(ctx, 1)
				if err != nil {
					return
				}
				req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
				resp, err := http.DefaultClient.Do(req)
				if err == nil {
					resp.Body.Close()
				}
				gov.Release(ctx, grant)
			}()
		}
		wg.Wait()
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); doRequests(g1) }()
	go func() { defer wg.Done(); doRequests(g2) }()
	wg.Wait()

	if got := maxObserved.Load(); got > int32(spec.MaxInflight) {
		t.Fatalf("cluster inflight peaked at %d, exceeds cap %d", got, spec.MaxInflight)
	}
	if got := maxObserved.Load(); got < int32(spec.MaxInflight) {
		t.Fatalf("inflight never reached cap %d (peak %d); cap not exercised",
			spec.MaxInflight, got)
	}
}
