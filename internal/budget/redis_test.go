package budget

import (
	"context"
	"math"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// testRedis 连接测试用 Redis；未设置 AQ_TEST_REDIS 或连接失败时跳过。
func testRedis(t *testing.T) *redis.Client {
	t.Helper()
	url := os.Getenv("AQ_TEST_REDIS")
	if url == "" {
		url = "redis://localhost:6379/15"
	}
	opt, err := redis.ParseURL(url)
	if err != nil {
		t.Skipf("parse redis url: %v", err)
	}
	rdb := redis.NewClient(opt)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		t.Skipf("redis unavailable: %v", err)
	}
	t.Cleanup(func() {
		_ = rdb.Close()
	})
	return rdb
}

func testSpec() Spec {
	return Spec{Rate: 50, Burst: 5, MaxInflight: 3, MaxAttempts: 1,
		FallbackFraction: 0.5, Version: 1}
}

func newTestGovernor(t *testing.T, rdb *redis.Client, site string, spec Spec) *redisGovernor {
	t.Helper()
	g := NewRedisGovernor(rdb, "worker-test", site, spec).(*redisGovernor)
	t.Cleanup(func() {
		ctx := context.Background()
		_ = rdb.Del(ctx, g.keyState, g.keyFlight).Err()
		_ = g.Close()
	})
	return g
}

func TestRedisAcquireAndRelease(t *testing.T) {
	rdb := testRedis(t)
	spec := testSpec()
	g := newTestGovernor(t, rdb, "t-acquire", spec)
	ctx := context.Background()

	grant, err := g.Acquire(ctx, 1)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if grant.Status != StatusGranted {
		t.Fatalf("first grant status = %s, want granted", grant.Status)
	}
	snap := g.Snapshot(ctx)
	if snap.Inflight != 1 {
		t.Fatalf("inflight = %d, want 1", snap.Inflight)
	}
	g.Release(ctx, grant)
	snap = g.Snapshot(ctx)
	if snap.Inflight != 0 {
		t.Fatalf("inflight after release = %d, want 0", snap.Inflight)
	}
}

func TestRedisInflightCapEnforcedClusterWide(t *testing.T) {
	rdb := testRedis(t)
	spec := testSpec()
	// 两个治理器模拟两个实例访问同一站点。
	g1 := newTestGovernor(t, rdb, "t-inflight", spec)
	g2 := newTestGovernor(t, rdb, "t-inflight", spec)
	ctx := context.Background()

	var grants []Grant
	for i := 0; i < spec.MaxInflight; i++ {
		grant, err := g1.Acquire(ctx, 1)
		if err != nil {
			t.Fatalf("acquire %d: %v", i, err)
		}
		grants = append(grants, grant)
	}

	// 共享在途已满：另一实例的申请必须等待。
	done := make(chan error, 1)
	go func() {
		_, err := g2.Acquire(ctx, 1)
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("acquire beyond inflight cap should block, got err=%v", err)
	case <-time.After(150 * time.Millisecond):
	}

	// 释放一个名额后，等待者应被放行。
	g1.Release(ctx, grants[0])
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("blocked acquire after release: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("blocked acquire not released after a slot freed")
	}
}

func TestRedisQuotaWaitRecorded(t *testing.T) {
	rdb := testRedis(t)
	spec := Spec{Rate: 5, Burst: 1, MaxInflight: 5, MaxAttempts: 1,
		FallbackFraction: 0.5, Version: 1}
	g := newTestGovernor(t, rdb, "t-wait", spec)
	ctx := context.Background()

	// 取走唯一令牌后，下一次申请必然经历配额等待。
	first, err := g.Acquire(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Release(ctx, first)

	start := time.Now()
	second, err := g.Acquire(ctx, 1)
	if err != nil {
		t.Fatalf("second acquire: %v", err)
	}
	defer g.Release(ctx, second)
	if second.Status != StatusQuotaWait {
		t.Fatalf("expected quota_wait, got %s", second.Status)
	}
	if time.Since(start) < 100*time.Millisecond {
		t.Fatalf("quota wait returned too fast: %v", time.Since(start))
	}
	if g.waits.Load() != 1 {
		t.Fatalf("waits counter = %d, want 1", g.waits.Load())
	}
}

func TestRedisAIMDShrinkAndRecover(t *testing.T) {
	rdb := testRedis(t)
	spec := Spec{Rate: 10, Burst: 10, MaxInflight: 5, MaxAttempts: 1,
		FallbackFraction: 0.5, Version: 1}
	g := newTestGovernor(t, rdb, "t-aimd", spec)
	ctx := context.Background()

	g.MarkLimited(ctx, time.Second)
	snap := g.Snapshot(ctx)
	if snap.EffectiveRate >= spec.Rate {
		t.Fatalf("after 429 effective rate must shrink: %v", snap.EffectiveRate)
	}
	if snap.CooldownUntil.IsZero() || snap.CooldownUntil.Before(time.Now()) {
		t.Fatal("cooldown must be set in the future")
	}
	shrunk := snap.EffectiveRate

	// 连续限流：平稳乘性收缩，不低于 10% 地板。
	for i := 0; i < 20; i++ {
		g.MarkLimited(ctx, 0)
	}
	snap = g.Snapshot(ctx)
	if snap.EffectiveRate > shrunk {
		t.Fatalf("repeated limits must not increase rate: %v > %v",
			snap.EffectiveRate, shrunk)
	}
	if snap.EffectiveRate < spec.Rate*MinEffect {
		t.Fatalf("rate below min floor: %v", snap.EffectiveRate)
	}

	// 成功后逐步加性恢复，但不超过配置速率。
	for i := 0; i < 100; i++ {
		g.MarkSuccess(ctx)
	}
	snap = g.Snapshot(ctx)
	if snap.EffectiveRate > spec.Rate {
		t.Fatalf("recovery exceeded configured rate: %v", snap.EffectiveRate)
	}
	if snap.EffectiveRate < spec.Rate {
		t.Fatalf("rate should fully recover after enough successes, got %v", snap.EffectiveRate)
	}
}

func TestRedisConfigVersionResetsBucket(t *testing.T) {
	rdb := testRedis(t)
	spec := Spec{Rate: 10, Burst: 10, MaxInflight: 5, MaxAttempts: 1,
		FallbackFraction: 0.5, Version: 1}
	g := newTestGovernor(t, rdb, "t-version", spec)
	ctx := context.Background()

	g.MarkLimited(ctx, 0)
	before := g.Snapshot(ctx).EffectiveRate
	if before >= spec.Rate {
		t.Fatal("precondition: rate should be shrunk")
	}

	// 配置变更：版本号变化后，下一次申请应把桶重置为满、有效速率复位。
	newSpec := spec
	newSpec.Version = 2
	g.UpdateSpec(newSpec)
	if _, err := g.Acquire(ctx, 1); err != nil {
		t.Fatalf("acquire after config change: %v", err)
	}
	snap := g.Snapshot(ctx)
	if snap.EffectiveRate != spec.Rate {
		t.Fatalf("config change must reset effective rate, got %v want %v",
			snap.EffectiveRate, spec.Rate)
	}
}

func TestRedisAcquireContextCancel(t *testing.T) {
	rdb := testRedis(t)
	spec := Spec{Rate: 5, Burst: 1, MaxInflight: 5, MaxAttempts: 1,
		FallbackFraction: 0.5, Version: 1}
	g := newTestGovernor(t, rdb, "t-cancel", spec)
	ctx := context.Background()

	first, err := g.Acquire(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Release(ctx, first)

	cctx, cancel := context.WithTimeout(ctx, 60*time.Millisecond)
	defer cancel()
	if _, err := g.Acquire(cctx, 1); err == nil {
		t.Fatal("expected context cancellation while waiting for tokens")
	}
}

func TestRedisFallbackWhenBackendLost(t *testing.T) {
	rdb := testRedis(t)
	spec := testSpec()
	g := newTestGovernor(t, rdb, "t-fallback", spec)
	ctx := context.Background()

	// 关闭后端模拟失联。
	_ = rdb.Close()

	grant, err := g.Acquire(ctx, 1)
	if err != nil {
		t.Fatalf("acquire must degrade, not fail: %v", err)
	}
	if grant.Status != StatusFallback {
		t.Fatalf("expected fallback grant, got %s", grant.Status)
	}
	if g.Snapshot(ctx).Mode != ModeFallback {
		t.Fatal("snapshot mode must be fallback")
	}
	g.Release(ctx, grant)
}

func TestRedisFractionalRatePrecisionPreserved(t *testing.T) {
	rdb := testRedis(t)
	// 选一个收缩后必然带小数的速率，回归 Lua number 被截成整数的缺陷。
	spec := Spec{Rate: 3, Burst: 5, MaxInflight: 5, MaxAttempts: 1,
		FallbackFraction: 0.5, Version: 1}
	g := newTestGovernor(t, rdb, "t-precision", spec)
	ctx := context.Background()

	// 3 * 0.7 = 2.1，必须保留小数而不是被截成整数 2（用容差消化浮点表示误差）。
	g.MarkLimited(ctx, 0)
	if got := g.Snapshot(ctx).EffectiveRate; math.Abs(got-2.1) > 1e-9 {
		t.Fatalf("effective rate truncated: got %v, want ~2.1", got)
	}
	// 2.1 + 3*0.05 = 2.25，再次确认恢复路径也不丢精度。
	g.MarkSuccess(ctx)
	if got := g.Snapshot(ctx).EffectiveRate; math.Abs(got-2.25) > 1e-9 {
		t.Fatalf("recovered rate truncated: got %v, want ~2.25", got)
	}
}

func TestRedisTwoGovernorsShareOneRate(t *testing.T) {
	rdb := testRedis(t)
	// 低速率 + 小突发，便于在短时间窗口内观察到集群级节流。
	spec := Spec{Rate: 4, Burst: 2, MaxInflight: 10, MaxAttempts: 1,
		FallbackFraction: 0.5, Version: 1}
	g1 := newTestGovernor(t, rdb, "t-shared", spec)
	g2 := newTestGovernor(t, rdb, "t-shared", spec)
	ctx := context.Background()

	var wg sync.WaitGroup
	wg.Add(2)
	granted := make(chan Grant, 20)
	run := func(gov Governor) {
		defer wg.Done()
		deadline := time.Now().Add(600 * time.Millisecond)
		for time.Now().Before(deadline) {
			grant, err := gov.Acquire(ctx, 1)
			if err != nil {
				return
			}
			granted <- grant
			gov.Release(ctx, grant)
		}
	}
	go run(g1)
	go run(g2)
	wg.Wait()
	close(granted)

	n := 0
	seenWait := false
	for grant := range granted {
		n++
		if grant.Status == StatusQuotaWait {
			seenWait = true
		}
	}
	// rate=4/s，0.6s 理论约 2.4 + burst 2 ≈ 4~5 个；放宽上界，关键是远低于
	// 两个独立 4/s 限流各跑满的量级（≥8），证明共享的是同一份配额。
	if n > 7 {
		t.Fatalf("cluster-wide rate not shared: %d grants in 600ms", n)
	}
	if n < 2 {
		t.Fatalf("too few grants, budget may be stuck: %d", n)
	}
	if !seenWait {
		t.Fatal("expected at least one quota_wait across the two instances")
	}
}
