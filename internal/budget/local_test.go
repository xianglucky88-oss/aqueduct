package budget

import (
	"context"
	"testing"
	"time"
)

func TestLocalLimiterBurstThenRefill(t *testing.T) {
	l := newLocalLimiter(10, 2, 5)
	now := time.Now()

	// 初始满桶：可连续拿到 burst 个。
	if ok, _ := l.tryAcquire(now); !ok {
		t.Fatal("first acquire should pass (full bucket)")
	}
	if ok, _ := l.tryAcquire(now); !ok {
		t.Fatal("second acquire should pass (burst=2)")
	}
	if ok, hint := l.tryAcquire(now); ok {
		t.Fatal("third acquire must block until refill")
	} else if hint <= 0 {
		t.Fatalf("blocked acquire should suggest a wait, got %v", hint)
	}

	// 补满一个令牌后可再次通过。
	later := now.Add(150 * time.Millisecond) // rate=10/s → 100ms 补 1 个
	if ok, _ := l.tryAcquire(later); !ok {
		t.Fatal("acquire after refill window should pass")
	}
}

func TestLocalLimiterInflightCap(t *testing.T) {
	l := newLocalLimiter(1000, 1000, 2)
	ctx := context.Background()
	if err := l.Acquire(ctx); err != nil {
		t.Fatal(err)
	}
	if err := l.Acquire(ctx); err != nil {
		t.Fatal(err)
	}
	// 在途达上限：非阻塞尝试必须失败。
	if ok, _ := l.tryAcquire(time.Now()); ok {
		t.Fatal("inflight cap must be enforced")
	}
	l.release()
	if ok, _ := l.tryAcquire(time.Now()); !ok {
		t.Fatal("after release an inflight slot must be available")
	}
}

func TestLocalLimiterAIMD(t *testing.T) {
	l := newLocalLimiter(10, 10, 5)
	l.shrink(0.7, 1)
	r, _, _ := l.view()
	if r >= 10 {
		t.Fatalf("shrink must reduce rate, got %v", r)
	}
	// 连续收缩不得低于 floor。
	for i := 0; i < 20; i++ {
		l.shrink(0.7, 1)
	}
	r, _, _ = l.view()
	if r < 1 {
		t.Fatalf("rate must not fall below floor, got %v", r)
	}
	// 加性恢复不得超过 cap。
	for i := 0; i < 100; i++ {
		l.recover(0.05, 10)
	}
	r, _, _ = l.view()
	if r > 10 {
		t.Fatalf("recovery must not exceed cap, got %v", r)
	}
}

func TestLocalLimiterAcquireRespectsContext(t *testing.T) {
	l := newLocalLimiter(1, 1, 1)
	if err := l.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := l.Acquire(ctx); err == nil {
		t.Fatal("expected context deadline while waiting for capacity")
	}
}
