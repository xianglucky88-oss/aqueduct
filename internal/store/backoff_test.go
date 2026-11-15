package store

import (
	"testing"
	"time"
)

func TestBackoffDelayGrowsAndCaps(t *testing.T) {
	base := 5 * time.Second
	max := 2 * time.Minute

	// 抖动范围 ±20%，用上下界断言而非精确值。
	if d := BackoffDelay(1, base, max); d < 4*time.Second || d > 6*time.Second {
		t.Fatalf("failures=1: want ~5s, got %s", d)
	}
	prev := time.Duration(0)
	for _, f := range []int{1, 2, 3, 4, 5} {
		d := BackoffDelay(f, base, max)
		if d > max {
			t.Fatalf("failures=%d: %s exceeds cap %s", f, d, max)
		}
		if f >= 2 && d < prev/2 {
			t.Fatalf("failures=%d: backoff shrank unexpectedly: %s -> %s", f, prev, d)
		}
		prev = d
	}
	if d := BackoffDelay(100, base, max); d > max {
		t.Fatalf("large failure count must cap at %s, got %s", max, d)
	}
	if d := BackoffDelay(0, base, max); d < 4*time.Second {
		t.Fatalf("failures=0 should behave like 1, got %s", d)
	}
}

func TestIntervalArg(t *testing.T) {
	if got := intervalArg(1500 * time.Millisecond); got != "1500 milliseconds" {
		t.Fatalf("unexpected interval arg: %s", got)
	}
	// 亚毫秒必须向上取整，否则 now() + 0 会让租约立即过期。
	if got := intervalArg(0); got != "1 milliseconds" {
		t.Fatalf("zero duration must clamp to 1ms, got %s", got)
	}
}
