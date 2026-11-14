package budget

import (
	"context"
	"math"
	"sync"
	"time"
)

// localLimiter 是实例内的令牌桶 + 在途信号量。
//
// 两种用途：
//   - 未配置共享存储时的 local 模式：按全量配额限流（复刻历史单机语义）；
//   - 共享存储失联时的 fallback 模式：按 FallbackFraction 折减后的配额限流。
type localLimiter struct {
	mu        sync.Mutex
	rate      float64
	burst     float64
	tokens    float64
	updated   time.Time
	inflight  int
	maxFlight int
}

func newLocalLimiter(rate float64, burst, maxFlight int) *localLimiter {
	if rate <= 0 {
		rate = 1
	}
	if burst < 1 {
		burst = 1
	}
	if maxFlight < 1 {
		maxFlight = 1
	}
	return &localLimiter{
		rate:      rate,
		burst:     float64(burst),
		tokens:    float64(burst),
		updated:   time.Now(),
		maxFlight: maxFlight,
	}
}

// tryAcquire 非阻塞尝试一次。返回是否成功；失败时给出建议等待时长。
func (l *localLimiter) tryAcquire(now time.Time) (bool, time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()

	elapsed := now.Sub(l.updated).Seconds()
	if elapsed > 0 {
		l.tokens = math.Min(l.burst, l.tokens+elapsed*l.rate)
		l.updated = now
	}
	if l.tokens >= 1 && l.inflight < l.maxFlight {
		l.tokens--
		l.inflight++
		return true, 0
	}
	wait := time.Duration(math.Max(0, (1-l.tokens)/l.rate) * float64(time.Second))
	return false, maxDur(PollMin, minDur(wait, PollMax))
}

func (l *localLimiter) release() {
	l.mu.Lock()
	if l.inflight > 0 {
		l.inflight--
	}
	l.mu.Unlock()
}

// shrink 乘性下调本地速率，但不低于 floor。
func (l *localLimiter) shrink(factor, floor float64) {
	l.mu.Lock()
	l.rate *= factor
	if l.rate < floor {
		l.rate = floor
	}
	l.mu.Unlock()
}

// recover 加性恢复本地速率，上限为 cap。
func (l *localLimiter) recover(step, cap float64) {
	l.mu.Lock()
	l.rate += cap * step
	if l.rate > cap {
		l.rate = cap
	}
	l.mu.Unlock()
}

// view 返回当前速率、令牌数与在途数的一致快照。
func (l *localLimiter) view() (rate, tokens float64, inflight int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.rate, l.tokens, l.inflight
}

// Acquire 阻塞直到拿到本地配额或 ctx 结束。
func (l *localLimiter) Acquire(ctx context.Context) error {
	for {
		if ok, hint := l.tryAcquire(time.Now()); ok {
			return nil
		} else {
			select {
			case <-time.After(hint):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

func minDur(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

func maxDur(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}
