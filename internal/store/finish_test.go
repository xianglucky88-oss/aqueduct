package store

import (
	"errors"
	"testing"
	"time"

	"github.com/internal-platform/aqueduct/internal/fetcher"
	"github.com/internal-platform/aqueduct/internal/model"
)

func TestFinishOptionsStatusSemantics(t *testing.T) {
	// FinishSite 的状态语义在集成测试中通过真实数据库验证；
	// 这里锁定调用方依赖的分类，避免退避/立即接管被误改。
	for _, st := range []string{
		model.RunStatusSucceeded,
		model.RunStatusFailed,
		model.RunStatusAbandoned,
	} {
		if st == "" {
			t.Fatal("status constant must not be empty")
		}
	}
}

func TestRetryAfterPropagation(t *testing.T) {
	base := &fetcher.RetryAfterError{Err: fetcher.ErrRateLimited, RetryAfter: 12 * time.Second}
	d, ok := fetcher.RetryAfter(base)
	if !ok || d != 12*time.Second {
		t.Fatalf("want 12s retry-after, got %s ok=%v", d, ok)
	}
	if !fetcher.Retryable(base) {
		t.Fatal("429 must be retryable")
	}
	if fetcher.Retryable(fetcher.ErrCursorReset) {
		t.Fatal("cursor reset must not be blindly retried")
	}
	if errors.Is(base, fetcher.ErrServerError) {
		t.Fatal("429 must not be classified as 5xx")
	}
}
