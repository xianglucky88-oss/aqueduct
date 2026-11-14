package collector

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/internal-platform/aqueduct/internal/config"
	"github.com/internal-platform/aqueduct/internal/model"
)

// fakeRunner 记录每个站点被调度的次数，并可让指定站点阻塞，
// 用来证明一个站点的等待不会阻塞其他站点。
type fakeRunner struct {
	mu    sync.Mutex
	calls map[string]int
	block map[string]chan struct{}
}

func newFakeRunner() *fakeRunner {
	return &fakeRunner{
		calls: map[string]int{},
		block: map[string]chan struct{}{},
	}
}

func (f *fakeRunner) RunSite(ctx context.Context, site config.Site) (model.Run, error) {
	f.mu.Lock()
	f.calls[site.ID]++
	block := f.block[site.ID]
	f.mu.Unlock()
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return model.Run{SiteID: site.ID}, ctx.Err()
		}
	}
	return model.Run{SiteID: site.ID, Status: "succeeded"}, nil
}

func (f *fakeRunner) count(id string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[id]
}

func TestRunnerSchedulesSitesIndependently(t *testing.T) {
	blockSlow := make(chan struct{})
	fr := newFakeRunner()
	fr.block["slow"] = blockSlow

	cfg := &config.Config{Sites: []config.Site{
		{ID: "slow", Enabled: true, Interval: 30 * time.Millisecond},
		{ID: "fast", Enabled: true, Interval: 30 * time.Millisecond},
	}}
	runner := NewRunner(cfg, fr)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_ = runner.Start(ctx)

	if got := fr.count("fast"); got < 3 {
		t.Fatalf("fast site scheduled only %d times while slow site is blocked", got)
	}
	if got := fr.count("slow"); got != 1 {
		t.Fatalf("blocked slow site should run once, got %d", got)
	}
}

func TestRunnerNoEnabledSitesBlocksUntilCancel(t *testing.T) {
	fr := newFakeRunner()
	runner := NewRunner(&config.Config{}, fr)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := runner.Start(ctx); err != context.DeadlineExceeded && err != context.Canceled {
		t.Fatalf("expected context error, got %v", err)
	}
}
