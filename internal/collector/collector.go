package collector

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/google/uuid"

	"github.com/internal-platform/aqueduct/internal/budget"
	"github.com/internal-platform/aqueduct/internal/config"
	"github.com/internal-platform/aqueduct/internal/fetcher"
	"github.com/internal-platform/aqueduct/internal/metrics"
	"github.com/internal-platform/aqueduct/internal/model"
	"github.com/internal-platform/aqueduct/internal/store"
)

// attemptCounters 汇总一轮采集中各结果类别的尝试次数，
// 供运行人员区分正常配额等待、上游限流、预算失联与实际请求失败。
type attemptCounters struct {
	requests    int // 实际发出的 HTTP 尝试总数（含重试）
	quotaWaits  int // 经历过正常配额等待的尝试数
	rateLimited int // 收到 429 的尝试数
	fallback    int // 预算后端失联、走本地降级的尝试数
	retries     int // 发生重试的尝试数
}

// Collector 按站点串行采集。
type Collector struct {
	cfg     *config.Config
	store   *store.Store
	budgets *budget.Registry
	reqs    *metrics.Requests
}

// New 构造采集器。reqs 可为 nil。
func New(cfg *config.Config, st *store.Store, budgets *budget.Registry, reqs *metrics.Requests) *Collector {
	return &Collector{cfg: cfg, store: st, budgets: budgets, reqs: reqs}
}

// RunSite 采集单个站点的一轮任务。
//
// 行为：
//  1. 游标只在整轮成功后一次性落库。任何一页失败，前面已写入的数据保留，
//     游标停留在本轮起点，下一轮从该处重新抓取。
//  2. 落库唯一键由 Identity() 计算，哈希里含价格与库存，
//     上游内容改变后会生成新的 ItemKey。
//  3. 每个 HTTP 尝试都经过站点级预算治理器，重试消耗同一份配额。
func (c *Collector) RunSite(ctx context.Context, site config.Site) (model.Run, error) {
	run := model.Run{
		ID:        uuid.NewString(),
		SiteID:    site.ID,
		StartedAt: time.Now().UTC(),
		Status:    "running",
	}
	if err := c.store.StartRun(ctx, run); err != nil {
		return run, err
	}

	fail := func(err error, ac attemptCounters) (model.Run, error) {
		run.Status = "failed"
		run.HTTPAttempts = ac.requests
		run.QuotaWaits = ac.quotaWaits
		run.RateLimited = ac.rateLimited
		run.FallbackUses = ac.fallback
		if e := c.store.FinishRun(ctx, run.ID, "failed", run.ItemsSeen, run.ItemsWrote,
			ac.requests, ac.quotaWaits, ac.rateLimited, ac.fallback); e != nil {
			log.Printf("finish run %s: %v", run.ID, e)
		}
		return run, err
	}

	start, err := c.store.Cursor(ctx, site.ID)
	if err != nil {
		return fail(err, attemptCounters{})
	}

	var ac attemptCounters
	gov, _ := c.budgets.Get(site.ID)
	client := fetcher.New(site, gov, fetcher.WithObserver(func(siteID string, attempt, maxAttempts int,
		waited time.Duration, status budget.AcquireStatus, outcome fetcher.AttemptOutcome, err error) {
		c.reqs.Incr(ctx, siteID)
		ac.requests++
		if attempt > 1 {
			ac.retries++
		}
		if status == budget.StatusQuotaWait {
			ac.quotaWaits++
		}
		if status == budget.StatusFallback {
			ac.fallback++
		}
		if outcome == fetcher.OutcomeLimited {
			ac.rateLimited++
		}
		log.Printf("budget site=%s attempt=%d/%d wait=%s mode=%s outcome=%s err=%v",
			siteID, attempt, maxAttempts, waited.Round(time.Millisecond), status, outcome, err)
	}))

	cur := start
	pending := start // 直到整轮成功才落库

	for page := 0; page < 200; page++ {
		p, err := client.Page(ctx, cur)
		if err != nil {
			return fail(err, ac)
		}
		run.ItemsSeen += len(p.Items)
		n, err := c.store.Upsert(ctx, p.Items)
		if err != nil {
			return fail(err, ac)
		}
		run.ItemsWrote += n

		if !p.HasMore || p.NextCursor == "" {
			break
		}
		cur = p.NextCursor
		pending = cur
	}

	// 整轮成功后才推进游标。
	if err := c.store.SaveCursor(ctx, site.ID, pending); err != nil {
		return fail(err, ac)
	}

	run.Status = "succeeded"
	run.FinishedAt = ptr(time.Now().UTC())
	run.HTTPAttempts = ac.requests
	run.QuotaWaits = ac.quotaWaits
	run.RateLimited = ac.rateLimited
	run.FallbackUses = ac.fallback
	if err := c.store.FinishRun(ctx, run.ID, run.Status, run.ItemsSeen, run.ItemsWrote,
		ac.requests, ac.quotaWaits, ac.rateLimited, ac.fallback); err != nil {
		return run, err
	}
	return run, nil
}

func ptr[T any](v T) *T { return &v }

// siteRunner 抽象单站点执行，便于在测试中替换为不依赖数据库的假实现。
type siteRunner interface {
	RunSite(ctx context.Context, site config.Site) (model.Run, error)
}

// Runner 周期性执行所有启用站点。
type Runner struct {
	cfg       *config.Config
	collector siteRunner
}

// NewRunner 构造周期执行器。
func NewRunner(cfg *config.Config, c siteRunner) *Runner {
	return &Runner{cfg: cfg, collector: c}
}

// Start 阻塞运行，直至 ctx 结束。
//
// 每个站点独立 ticker、独立 goroutine：一个站点的预算等待或失败
// 不会阻塞其他站点的调度。
func (r *Runner) Start(ctx context.Context) error {
	sites := r.cfg.EnabledSites()
	if len(sites) == 0 {
		log.Println("no enabled sites; runner idle")
		<-ctx.Done()
		return ctx.Err()
	}
	log.Printf("runner started: %d sites, per-site tickers", len(sites))

	for _, s := range sites {
		go r.runLoop(ctx, s)
	}
	<-ctx.Done()
	return ctx.Err()
}

// runLoop 驱动单个站点的周期采集，站点之间互不阻塞。
func (r *Runner) runLoop(ctx context.Context, site config.Site) error {
	ticker := time.NewTicker(site.Interval)
	defer ticker.Stop()

	r.runOnce(ctx, site)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			r.runOnce(ctx, site)
		}
	}
}

func (r *Runner) runOnce(ctx context.Context, s config.Site) {
	run, err := r.collector.RunSite(ctx, s)
	switch {
	case errors.Is(err, fetcher.ErrRateLimited):
		log.Printf("site=%s run=%s rate limited (retries exhausted)", s.ID, run.ID)
	case err != nil:
		log.Printf("site=%s run=%s failed: %v", s.ID, run.ID, err)
	default:
		log.Printf("site=%s run=%s ok seen=%d wrote=%d", s.ID, run.ID, run.ItemsSeen, run.ItemsWrote)
	}
}
