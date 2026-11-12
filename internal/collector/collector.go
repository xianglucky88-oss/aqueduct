package collector

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/google/uuid"

	"github.com/internal-platform/aqueduct/internal/config"
	"github.com/internal-platform/aqueduct/internal/fetcher"
	"github.com/internal-platform/aqueduct/internal/metrics"
	"github.com/internal-platform/aqueduct/internal/model"
	"github.com/internal-platform/aqueduct/internal/store"
)

// Collector 按站点串行采集。
type Collector struct {
	cfg   *config.Config
	store *store.Store
	reqs  *metrics.Requests
}

// New 构造采集器。reqs 可为 nil。
func New(cfg *config.Config, st *store.Store, reqs *metrics.Requests) *Collector {
	return &Collector{cfg: cfg, store: st, reqs: reqs}
}

// RunSite 采集单个站点的一轮任务。
//
// 当前实现的行为：
//  1. 游标只在整轮成功后一次性落库。任何一页失败，前面已写入的数据保留，
//     游标停留在本轮起点，下一轮从该处重新抓取。
//  2. 落库唯一键由 Identity() 计算，哈希里含价格与库存，
//     上游内容改变后会生成新的 ItemKey。
//  3. 租约表已建好，但采集流程未申请租约。
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

	fail := func(err error) (model.Run, error) {
		run.Status = "failed"
		if e := c.store.FinishRun(ctx, run.ID, "failed", run.ItemsSeen, run.ItemsWrote); e != nil {
			log.Printf("finish run %s: %v", run.ID, e)
		}
		return run, err
	}

	start, err := c.store.Cursor(ctx, site.ID)
	if err != nil {
		return fail(err)
	}

	client := fetcher.New(site)
	cur := start
	pending := start // 直到整轮成功才落库

	for page := 0; page < 200; page++ {
		c.reqs.Incr(ctx, site.ID)

		p, err := client.Page(ctx, cur)
		if err != nil {
			return fail(err)
		}
		run.ItemsSeen += len(p.Items)
		n, err := c.store.Upsert(ctx, p.Items)
		if err != nil {
			return fail(err)
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
		return fail(err)
	}

	run.Status = "succeeded"
	run.FinishedAt = ptr(time.Now().UTC())
	if err := c.store.FinishRun(ctx, run.ID, run.Status, run.ItemsSeen, run.ItemsWrote); err != nil {
		return run, err
	}
	return run, nil
}

func ptr[T any](v T) *T { return &v }

// Runner 周期性执行所有启用站点。
type Runner struct {
	cfg       *config.Config
	collector *Collector
}

// NewRunner 构造周期执行器。
func NewRunner(cfg *config.Config, c *Collector) *Runner {
	return &Runner{cfg: cfg, collector: c}
}

// Start 阻塞运行，直至 ctx 结束。
//
// 所有站点共用一个 ticker，间隔取最短者；每个 tick 内串行遍历站点。
// 单机假设：不协调多实例。
func (r *Runner) Start(ctx context.Context) error {
	sites := r.cfg.EnabledSites()
	if len(sites) == 0 {
		log.Println("no enabled sites; runner idle")
		<-ctx.Done()
		return ctx.Err()
	}
	interval := sites[0].Interval
	for _, s := range sites {
		if s.Interval < interval {
			interval = s.Interval
		}
	}
	log.Printf("runner started: %d sites, tick=%s", len(sites), interval)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	r.tick(ctx, sites)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			r.tick(ctx, sites)
		}
	}
}

func (r *Runner) tick(ctx context.Context, sites []config.Site) {
	for _, s := range sites {
		run, err := r.collector.RunSite(ctx, s)
		if err != nil {
			if errors.Is(err, fetcher.ErrRateLimited) {
				// 撞 429 时跳过本轮。
				log.Printf("site=%s run=%s rate limited, no backoff", s.ID, run.ID)
				continue
			}
			log.Printf("site=%s run=%s failed: %v", s.ID, run.ID, err)
			continue
		}
		log.Printf("site=%s run=%s ok seen=%d wrote=%d", s.ID, run.ID, run.ItemsSeen, run.ItemsWrote)
	}
}
