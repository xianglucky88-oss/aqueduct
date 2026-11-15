package collector

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/internal-platform/aqueduct/internal/config"
	"github.com/internal-platform/aqueduct/internal/fetcher"
	"github.com/internal-platform/aqueduct/internal/metrics"
	"github.com/internal-platform/aqueduct/internal/model"
	"github.com/internal-platform/aqueduct/internal/store"
)

// ErrLeaseLost 表示本站点的租约已被其他副本接管，本副本必须停手。
var ErrLeaseLost = store.ErrLeaseLost

// Result 描述一轮采集的结果。
type Result struct {
	Run    model.Run
	Err    error
	Status string
}

// Collector 按站点串行采集。
//
// 一轮采集的原子单位是「一页」：每次 CommitPage 都在同一个事务里写入
// 记录、推进游标并累加计数，且校验租约围栏。因此进程在任何时刻退出或
// 被接管，都不会出现「数据已写而游标未动」或「游标前进而数据缺失」，
// 也不会重复写坏数据（item_key 稳定，UPSERT 幂等）。
type Collector struct {
	cfg   *config.Config
	store *store.Store
	reqs  *metrics.Requests

	draining  chan struct{}
	drainOnce sync.Once
}

// New 构造采集器。reqs 可为 nil。
func New(cfg *config.Config, st *store.Store, reqs *metrics.Requests) *Collector {
	return &Collector{cfg: cfg, store: st, reqs: reqs, draining: make(chan struct{})}
}

// BeginDrain 之后不再接受新的站点任务，但已在途的轮次会继续执行到
// ctx 被取消或自行结束。Kubernetes 的 preStop/就绪探针配合使用。
func (c *Collector) BeginDrain() {
	c.drainOnce.Do(func() { close(c.draining) })
}

// Draining 报告是否已进入终止流程。
func (c *Collector) Draining() bool {
	select {
	case <-c.draining:
		return true
	default:
		return false
	}
}

// RunSite 在已持有的租约下执行一个站点的一轮采集。
//
// ctx 取消时：不再发起新请求，若当前页尚未提交则放弃该页；已提交的页与
// 游标保留。调用方负责 FinishSite 与释放租约。
func (c *Collector) RunSite(ctx context.Context, lease store.Lease, site config.Site) Result {
	run := model.Run{
		ID:        uuid.NewString(),
		SiteID:    site.ID,
		WorkerID:  c.cfg.WorkerID,
		StartedAt: time.Now().UTC(),
		Status:    model.RunStatusRunning,
	}
	if err := c.store.BeginSiteRun(ctx, lease, run); err != nil {
		run.Status = model.RunStatusAbandoned
		return Result{Run: run, Err: err, Status: model.RunStatusAbandoned}
	}

	// 心跳：续租失败意味着围栏失效，立刻取消本轮的全部工作。
	runCtx, cancelRun := context.WithCancelCause(ctx)

	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		c.heartbeat(runCtx, lease, cancelRun)
	}()

	res := c.collectPages(runCtx, lease, site, run)
	cancelRun(nil)
	<-heartbeatDone

	if cause := context.Cause(runCtx); cause != nil && !errors.Is(cause, context.Canceled) {
		res.Err = cause
		res.Status = model.RunStatusAbandoned
	}
	finishCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	lastErr := ""
	if res.Err != nil {
		lastErr = res.Err.Error()
	}
	if err := c.store.FinishRun(finishCtx, run.ID, res.Status, lastErr); err != nil {
		log.Printf("site=%s run=%s finish: %v", site.ID, run.ID, err)
	}
	// 返回给调用方（API/CLI）的结果必须反映最终状态与计数，
	// 而不是创建时的 running 快照。计数已在 CommitPage 内逐页累加，
	// 这里从库内读回，避免调用方拿到过期数字。
	res.Run.Status = res.Status
	now := time.Now().UTC()
	res.Run.FinishedAt = &now
	var seen, wrote int
	if err := c.store.RunCounters(finishCtx, run.ID, &seen, &wrote); err == nil {
		res.Run.ItemsSeen, res.Run.ItemsWrote = seen, wrote
	}
	return res
}

func (c *Collector) heartbeat(ctx context.Context, lease store.Lease, cancelRun context.CancelCauseFunc) {
	ticker := time.NewTicker(c.cfg.LeaseRenewInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			renewCtx, cancel := context.WithTimeout(ctx, c.cfg.LeaseRenewInterval)
			ok, err := c.store.RenewLease(renewCtx, lease, c.cfg.LeaseTTL)
			cancel()
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("site=%s lease renew error: %v", lease.SiteID, err)
				continue
			}
			if !ok {
				log.Printf("site=%s lease fence=%d lost, abandoning run", lease.SiteID, lease.Fence)
				cancelRun(ErrLeaseLost)
				return
			}
		}
	}
}

func (c *Collector) collectPages(ctx context.Context, lease store.Lease, site config.Site, run model.Run) Result {
	cur, err := c.store.Cursor(ctx, site.ID)
	if err != nil {
		return Result{Run: run, Err: err, Status: model.RunStatusFailed}
	}

	client := fetcher.New(site)
	lastCommitted := cur
	// 每轮最多重置一次游标：上游可能因重建索引而拒绝旧游标，
	// 但若是配置错误，重置也不会成功，不能无限循环重采。
	resetUsed := false

	for page := 0; page < c.cfg.MaxPages; page++ {
		if err := ctx.Err(); err != nil {
			// 已提交的页是检查点，下一轮从这里继续；未提交的当前页丢弃。
			log.Printf("site=%s run=%s interrupted at cursor=%q (%v)", site.ID, run.ID, lastCommitted, err)
			return Result{Run: run, Err: err, Status: model.RunStatusAbandoned}
		}

		c.reqs.Incr(ctx, site.ID)
		p, err := client.Page(ctx, cur)
		if err != nil {
			if errors.Is(err, fetcher.ErrCursorReset) && cur != "" && !resetUsed {
				resetUsed = true
				log.Printf("site=%s cursor rejected upstream; resetting to head and retrying once", site.ID)
				if resetErr := c.store.ResetCursor(ctx, lease); resetErr != nil {
					return Result{Run: run, Err: resetErr, Status: model.RunStatusAbandoned}
				}
				cur = ""
				lastCommitted = ""
				continue
			}
			status := model.RunStatusFailed
			if ctx.Err() != nil {
				status = model.RunStatusAbandoned
			}
			return Result{Run: run, Err: err, Status: status}
		}

		next := p.NextCursor
		switch {
		case site.Mode == "single":
			// 单页模式没有游标；游标始终为空表示「本轮完成」。
			next = ""
		case p.HasMore && next == "":
			// 上游声明还有数据却没给游标：这属于上游契约违约。
			// 不能当作末页处理，否则剩余数据会被静默跳过（遗漏）。
			return Result{
				Run:    run,
				Err:    errors.New("upstream returned has_more=true without next_cursor"),
				Status: model.RunStatusFailed,
			}
		case !p.HasMore:
			// 末页：游标置空表示完整一轮结束。
			next = ""
		}

		if _, err := c.store.CommitPage(ctx, lease, run.ID, p.Items, next); err != nil {
			status := model.RunStatusFailed
			if errors.Is(err, store.ErrLeaseLost) || ctx.Err() != nil {
				status = model.RunStatusAbandoned
			}
			return Result{Run: run, Err: err, Status: status}
		}
		lastCommitted = next
		if next == "" {
			return Result{Run: run, Status: model.RunStatusSucceeded}
		}
		cur = next
	}

	return Result{
		Run:    run,
		Err:    errors.New("max pages reached; checkpoint kept"),
		Status: model.RunStatusFailed,
	}
}

// Scheduler 是本副本的站点调度器。
//
// 每个副本独立轮询到期站点并通过租约竞争取得执行权。没有主节点：
// 站点归属完全由数据库中的租约决定，因此滚动升级、驱逐、宕机都表现为
// 「租约到期后被其他副本接管」。
type Scheduler struct {
	cfg       *config.Config
	store     *store.Store
	collector *Collector

	wake chan struct{}

	mu       sync.Mutex
	inflight map[string]context.CancelFunc
	wg       sync.WaitGroup
}

// NewScheduler 构造调度器。
func NewScheduler(cfg *config.Config, st *store.Store, c *Collector) *Scheduler {
	return &Scheduler{
		cfg: cfg, store: st, collector: c,
		wake:     make(chan struct{}, 1),
		inflight: make(map[string]context.CancelFunc),
	}
}

// Start 阻塞运行直到 ctx 结束。
//
// 停止语义：
//  1. ctx 取消（SIGTERM）时立即停止领取新站点；
//  2. 已在途的站点在 ShutdownGrace 内继续，尽量自行跑完当前轮
//     （每页都已提交，因此即使中途停下也不丢数据）；
//  3. 宽限期到后取消剩余执行，写 abandoned 并释放租约，
//     让其他副本立刻接管。
func (s *Scheduler) Start(ctx context.Context) error {
	sites := s.cfg.EnabledSites()
	if len(sites) == 0 {
		log.Println("no enabled sites; scheduler idle")
		<-ctx.Done()
		return ctx.Err()
	}
	byID := make(map[string]config.Site, len(sites))
	ids := make([]string, 0, len(sites))
	for _, site := range sites {
		byID[site.ID] = site
		ids = append(ids, site.ID)
	}
	if err := s.store.EnsureSites(ctx, ids); err != nil {
		return err
	}
	log.Printf("scheduler started: worker=%s sites=%d concurrency=%d lease_ttl=%s",
		s.cfg.WorkerID, len(ids), s.cfg.MaxConcurrentSites, s.cfg.LeaseTTL)

	slots := make(chan struct{}, s.cfg.MaxConcurrentSites)
	ticker := time.NewTicker(s.cfg.SchedulerPoll)
	defer ticker.Stop()

loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case <-ticker.C:
		case <-s.wake:
		}
		if s.collector.Draining() {
			continue
		}
		s.poll(ctx, ids, byID, slots)
	}

	// 不再领取新站点。给在途站点一个收尾窗口，超时后强制取消。
	// 预算控制：在途收尾占用 ShutdownGrace 的 80%，剩余时间留给
	// 强制取消后的状态落库与租约释放，整体不会超过 terminationGracePeriod。
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	grace := s.cfg.ShutdownGrace
	settleBudget := grace * 4 / 5
	select {
	case <-done:
		if ctx.Err() != nil {
			log.Printf("scheduler: all in-flight sites finished within grace")
		}
	case <-time.After(settleBudget):
		log.Printf("scheduler: shutdown grace %s exceeded, abandoning in-flight sites", settleBudget)
		s.cancelAll()
		// 取消后仍给收尾（写状态、释放租约）剩余预算。
		select {
		case <-done:
		case <-time.After(grace - settleBudget):
			log.Printf("scheduler: in-flight sites did not settle after cancel")
		}
	}
	return ctx.Err()
}

// StopAccepting 由 main 在 SIGTERM 时调用，通知调度器不再领取新站点。
func (s *Scheduler) StopAccepting() {
	s.collector.BeginDrain()
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *Scheduler) cancelAll() {
	s.mu.Lock()
	cancels := make([]context.CancelFunc, 0, len(s.inflight))
	for _, cancel := range s.inflight {
		cancels = append(cancels, cancel)
	}
	s.mu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
}

func (s *Scheduler) poll(ctx context.Context, ids []string, byID map[string]config.Site, slots chan struct{}) {
	due, err := s.store.DueSites(ctx, ids)
	if err != nil {
		if ctx.Err() == nil {
			log.Printf("scheduler: list due sites: %v", err)
		}
		return
	}
	for _, id := range due {
		if ctx.Err() != nil || s.collector.Draining() {
			return
		}
		select {
		case slots <- struct{}{}:
		default:
			// 本副本并发已满，留给他副本或下一轮。
			return
		}
		site := byID[id]
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer func() { <-slots }()
			s.runSite(site)
		}()
	}
}

func (s *Scheduler) runSite(site config.Site) {
	// 单站点执行上下文独立于调度循环：父 ctx 取消不代表站点立刻停下，
	// 只有在宽限期结束时由 cancelAll 统一取消。
	ctx, cancel := context.WithCancel(context.Background())
	s.mu.Lock()
	s.inflight[site.ID] = cancel
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.inflight, site.ID)
		s.mu.Unlock()
		cancel()
	}()

	lease, err := s.store.AcquireLease(ctx, site.ID, s.cfg.WorkerID, s.cfg.LeaseTTL, false)
	if err != nil {
		if ctx.Err() == nil {
			log.Printf("site=%s acquire lease: %v", site.ID, err)
		}
		return
	}
	if lease == nil {
		return // 未到期或他人持有
	}

	res := s.collector.RunSite(ctx, *lease, site)
	s.finish(lease, site, res)
}

// FinishOptionsFor 根据本轮结果推导收尾参数。
//
// 失败分两类：可重试错误（429/5xx/网络）从较短退避开始；不可重试错误
// （配置类 4xx、游标反复失效）直接使用最长退避，避免对必然失败的请求
// 做密集重试，同时仍保持周期性探测以便配置修复后自动恢复。
func FinishOptionsFor(site config.Site, res Result) store.FinishOptions {
	opts := store.FinishOptions{
		Status:      res.Status,
		Interval:    site.Interval,
		BackoffBase: backoffBase(site),
		BackoffMax:  backoffMax(site),
	}
	if res.Err != nil {
		opts.LastError = res.Err.Error()
		if !fetcher.Retryable(res.Err) {
			opts.BackoffBase = opts.BackoffMax
		}
	}
	if ra, ok := fetcher.RetryAfter(res.Err); ok {
		opts.RetryAfter = ra
	}
	return opts
}

func (s *Scheduler) finish(lease *store.Lease, site config.Site, res Result) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	delay, err := s.store.FinishSite(ctx, *lease, FinishOptionsFor(site, res))
	if errors.Is(err, store.ErrLeaseLost) {
		log.Printf("site=%s run=%s fence=%d lost before finish; new owner will supersede",
			site.ID, res.Run.ID, lease.Fence)
		return
	}
	if err != nil {
		log.Printf("site=%s run=%s finish site: %v", site.ID, res.Run.ID, err)
		return
	}
	if res.Err != nil {
		log.Printf("site=%s run=%s status=%s retry_in=%s err=%v",
			site.ID, res.Run.ID, res.Status, delay.Round(time.Millisecond), res.Err)
		return
	}
	log.Printf("site=%s run=%s status=%s next_in=%s",
		site.ID, res.Run.ID, res.Status, delay.Round(time.Millisecond))
}

// backoffBase 从站点周期推导退避起点：周期本身就是一个合理的重试间隔，
// 但至少 5 秒，避免高频站点在故障时形成重试风暴。
func backoffBase(site config.Site) time.Duration {
	base := site.Interval / 4
	if base < 5*time.Second {
		base = 5 * time.Second
	}
	return base
}

func backoffMax(site config.Site) time.Duration {
	max := site.Interval * 4
	if max < 2*time.Minute {
		max = 2 * time.Minute
	}
	return max
}
