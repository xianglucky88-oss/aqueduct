package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/internal-platform/aqueduct/internal/api"
	"github.com/internal-platform/aqueduct/internal/collector"
	"github.com/internal-platform/aqueduct/internal/config"
	"github.com/internal-platform/aqueduct/internal/health"
	"github.com/internal-platform/aqueduct/internal/metrics"
	"github.com/internal-platform/aqueduct/internal/store"
)

// version 由构建时 -ldflags 注入。
var version = "dev"

func main() {
	var (
		configPath = flag.String("config", "configs/sites.yaml", "path to config file")
		mode       = flag.String("mode", "all", "all | runner | api | once")
		siteID     = flag.String("site", "", "site id for -mode=once")
		showVer    = flag.Bool("version", false, "print version and exit")
	)
	flag.Parse()
	if *showVer {
		fmt.Printf("aqueduct %s\n", version)
		return
	}

	log.SetFlags(log.LstdFlags | log.LUTC)
	cfg, err := config.Load(*configPath)
	if err != nil {
		// 配置错误不可恢复：快速失败让部署立即暴露问题，
		// 而不是让 Pod 反复重启却始终不 Ready。
		log.Fatalf("load config: %v", err)
	}
	log.Printf("worker=%s mode=%s api=%s lease_ttl=%s grace=%s",
		cfg.WorkerID, *mode, cfg.APIAddr, cfg.LeaseTTL, cfg.ShutdownGrace)

	// 两阶段终止：
	//   阶段一：收到 SIGTERM，立即把本实例标记为 draining（readyz 失败、
	//           停止领取新站点），但进程继续服务在途工作。
	//   阶段二：在 ShutdownGrace 内完成收尾，或在宽限期后强制退出。
	// 收到第一个信号后立即 signal.Stop：这样第二次 SIGTERM 恢复默认行为，
	// 可以直接强杀卡住的进程（Kubernetes 宽限期结束时正是这样做的）。
	sigCtx, stopSignals := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stopSignals()

	bootCtx, cancelBoot := context.WithTimeout(sigCtx, 60*time.Second)
	defer cancelBoot()

	st, err := store.Open(bootCtx, cfg.DatabaseURL, cfg.DBMaxOpenConns, cfg.DBMaxIdleConns)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	defer st.Close()

	if err := st.Migrate(bootCtx); err != nil {
		log.Fatalf("migrate: %v", err)
	}
	cancelBoot()

	var reqs *metrics.Requests
	if cfg.RedisURL != "" {
		reqs, err = metrics.Open(sigCtx, cfg.RedisURL)
		if err != nil {
			// 不阻断启动：Redis 只用于计数，缺了不影响采集。
			log.Printf("warn: redis unavailable (%v), request counters disabled", err)
			reqs = nil
		} else {
			defer reqs.Close()
		}
	}

	log.Printf("worker=%s db ready", cfg.WorkerID)
	col := collector.New(cfg, st, reqs)
	// 显式区分 nil：避免把 typed-nil 装进接口后误判「已配置」。
	var cacheDep health.Dep
	if reqs != nil {
		cacheDep = reqs
	}
	state := health.New(st, cacheDep, cfg.ReadinessCacheTTL, 2*time.Second)

	switch *mode {
	case "once":
		runOnce(sigCtx, cfg, st, col, *siteID)
	case "api":
		srv := api.New(cfg, st, col, reqs, state)
		state.MarkStarted()
		runUntilSignal(sigCtx, stopSignals, cfg, state, srv, nil)
	case "runner":
		sched := collector.NewScheduler(cfg, st, col)
		state.MarkStarted()
		runUntilSignal(sigCtx, stopSignals, cfg, state, nil, sched)
	case "all":
		srv := api.New(cfg, st, col, reqs, state)
		sched := collector.NewScheduler(cfg, st, col)
		srv.SetScheduler(sched)
		state.MarkStarted()
		runUntilSignal(sigCtx, stopSignals, cfg, state, srv, sched)
	default:
		log.Fatalf("unknown mode %q", *mode)
	}
}

func runOnce(ctx context.Context, cfg *config.Config, st *store.Store, col *collector.Collector, id string) {
	if id == "" {
		log.Fatal("-mode=once requires -site")
	}
	site, ok := cfg.SiteByID(id)
	if !ok {
		log.Fatalf("unknown site %q", id)
	}
	lease, err := st.AcquireLease(ctx, site.ID, cfg.WorkerID, cfg.LeaseTTL, true)
	if err != nil {
		log.Fatalf("acquire lease: %v", err)
	}
	if lease == nil {
		log.Fatalf("site %q is held by another worker", site.ID)
	}
	res := col.RunSite(ctx, *lease, site)
	finishCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := st.FinishSite(finishCtx, *lease, collector.FinishOptionsFor(site, res)); err != nil {
		log.Printf("finish site: %v", err)
	}
	if res.Err != nil {
		log.Printf("run failed: %v", res.Err)
		os.Exit(1)
	}
	log.Printf("ok seen=%d wrote=%d run=%s status=%s", res.Run.ItemsSeen, res.Run.ItemsWrote, res.Run.ID, res.Status)
}

// runUntilSignal 编排 API 与调度器的启动和两阶段停止。
func runUntilSignal(sigCtx context.Context, stopSignals func(), cfg *config.Config, state *health.State, srv *api.Server, sched *collector.Scheduler) {
	apiDone := make(chan struct{})
	if srv != nil {
		go func() {
			defer close(apiDone)
			if err := srv.ListenAndServe(); err != nil {
				log.Printf("api stopped: %v", err)
			}
		}()
	}

	schedErr := make(chan error, 1)
	if sched != nil {
		go func() { schedErr <- sched.Start(sigCtx) }()
	}

	<-sigCtx.Done()
	// 恢复 SIGTERM 的默认处置：第二次信号会直接终止进程，
	// 由 Kubernetes 在宽限期结束时兜底。
	stopSignals()
	// 阶段一：公开停止接收工作负载。readyz 先失败，Kubernetes 会先把
	// 本 Pod 从 Endpoints 摘除，再等待 terminationGracePeriod。
	log.Printf("signal received: draining (readyz -> 503, no new sites)")
	state.MarkDraining()
	if sched != nil {
		sched.StopAccepting()
	}

	// 阶段二：等待调度器把在途站点收尾。调度器自身以 ShutdownGrace 为上限；
	// 这里再留 3s 余量，整体应小于 terminationGracePeriodSeconds。
	// 超时后不再等待该协程，直接走关闭流程——此时取消与租约释放已由
	// 调度器内部完成，进程退出后其他副本会在租约到期时接管。
	if sched != nil {
		select {
		case err := <-schedErr:
			logSchedulerStop(err)
		case <-time.After(cfg.ShutdownGrace + 3*time.Second):
			log.Printf("scheduler did not stop in time; exiting anyway")
		}
	}

	// API 最后关闭，确保调度器收尾期间探针仍可观察状态。
	if srv != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Printf("api shutdown: %v", err)
		}
		<-apiDone
	}
	log.Printf("shutdown complete")
}

func logSchedulerStop(err error) {
	if err == nil || errors.Is(err, context.Canceled) {
		log.Printf("scheduler stopped cleanly")
		return
	}
	log.Printf("scheduler stopped: %v", err)
}
