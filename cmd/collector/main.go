package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/internal-platform/aqueduct/internal/api"
	"github.com/internal-platform/aqueduct/internal/budget"
	"github.com/internal-platform/aqueduct/internal/collector"
	"github.com/internal-platform/aqueduct/internal/config"
	"github.com/internal-platform/aqueduct/internal/metrics"
	"github.com/internal-platform/aqueduct/internal/store"
)

func main() {
	var (
		configPath = flag.String("config", "configs/sites.yaml", "path to config file")
		mode       = flag.String("mode", "all", "all | runner | api | once")
		siteID     = flag.String("site", "", "site id for -mode=once")
	)
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.LUTC)
	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	st, err := store.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	defer st.Close()

	if err := st.Migrate(ctx); err != nil {
		log.Fatalf("migrate: %v", err)
	}

	var (
		reqs    *metrics.Requests
		rdb     *redis.Client
		budgets *budget.Registry
	)
	if cfg.RedisURL != "" {
		client, err := budget.OpenRedis(ctx, cfg.RedisURL)
		if err != nil {
			// 不阻断启动：预算治理降级为实例内本地限流，请求计数同时关闭。
			log.Printf("warn: redis unavailable (%v), budget governance in local mode", err)
			rdb = nil
		} else {
			rdb = client
			defer rdb.Close()
			reqs = metrics.New(rdb)
		}
	}
	budgets, err = budget.NewRegistry(cfg, rdb)
	if err != nil {
		log.Fatalf("build budget registry: %v", err)
	}
	defer budgets.Close()
	if rdb == nil {
		log.Printf("worker=%s db ready, budget=local (no shared backend)", cfg.WorkerID)
	} else {
		log.Printf("worker=%s db ready, budget=redis (distributed)", cfg.WorkerID)
	}
	col := collector.New(cfg, st, budgets, reqs)

	switch *mode {
	case "once":
		if *siteID == "" {
			log.Fatal("-mode=once requires -site")
		}
		site, ok := cfg.SiteByID(*siteID)
		if !ok {
			log.Fatalf("unknown site %q", *siteID)
		}
		run, err := col.RunSite(ctx, site)
		if err != nil {
			log.Printf("run failed: %v", err)
			os.Exit(1)
		}
		log.Printf("ok seen=%d wrote=%d run=%s", run.ItemsSeen, run.ItemsWrote, run.ID)
	case "api":
		srv := api.New(cfg, st, col, reqs, budgets)
		if err := srv.ListenAndServe(); err != nil {
			log.Printf("api stopped: %v", err)
		}
	case "runner":
		if err := collector.NewRunner(cfg, col).Start(ctx); err != nil {
			log.Printf("runner stopped: %v", err)
		}
	case "all":
		srv := api.New(cfg, st, col, reqs, budgets)
		go func() {
			if err := srv.ListenAndServe(); err != nil {
				log.Printf("api stopped: %v", err)
			}
		}()
		if err := collector.NewRunner(cfg, col).Start(ctx); err != nil {
			log.Printf("runner stopped: %v", err)
		}
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	default:
		log.Fatalf("unknown mode %q", *mode)
	}
}
