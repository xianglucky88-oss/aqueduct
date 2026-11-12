package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/internal-platform/aqueduct/internal/api"
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

	var reqs *metrics.Requests
	if cfg.RedisURL != "" {
		reqs, err = metrics.Open(ctx, cfg.RedisURL)
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
		srv := api.New(cfg, st, col, reqs)
		if err := srv.ListenAndServe(); err != nil {
			log.Printf("api stopped: %v", err)
		}
	case "runner":
		if err := collector.NewRunner(cfg, col).Start(ctx); err != nil {
			log.Printf("runner stopped: %v", err)
		}
	case "all":
		srv := api.New(cfg, st, col, reqs)
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
