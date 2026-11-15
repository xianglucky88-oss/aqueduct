package api

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/internal-platform/aqueduct/internal/collector"
	"github.com/internal-platform/aqueduct/internal/config"
	"github.com/internal-platform/aqueduct/internal/health"
	"github.com/internal-platform/aqueduct/internal/metrics"
	"github.com/internal-platform/aqueduct/internal/model"
	"github.com/internal-platform/aqueduct/internal/store"
)

// Server 是管理接口与探针服务。
//
//	GET  /startupz              初始化是否完成（失败时存活探针尚未生效）
//	GET  /healthz               兼容别名，语义等同 readyz
//	GET  /livez                 进程存活，不检查依赖
//	GET  /readyz                是否具备接收工作负载的条件
//	GET  /stats?with_requests=1 落库统计、重复率、可选请求计数
//	GET  /runs?limit=20         最近运行记录
//	GET  /sites                 各站点调度状态与租约归属
//	POST /collect/{site}        手动触发一次采集（同步执行，需持有租约）
//	POST /drain                 进入终止流程：只拒绝新工作，不退出进程
//	GET  /version               构建信息
type Server struct {
	cfg   *config.Config
	st    *store.Store
	col   *collector.Collector
	reqs  *metrics.Requests
	state *health.State
	http  *http.Server

	sched interface{ StopAccepting() }
}

// New 构造 API 服务。
func New(cfg *config.Config, st *store.Store, col *collector.Collector, reqs *metrics.Requests, state *health.State) *Server {
	s := &Server{cfg: cfg, st: st, col: col, reqs: reqs, state: state}
	mux := http.NewServeMux()
	mux.HandleFunc("/startupz", s.handleStartup)
	mux.HandleFunc("/livez", s.handleLive)
	mux.HandleFunc("/readyz", s.handleReady)
	mux.HandleFunc("/healthz", s.handleReady) // 兼容旧调用方
	mux.HandleFunc("/stats", s.handleStats)
	mux.HandleFunc("/runs", s.handleRuns)
	mux.HandleFunc("/sites", s.handleSites)
	mux.HandleFunc("/collect/", s.handleCollect)
	mux.HandleFunc("/drain", s.handleDrain)
	mux.HandleFunc("/version", s.handleVersion)
	s.http = &http.Server{
		Addr:              cfg.APIAddr,
		Handler:           logMiddleware(mux),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		// 手动采集是同步执行的，写超时必须覆盖它的最长耗时。
		WriteTimeout: 3 * time.Minute,
		IdleTimeout:  60 * time.Second,
	}
	return s
}

// SetScheduler 注入调度器，使 /drain 能同步停止领取新工作。
func (s *Server) SetScheduler(sched interface{ StopAccepting() }) { s.sched = sched }

// ListenAndServe 启动服务。
func (s *Server) ListenAndServe() error {
	log.Printf("api listening on %s", s.http.Addr)
	err := s.http.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// Shutdown 优雅关闭。
func (s *Server) Shutdown(ctx context.Context) error { return s.http.Shutdown(ctx) }

func (s *Server) handleStartup(w http.ResponseWriter, r *http.Request) {
	rep := s.state.Startup()
	code := http.StatusOK
	if rep.Status != "ok" {
		code = http.StatusServiceUnavailable
	}
	writeJSON(w, code, rep)
}

func (s *Server) handleLive(w http.ResponseWriter, r *http.Request) {
	// 存活探针永远 200：依赖故障不是重启进程的理由。
	writeJSON(w, http.StatusOK, s.state.Live())
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	rep := s.state.Ready(r.Context())
	code := http.StatusOK
	if !health.Healthy(rep) {
		code = http.StatusServiceUnavailable
	}
	writeJSON(w, code, rep)
}

func (s *Server) handleDrain(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST only"})
		return
	}
	s.state.MarkDraining()
	s.col.BeginDrain()
	if s.sched != nil {
		s.sched.StopAccepting()
	}
	log.Printf("drain requested via API; readyz now failing, in-flight work continues")
	writeJSON(w, http.StatusOK, map[string]string{"status": "draining"})
}

func (s *Server) handleVersion(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"worker_id": s.cfg.WorkerID,
		"hostname":  hostname(),
		"version":   "1.5.0",
	})
}

type siteOut struct {
	model.SiteStats
	DuplicateRatio float64 `json:"duplicate_ratio"`
	Requests       int64   `json:"requests_last_120m,omitempty"`
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	st, err := s.st.Stats(r.Context())
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
		return
	}
	withReqs := r.URL.Query().Get("with_requests") == "1"
	out := struct {
		TotalRows int               `json:"total_rows"`
		TotalRuns int               `json:"total_runs"`
		LastRunAt string            `json:"last_run_at"`
		Sites     []siteOut         `json:"sites"`
		Cursors   map[string]string `json:"cursors"`
	}{TotalRows: st.TotalRows, TotalRuns: st.TotalRuns, LastRunAt: st.LastRunAt, Cursors: st.Cursors}
	for _, ss := range st.Sites {
		o := siteOut{SiteStats: ss, DuplicateRatio: round4(ss.DuplicateRatio())}
		if withReqs {
			o.Requests = s.reqs.Total(r.Context(), ss.SiteID)
		}
		out.Sites = append(out.Sites, o)
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleRuns(w http.ResponseWriter, r *http.Request) {
	limit := 20
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 500 {
			limit = n
		}
	}
	runs, err := s.st.RecentRuns(r.Context(), limit)
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"runs": runs})
}

func (s *Server) handleSites(w http.ResponseWriter, r *http.Request) {
	leases, err := s.st.Leases(r.Context())
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
		return
	}
	runtimes, err := s.st.SiteRuntimes(r.Context())
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"worker_id":    s.cfg.WorkerID,
		"draining":     s.state.Draining(),
		"leases":       leases,
		"site_runtime": runtimes,
	})
}

func (s *Server) handleCollect(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST only"})
		return
	}
	if s.state.Draining() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "instance is draining"})
		return
	}
	siteID := r.URL.Path[len("/collect/"):]
	site, ok := s.cfg.SiteByID(siteID)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown site: " + siteID})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Minute)
	defer cancel()

	// 手动触发同样必须竞争租约：否则会和自动调度或其他副本重复采集。
	lease, err := s.st.AcquireLease(ctx, site.ID, s.cfg.WorkerID, s.cfg.LeaseTTL, true)
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": err.Error()})
		return
	}
	if lease == nil {
		writeJSON(w, http.StatusConflict, map[string]any{
			"error": "site is held by another worker or not due; retry later",
		})
		return
	}
	res := s.col.RunSite(ctx, *lease, site)
	finishCtx, finishCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer finishCancel()
	if _, err := s.st.FinishSite(finishCtx, *lease, collector.FinishOptionsFor(site, res)); err != nil {
		log.Printf("manual collect %s: finish: %v", site.ID, err)
	}

	if res.Err != nil {
		code := http.StatusBadGateway
		if errors.Is(res.Err, store.ErrLeaseLost) {
			code = http.StatusConflict
		}
		writeJSON(w, code, map[string]any{"run": res.Run, "error": res.Err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, res.Run)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func round4(f float64) float64 {
	return float64(int64(f*10000+0.5)) / 10000
}

func hostname() string {
	h, _ := os.Hostname()
	return h
}

func logMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 探针调用频率高，降为不记录，避免刷屏。
		if r.URL.Path == "/livez" || r.URL.Path == "/readyz" || r.URL.Path == "/startupz" {
			next.ServeHTTP(w, r)
			return
		}
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("%s %s %s", r.Method, r.URL.Path, time.Since(start).Round(time.Millisecond))
	})
}
