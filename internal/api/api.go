package api

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/internal-platform/aqueduct/internal/budget"
	"github.com/internal-platform/aqueduct/internal/collector"
	"github.com/internal-platform/aqueduct/internal/config"
	"github.com/internal-platform/aqueduct/internal/metrics"
	"github.com/internal-platform/aqueduct/internal/model"
	"github.com/internal-platform/aqueduct/internal/store"
)

// Server 是只读管理接口。
//
//	GET  /healthz              健康检查
//	GET  /stats?with_requests=1 落库统计、重复率、可选请求计数
//	GET  /runs?limit=20        最近运行记录（含预算分类计数）
//	GET  /budgets              各站点预算治理状态（模式/有效速率/在途/收缩）
//	POST /collect/{site}       手动触发一次采集（同步执行）
type Server struct {
	cfg     *config.Config
	st      *store.Store
	col     *collector.Collector
	reqs    *metrics.Requests
	budgets *budget.Registry
	http    *http.Server
}

// New 构造 API 服务。
func New(cfg *config.Config, st *store.Store, col *collector.Collector,
	reqs *metrics.Requests, budgets *budget.Registry) *Server {
	s := &Server{cfg: cfg, st: st, col: col, reqs: reqs, budgets: budgets}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/stats", s.handleStats)
	mux.HandleFunc("/runs", s.handleRuns)
	mux.HandleFunc("/budgets", s.handleBudgets)
	mux.HandleFunc("/collect/", s.handleCollect)
	s.http = &http.Server{
		Addr:              ":8081",
		Handler:           logMiddleware(mux),
		ReadHeaderTimeout: 5 * time.Second,
	}
	return s
}

// ListenAndServe 启动服务。
func (s *Server) ListenAndServe() error {
	log.Printf("api listening on %s", s.http.Addr)
	return s.http.ListenAndServe()
}

// Shutdown 优雅关闭。
func (s *Server) Shutdown(ctx context.Context) error { return s.http.Shutdown(ctx) }

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

type siteOut struct {
	model.SiteStats
	DuplicateRatio float64 `json:"duplicate_ratio"`
	Requests       int64   `json:"requests_last_120m,omitempty"`
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	st, err := s.st.Stats(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	withReqs := r.URL.Query().Get("with_requests") == "1"
	out := struct {
		TotalRows int                 `json:"total_rows"`
		TotalRuns int                 `json:"total_runs"`
		LastRunAt string              `json:"last_run_at"`
		Sites     []siteOut           `json:"sites"`
		Cursors   map[string]string   `json:"cursors"`
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
	rows, err := s.st.DB().QueryContext(r.Context(), `
		SELECT run_id, site_id, started_at, finished_at, status, items_seen, items_wrote,
		       http_attempts, quota_waits, rate_limited, fallback_uses
		FROM runs ORDER BY started_at DESC LIMIT $1`, limit)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	defer rows.Close()
	out := []model.Run{}
	for rows.Next() {
		var run model.Run
		var fin *time.Time
		if err := rows.Scan(&run.ID, &run.SiteID, &run.StartedAt, &fin, &run.Status,
			&run.ItemsSeen, &run.ItemsWrote, &run.HTTPAttempts, &run.QuotaWaits,
			&run.RateLimited, &run.FallbackUses); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		run.FinishedAt = fin
		out = append(out, run)
	}
	writeJSON(w, http.StatusOK, map[string]any{"runs": out})
}

// handleBudgets 暴露各站点预算治理的实时状态。
//
// mode 区分 redis（分布式生效）/ fallback（后端失联、本地折减）/ local（单机）；
// effective_rate 低于 configured_rate 即表示正处于限流收缩或恢复过程中。
func (s *Server) handleBudgets(w http.ResponseWriter, r *http.Request) {
	if s.budgets == nil {
		writeJSON(w, http.StatusServiceUnavailable,
			map[string]string{"error": "budget registry not configured"})
		return
	}
	snaps := s.budgets.Snapshots(r.Context())
	if snaps == nil {
		snaps = []budget.Snapshot{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"budgets": snaps})
}

func (s *Server) handleCollect(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST only"})
		return
	}
	siteID := r.URL.Path[len("/collect/"):]
	site, ok := s.cfg.SiteByID(siteID)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown site: " + siteID})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 120*time.Second)
	defer cancel()
	run, err := s.col.RunSite(ctx, site)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"run": run, "error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, run)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func round4(f float64) float64 {
	return float64(int64(f*10000+0.5)) / 10000
}

func logMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("%s %s %s", r.Method, r.URL.Path, time.Since(start).Round(time.Millisecond))
	})
}
