package api

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/internal-platform/aqueduct/internal/collector"
	"github.com/internal-platform/aqueduct/internal/config"
	"github.com/internal-platform/aqueduct/internal/metrics"
	"github.com/internal-platform/aqueduct/internal/model"
	"github.com/internal-platform/aqueduct/internal/store"
)

// Server 是只读管理接口。
//
//	GET  /healthz                                  健康检查
//	GET  /stats?with_requests=1                    落库统计、重复率、历史版本数、迁移进度
//	GET  /runs?limit=20                            最近运行记录
//	POST /collect/{site}                           手动触发一次采集（同步执行）
//	GET  /items/{site}/{source_id}                 当前状态
//	GET  /items/{site}/{source_id}/history?limit=  版本链，revision 倒序
//	GET  /items/{site}/{source_id}/as-of?at=       指定业务时点的可见版本
//	GET  /snapshot/{site}?at=&limit=&offset=       站点在指定时点的全量可见状态
type Server struct {
	cfg  *config.Config
	st   *store.Store
	col  *collector.Collector
	reqs *metrics.Requests
	http *http.Server
}

// New 构造 API 服务。
func New(cfg *config.Config, st *store.Store, col *collector.Collector, reqs *metrics.Requests) *Server {
	s := &Server{cfg: cfg, st: st, col: col, reqs: reqs}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/stats", s.handleStats)
	mux.HandleFunc("/runs", s.handleRuns)
	mux.HandleFunc("/collect/", s.handleCollect)
	mux.HandleFunc("/items/", s.handleItems)
	mux.HandleFunc("/snapshot/", s.handleSnapshot)
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
		Migration model.MigrationInfo `json:"migration"`
	}{TotalRows: st.TotalRows, TotalRuns: st.TotalRuns, LastRunAt: st.LastRunAt,
		Cursors: st.Cursors, Migration: st.Migration}
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
		SELECT run_id, site_id, started_at, finished_at, status, items_seen,
		       items_wrote, items_stale, items_unchanged
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
			&run.ItemsSeen, &run.ItemsWrote, &run.ItemsStale, &run.ItemsUnchanged); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		run.FinishedAt = fin
		out = append(out, run)
	}
	writeJSON(w, http.StatusOK, map[string]any{"runs": out})
}

// handleItems 处理当前状态、版本链与时点查询。
//
//	/items/{site}/{source_id}
//	/items/{site}/{source_id}/history
//	/items/{site}/{source_id}/as-of?at=RFC3339
func (s *Server) handleItems(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 3 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "want /items/{site}/{source_id}"})
		return
	}
	siteID, sourceID := parts[1], parts[2]
	action := ""
	if len(parts) >= 4 {
		action = parts[3]
	}

	switch action {
	case "":
		v, err := s.st.Current(r.Context(), siteID, sourceID)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		if v == nil {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown item"})
			return
		}
		writeJSON(w, http.StatusOK, v)
	case "history":
		limit := intQuery(r, "limit", 100, 1, 1000)
		vs, err := s.st.History(r.Context(), siteID, sourceID, limit)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"site_id": siteID, "source_id": sourceID, "versions": vs})
	case "as-of":
		at, err := parseAt(r.URL.Query().Get("at"))
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		v, err := s.st.AsOf(r.Context(), siteID, sourceID, at)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		if v == nil {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "no version visible at that time"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"at": at, "version": v})
	default:
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown action: " + action})
	}
}

// handleSnapshot 返回站点在某个业务时点的全量可见状态。
func (s *Server) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	siteID := strings.TrimPrefix(r.URL.Path, "/snapshot/")
	if siteID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "want /snapshot/{site}"})
		return
	}
	at, err := parseAt(r.URL.Query().Get("at"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	limit := intQuery(r, "limit", 500, 1, 5000)
	offset := intQuery(r, "offset", 0, 0, 1<<30)
	vs, err := s.st.SnapshotAsOf(r.Context(), siteID, at, limit, offset)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"site_id": siteID, "at": at, "items": vs, "limit": limit, "offset": offset,
	})
}

func parseAt(raw string) (time.Time, error) {
	if raw == "" {
		return time.Time{}, errBadAt("missing at (RFC3339)")
	}
	at, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}, errBadAt("bad at, want RFC3339: " + raw)
	}
	return at.UTC(), nil
}

type errBadAt string

func (e errBadAt) Error() string { return string(e) }

func intQuery(r *http.Request, name string, def, lo, hi int) int {
	v := r.URL.Query().Get(name)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < lo || n > hi {
		return def
	}
	return n
}

// handleCollect 处理 POST /collect/{site}。
//
// 默认按已保存游标增量采集。加 ?full=1 时忽略游标、从第一页重扫，
// 用于运维复核（不是建立历史的必要条件）。
func (s *Server) handleCollect(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST only"})
		return
	}
	siteID := strings.TrimPrefix(r.URL.Path, "/collect/")
	site, ok := s.cfg.SiteByID(siteID)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown site: " + siteID})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 120*time.Second)
	defer cancel()
	var run model.Run
	var err error
	if r.URL.Query().Get("full") == "1" {
		run, err = s.col.RunSiteFull(ctx, site)
	} else {
		run, err = s.col.RunSite(ctx, site)
	}
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
