// Package health 维护进程的启动、就绪与依赖状态。
//
// 四种状态刻意分开，对应 Kubernetes 的三类探针：
//
//	startupz  初始化是否完成（迁移、调度器、API 已就绪）。未完成时
//	          存活探针不生效，避免启动慢被误杀。
//	livez     进程是否还活着。不检查依赖：数据库短暂不可用时重启进程
//	          只会让恢复更慢，因此依赖故障绝不应导致存活探针失败。
//	readyz    是否具备接收工作负载的条件：未在终止中、数据库可达。
//	          失败只会把本 Pod 从 Service Endpoints 摘除。
//
// Redis 仅用于请求计数，属于可降级依赖：不可用时 readyz 标记 degraded
// 但仍返回 200，不影响接流量。
package health

import (
	"context"
	"sync"
	"time"
)

// Dep 是一个可检查的依赖。
type Dep interface {
	Ping(ctx context.Context) error
}

// Check 是单项检查结果。
type Check struct {
	OK       bool   `json:"ok"`
	Required bool   `json:"required"`
	Detail   string `json:"detail,omitempty"`
}

// Report 是一次探测的完整结果。
type Report struct {
	Status string           `json:"status"` // ok | degraded | unavailable | starting | draining
	Checks map[string]Check `json:"checks"`
}

// State 汇总进程状态。并发安全。
type State struct {
	db    Dep
	cache Dep

	cacheTTL time.Duration
	pingTTL  time.Duration

	started  bool
	draining bool

	mu       sync.Mutex
	checked  time.Time
	dbCheck  Check
	cacheChk Check
}

// New 构造状态机。cache 可为 nil（未配置 Redis）。
func New(db, cache Dep, cacheTTL, pingTTL time.Duration) *State {
	return &State{db: db, cache: cache, cacheTTL: cacheTTL, pingTTL: pingTTL}
}

// MarkStarted 表示初始化完成，startupz 从此通过。
func (s *State) MarkStarted() {
	s.mu.Lock()
	s.started = true
	s.mu.Unlock()
}

// MarkDraining 表示进程进入终止流程，readyz 立即失败。
func (s *State) MarkDraining() {
	s.mu.Lock()
	s.draining = true
	s.mu.Unlock()
}

// Draining 报告是否处于终止流程。
func (s *State) Draining() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.draining
}

// Started 报告初始化是否完成。
func (s *State) Started() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.started
}

// Startup 返回 startupz 结果。
func (s *State) Startup() Report {
	if !s.Started() {
		return Report{Status: "starting", Checks: map[string]Check{
			"initialized": {OK: false, Required: true, Detail: "bootstrap in progress"},
		}}
	}
	return Report{Status: "ok", Checks: map[string]Check{
		"initialized": {OK: true, Required: true},
	}}
}

// Live 返回 livez 结果。只要进程能响应即为 ok；依赖状态仅作参考。
func (s *State) Live() Report {
	checks := map[string]Check{
		"process": {OK: true, Required: true},
	}
	s.mu.Lock()
	draining := s.draining
	s.mu.Unlock()
	if draining {
		checks["draining"] = Check{OK: true, Required: false, Detail: "terminating; no new work"}
	}
	return Report{Status: "ok", Checks: checks}
}

// Ready 返回 readyz 结果。数据库是硬依赖；Redis 是软依赖。
func (s *State) Ready(ctx context.Context) Report {
	s.mu.Lock()
	if s.draining {
		s.mu.Unlock()
		return Report{Status: "draining", Checks: map[string]Check{
			"draining": {OK: false, Required: true, Detail: "instance is terminating, not accepting work"},
		}}
	}
	if !s.started {
		s.mu.Unlock()
		return s.Startup()
	}
	s.mu.Unlock()

	s.refresh(ctx)

	s.mu.Lock()
	defer s.mu.Unlock()
	checks := map[string]Check{
		"database": s.dbCheck,
	}
	if s.cache == nil {
		checks["cache"] = Check{OK: true, Required: false, Detail: "not configured; request counters disabled"}
	} else {
		// 缓存不可用属于降级，不参与 required。
		c := s.cacheChk
		c.Required = false
		checks["cache"] = c
	}
	status := "ok"
	if !s.dbCheck.OK {
		status = "unavailable"
	} else if s.cache != nil && !s.cacheChk.OK {
		status = "degraded"
	}
	return Report{Status: status, Checks: checks}
}

// Healthy 报告 readyz 是否应返回 2xx。
func Healthy(r Report) bool {
	return r.Status == "ok" || r.Status == "degraded"
}

// refresh 在 TTL 内复用依赖检查结果，避免高频探针压垮数据库。
func (s *State) refresh(ctx context.Context) {
	s.mu.Lock()
	fresh := time.Since(s.checked) < s.cacheTTL
	s.mu.Unlock()
	if fresh {
		return
	}

	dbCheck := probe(ctx, s.db, s.pingTTL)
	var cacheChk Check
	if s.cache != nil {
		cacheChk = probe(ctx, s.cache, s.pingTTL)
		cacheChk.Required = false // Redis 只影响计数，不阻断接流量
	} else {
		cacheChk = Check{OK: true, Required: false}
	}

	s.mu.Lock()
	s.dbCheck = dbCheck
	s.cacheChk = cacheChk
	s.checked = time.Now()
	s.mu.Unlock()
}

func probe(parent context.Context, dep Dep, timeout time.Duration) Check {
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	if err := dep.Ping(ctx); err != nil {
		return Check{OK: false, Required: true, Detail: err.Error()}
	}
	return Check{OK: true, Required: true}
}
