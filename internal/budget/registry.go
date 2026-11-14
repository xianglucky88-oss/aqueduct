package budget

import (
	"context"
	"fmt"
	"hash/fnv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/internal-platform/aqueduct/internal/config"
)

// Registry 持有每个站点一个治理器，并管理共享存储连接与降级模式。
type Registry struct {
	rdb       *redis.Client
	workerID  string
	mu        sync.RWMutex
	governors map[string]Governor
	specs     map[string]Spec
}

// NewRegistry 根据配置构造治理器集合。
//
// rdb 为 nil 时进入纯本地模式：实例各自按全量配额限流，
// 与预算治理特性引入前的单机行为保持一致。
func NewRegistry(cfg *config.Config, rdb *redis.Client) (*Registry, error) {
	r := &Registry{
		rdb:       rdb,
		workerID:  cfg.WorkerID,
		governors: map[string]Governor{},
		specs:     map[string]Spec{},
	}
	for _, site := range cfg.Sites {
		if err := r.upsert(site); err != nil {
			return nil, err
		}
	}
	return r, nil
}

func specFromSite(site config.Site) (Spec, error) {
	rb, err := site.Budget.Resolve(site.RPS)
	if err != nil {
		return Spec{}, err
	}
	return Spec{
		Rate:             rb.Rate,
		Burst:            rb.Burst,
		MaxInflight:      rb.MaxInflight,
		MaxAttempts:      rb.MaxAttempts,
		FallbackFraction: rb.FallbackFraction,
		Version:          specVersion(site),
	}, nil
}

// specVersion 由预算字段内容派生配置版本：字段任一变化都会换桶，
// 保证旧配置下累积的令牌与收缩状态不会泄漏到新配置。
//
// 用 FNV-1a 对解析后的规范化字段做哈希：确定性、与时间无关，
// 因此单纯重启不会被误判为配置变更，而真正改字段必然得到不同版本。
func specVersion(site config.Site) int64 {
	rb, _ := site.Budget.Resolve(site.RPS)
	h := fnv.New64a()
	fmt.Fprintf(h, "rate=%.6f|burst=%d|inflight=%d|attempts=%d|fallback=%.6f",
		rb.Rate, rb.Burst, rb.MaxInflight, rb.MaxAttempts, rb.FallbackFraction)
	return int64(h.Sum64())
}

func (r *Registry) upsert(site config.Site) error {
	spec, err := specFromSite(site)
	if err != nil {
		return fmt.Errorf("site %s: %w", site.ID, err)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.specs[site.ID] = spec
	if existing, ok := r.governors[site.ID]; ok {
		if rg, ok := existing.(*redisGovernor); ok {
			rg.UpdateSpec(spec)
			return nil
		}
	}
	if r.rdb == nil {
		r.governors[site.ID] = NewLocalGovernor(site.ID, spec)
	} else {
		r.governors[site.ID] = NewRedisGovernor(r.rdb, r.workerID, site.ID, spec)
	}
	return nil
}

// Reload 应用新配置：已存在的站点热更新规格，新增的站点创建治理器。
func (r *Registry) Reload(cfg *config.Config) error {
	for _, site := range cfg.Sites {
		if err := r.upsert(site); err != nil {
			return err
		}
	}
	return nil
}

// Get 返回站点治理器。
func (r *Registry) Get(siteID string) (Governor, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	g, ok := r.governors[siteID]
	return g, ok
}

// Must 与 Get 相同，但缺失时 panic，仅用于调用方确信配置存在的场景。
func (r *Registry) Must(siteID string) Governor {
	g, ok := r.Get(siteID)
	if !ok {
		panic("budget: unknown site " + siteID)
	}
	return g
}

// Snapshots 返回全部站点的观测快照。
func (r *Registry) Snapshots(ctx context.Context) []Snapshot {
	r.mu.RLock()
	ids := make([]string, 0, len(r.governors))
	govs := make([]Governor, 0, len(r.governors))
	for id, g := range r.governors {
		ids = append(ids, id)
		govs = append(govs, g)
	}
	r.mu.RUnlock()
	out := make([]Snapshot, 0, len(govs))
	for i, g := range govs {
		snap := g.Snapshot(ctx)
		if snap.SiteID == "" {
			snap.SiteID = ids[i]
		}
		out = append(out, snap)
	}
	return out
}

// Close 释放治理器资源。
func (r *Registry) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	var firstErr error
	for id, g := range r.governors {
		if err := g.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(r.governors, id)
	}
	return firstErr
}

// Ping 探测共享存储；本地模式下永远返回 nil。
func (r *Registry) Ping(ctx context.Context) error {
	if r.rdb == nil {
		return nil
	}
	c, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	return r.rdb.Ping(c).Err()
}
