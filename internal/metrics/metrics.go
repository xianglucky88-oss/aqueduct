package metrics

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Requests 是每次抓取请求的计数器，供 /stats 展示近期请求量。
//
// 计数只用于观测，不参与限流判定；限流判定由 internal/budget 负责。
type Requests struct {
	rdb *redis.Client
}

// New 基于已有连接构造计数器。rdb 为 nil 时所有方法静默空转。
func New(rdb *redis.Client) *Requests {
	return &Requests{rdb: rdb}
}

// Close 由连接的实际拥有方（budget.Registry 的创建处）负责，这里不关闭连接。
func (r *Requests) Close() error { return nil }

// Incr 为站点累加一次请求计数。键按分钟分桶，保留 2 小时。
func (r *Requests) Incr(ctx context.Context, siteID string) {
	if r == nil || r.rdb == nil {
		return
	}
	key := fmt.Sprintf("aq:req:%s:%s", siteID, time.Now().UTC().Format("20060102T1504"))
	pipe := r.rdb.Pipeline()
	pipe.Incr(ctx, key)
	pipe.Expire(ctx, key, 2*time.Hour)
	_, _ = pipe.Exec(ctx)
}

// Total 返回站点最近 120 分钟内的请求总数。供 /stats 展示。
func (r *Requests) Total(ctx context.Context, siteID string) int64 {
	if r == nil || r.rdb == nil {
		return 0
	}
	now := time.Now().UTC()
	var total int64
	for i := 0; i < 120; i++ {
		key := fmt.Sprintf("aq:req:%s:%s", siteID, now.Add(-time.Duration(i)*time.Minute).Format("20060102T1504"))
		v, err := r.rdb.Get(ctx, key).Int64()
		if err == nil {
			total += v
		}
	}
	return total
}
