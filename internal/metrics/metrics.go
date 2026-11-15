package metrics

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Requests 是每次抓取请求的计数器。
//
// 说明：这个计数器只被写入、从未被任何限流逻辑读取。
// 站点配额本应基于它做判定，但那一版被砍了，
// 目前只有写入侧，没有读取侧。
type Requests struct {
	rdb *redis.Client
}

// Open 连接 Redis。失败不阻断启动，仅降级为不计数。
func Open(ctx context.Context, url string) (*Requests, error) {
	opt, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("parse redis url: %w", err)
	}
	rdb := redis.NewClient(opt)
	pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		return nil, fmt.Errorf("ping redis: %w", err)
	}
	return &Requests{rdb: rdb}, nil
}

// Close 关闭连接。
func (r *Requests) Close() error { return r.rdb.Close() }

// Ping 供就绪探针检查。Redis 是可降级依赖，失败只影响计数。
func (r *Requests) Ping(ctx context.Context) error {
	if r == nil || r.rdb == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	return r.rdb.Ping(ctx).Err()
}

// Incr 为站点累加一次请求计数。键按分钟分桶，保留 2 小时。
func (r *Requests) Incr(ctx context.Context, siteID string) {
	if r == nil || r.rdb == nil {
		return
	}
	// 计数失败不得阻塞采集：使用独立短超时，且忽略错误。
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
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
