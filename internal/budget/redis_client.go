package budget

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// OpenRedis 连接共享存储并等待其可用。
//
// 启动阶段容忍短暂不可用：最多重试 5 次，每次间隔 1 秒；
// 最终仍失败时返回错误，由调用方决定降级为本地模式。
func OpenRedis(ctx context.Context, url string) (*redis.Client, error) {
	opt, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("parse redis url: %w", err)
	}
	rdb := redis.NewClient(opt)

	var lastErr error
	for i := 0; i < 5; i++ {
		pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		err := rdb.Ping(pingCtx).Err()
		cancel()
		if err == nil {
			return rdb, nil
		}
		lastErr = err
		select {
		case <-time.After(time.Second):
		case <-ctx.Done():
			_ = rdb.Close()
			return nil, ctx.Err()
		}
	}
	_ = rdb.Close()
	return nil, fmt.Errorf("ping redis: %w", lastErr)
}
