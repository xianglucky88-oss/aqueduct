package fetcher

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/internal-platform/aqueduct/internal/budget"
	"github.com/internal-platform/aqueduct/internal/config"
	"github.com/internal-platform/aqueduct/internal/model"
)

// ErrRateLimited 表示目标站点返回 429（所有重试耗尽后）。
var ErrRateLimited = fmt.Errorf("rate limited")

// ErrServerError 表示目标站点返回 5xx（所有重试耗尽后）。
var ErrServerError = fmt.Errorf("upstream server error")

// attemptOutcome 区分一次 HTTP 尝试的结果，驱动预算反馈。
type attemptOutcome int

// AttemptOutcome 是 attemptOutcome 的导出版本，供观测回调签名使用。
type AttemptOutcome = attemptOutcome

const (
	outcomeSuccess attemptOutcome = iota
	outcomeLimited
	outcomeRetryable
	outcomeFatal
)

// 观测回调可用的结果类别。
const (
	OutcomeSuccess   = outcomeSuccess
	OutcomeLimited   = outcomeLimited
	OutcomeRetryable = outcomeRetryable
	OutcomeFatal     = outcomeFatal
)

// String 让结果类别在日志中可读。
func (o attemptOutcome) String() string {
	switch o {
	case outcomeSuccess:
		return "success"
	case outcomeLimited:
		return "rate_limited"
	case outcomeRetryable:
		return "retryable"
	case outcomeFatal:
		return "fatal"
	default:
		return "unknown"
	}
}

// Client 是对单个站点的抓取客户端。
//
// 节流与并发控制全部委托给 budget.Governor：每次 HTTP 尝试（含重试）
// 都先申请令牌与在途名额，返回后释放名额并上报成功/限流，
// 因此长期速率、突发、并发与重试消耗受同一份配额约束。
type Client struct {
	site     config.Site
	gov      budget.Governor
	http     *http.Client
	onResult func(siteID string, attempt, maxAttempts int, waited time.Duration,
		status budget.AcquireStatus, outcome attemptOutcome, err error)
}

// Option 调整 Client 构造。
type Option func(*Client)

// WithObserver 注入每次尝试的观测回调（日志/指标用）。
func WithObserver(fn func(siteID string, attempt, maxAttempts int, waited time.Duration,
	status budget.AcquireStatus, outcome attemptOutcome, err error)) Option {
	return func(c *Client) { c.onResult = fn }
}

// New 构造一个客户端。gov 为 nil 时退化为不节流（仅测试使用）。
func New(site config.Site, gov budget.Governor, opts ...Option) *Client {
	c := &Client{
		site: site,
		gov:  gov,
		http: &http.Client{Timeout: 15 * time.Second},
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Page 抓取一页。cursor 为空表示第一页。
//
// 每次尝试都重新申请预算；429/5xx/网络错误在预算允许的 MaxAttempts 内重试，
// 4xx 等确定性错误不重试。
func (c *Client) Page(ctx context.Context, cursor string) (*model.Page, error) {
	maxAttempts := 1
	if c.gov != nil {
		maxAttempts = c.gov.Spec().MaxAttempts
	}

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		grant, err := c.acquire(ctx, attempt)
		if err != nil {
			return nil, err
		}
		page, outcome, retryAfter, err := c.doAttempt(ctx, cursor)
		if c.gov != nil {
			c.gov.Release(ctx, grant)
			switch outcome {
			case outcomeLimited:
				c.gov.MarkLimited(ctx, retryAfter)
			case outcomeSuccess:
				c.gov.MarkSuccess(ctx)
			}
		}
		if c.onResult != nil {
			c.onResult(c.site.ID, attempt, maxAttempts, grant.Waited, grant.Status, outcome, err)
		}

		switch outcome {
		case outcomeSuccess:
			return page, nil
		case outcomeFatal:
			return nil, err
		default: // outcomeLimited / outcomeRetryable
			lastErr = err
			if attempt >= maxAttempts {
				return nil, lastErr
			}
			if err := c.waitRetry(ctx, attempt, retryAfter, outcome); err != nil {
				return nil, err
			}
		}
	}
	return nil, lastErr
}

func (c *Client) acquire(ctx context.Context, attempt int) (budget.Grant, error) {
	if c.gov == nil {
		return budget.Grant{SiteID: c.site.ID, Attempt: attempt, Status: budget.StatusGranted}, nil
	}
	return c.gov.Acquire(ctx, attempt)
}

// waitRetry 在两次尝试之间等待。
//
// 限流（429）优先遵从上游 Retry-After；其余可重试错误使用短指数退避，
// 并叠加抖动避免多实例同步重试。attempt 是刚结束的尝试序号（从 1 起）。
func (c *Client) waitRetry(ctx context.Context, attempt int, retryAfter time.Duration, outcome attemptOutcome) error {
	d := retryAfter
	if d <= 0 {
		base := 100 * time.Millisecond
		if outcome == outcomeLimited {
			base = budget.DefaultRetryAfter
		}
		// 退避序列：base, 2*base, 4*base, ...，上限 16*base。
		shift := attempt - 1
		if shift > 4 {
			shift = 4
		}
		d = jitter(base * time.Duration(1<<shift))
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// jitterRand 带锁保护，避免多 goroutine 并发使用全局 rand 源。
var (
	jitterMu  sync.Mutex
	jitterSrc = rand.New(rand.NewSource(time.Now().UnixNano()))
)

// jitter 在 ±20% 范围内打散等待时长，避免集群内重试同拍。
func jitter(d time.Duration) time.Duration {
	jitterMu.Lock()
	f := 0.8 + jitterSrc.Float64()*0.4
	jitterMu.Unlock()
	jd := time.Duration(float64(d) * f)
	if jd < 10*time.Millisecond {
		jd = 10 * time.Millisecond
	}
	return jd
}

// doAttempt 执行一次 HTTP 尝试并分类结果。
func (c *Client) doAttempt(ctx context.Context, cursor string) (*model.Page, attemptOutcome, time.Duration, error) {
	u, err := url.Parse(c.site.BaseURL + c.site.Path)
	if err != nil {
		return nil, outcomeFatal, 0, fmt.Errorf("bad base url: %w", err)
	}
	q := u.Query()
	q.Set("page_size", strconv.Itoa(c.site.PageSize))
	switch c.site.Mode {
	case "cursor":
		if cursor != "" {
			q.Set("cursor", cursor)
		}
	case "page":
		if cursor == "" {
			cursor = "1"
		}
		q.Set("page", cursor)
	default:
		// single 模式忽略游标
	}
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, outcomeFatal, 0, err
	}
	req.Header.Set("User-Agent", "aqueduct/1.4.0")
	req.Header.Set("X-Worker", "collector")
	for k, v := range c.site.Headers {
		req.Header.Set(k, v)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		// 网络错误通常可重试；ctx 取消交由上层终止。
		if ctx.Err() != nil {
			return nil, outcomeFatal, 0, ctx.Err()
		}
		return nil, outcomeRetryable, 0, fmt.Errorf("request %s: %w", u.String(), err)
	}
	defer resp.Body.Close()

	switch {
	case resp.StatusCode == http.StatusTooManyRequests:
		io.Copy(io.Discard, io.LimitReader(resp.Body, 1024))
		return nil, outcomeLimited, parseRetryAfter(resp.Header.Get("Retry-After")),
			fmt.Errorf("%w: %s", ErrRateLimited, u.String())
	case resp.StatusCode >= 500:
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, outcomeRetryable, 0,
			fmt.Errorf("%w: %d %s", ErrServerError, resp.StatusCode, strings.TrimSpace(string(body)))
	case resp.StatusCode >= 400:
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, outcomeFatal, 0,
			fmt.Errorf("upstream %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var payload wirePage
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, outcomeFatal, 0, fmt.Errorf("decode page: %w", err)
	}
	return payload.toPage(c.site.ID), outcomeSuccess, 0, nil
}

// parseRetryAfter 解析 Retry-After，支持 delta-seconds 与 HTTP-date。
func parseRetryAfter(v string) time.Duration {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(v); err == nil {
		if secs < 0 {
			return 0
		}
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		if d := time.Until(t); d > 0 {
			return d
		}
	}
	return 0
}

type wirePage struct {
	Items []struct {
		SourceID   string            `json:"source_id"`
		Title      string            `json:"title"`
		PriceCents int64             `json:"price_cents"`
		Stock      int               `json:"stock"`
		UpdatedAt  string            `json:"updated_at"`
		Extra      map[string]string `json:"extra"`
	} `json:"items"`
	NextCursor string `json:"next_cursor"`
	HasMore    bool   `json:"has_more"`
}

func (w wirePage) toPage(siteID string) *model.Page {
	p := &model.Page{NextCursor: w.NextCursor, HasMore: w.HasMore}
	for _, it := range w.Items {
		ts, err := time.Parse(time.RFC3339, it.UpdatedAt)
		if err != nil {
			log.Printf("warn: bad updated_at %q on %s/%s", it.UpdatedAt, siteID, it.SourceID)
			ts = time.Now().UTC()
		}
		rec := model.Record{
			SiteID:     siteID,
			SourceID:   it.SourceID,
			Title:      it.Title,
			PriceCents: it.PriceCents,
			Stock:      it.Stock,
			UpdatedAt:  ts,
			Payload:    it.Extra,
		}
		rec.ItemKey = rec.Identity()
		p.Items = append(p.Items, rec)
	}
	return p
}
