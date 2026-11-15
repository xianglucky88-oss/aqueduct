package fetcher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/internal-platform/aqueduct/internal/config"
	"github.com/internal-platform/aqueduct/internal/model"
)

// 错误分类决定调用方的处置：
//   - 429 / 5xx / 网络错误：可重试，按退避重采；
//   - 其他 4xx：不可重试，通常意味着配置或凭据错误；
//   - 游标被上游拒绝：需要从头开始，而不是无限重试。
var (
	ErrRateLimited = errors.New("rate limited")
	ErrServerError = errors.New("upstream server error")
	ErrUpstream4xx = errors.New("upstream client error")
	ErrCursorReset = errors.New("upstream rejected cursor")
)

// RetryAfterError 携带上游给出的 Retry-After，供调度器优先采用。
type RetryAfterError struct {
	Err        error
	RetryAfter time.Duration
}

func (e *RetryAfterError) Error() string { return e.Err.Error() }
func (e *RetryAfterError) Unwrap() error { return e.Err }

// RetryAfter 从任意错误里提取上游建议的重试等待时间。
func RetryAfter(err error) (time.Duration, bool) {
	var ra *RetryAfterError
	if errors.As(err, &ra) {
		return ra.RetryAfter, true
	}
	return 0, false
}

// Retryable 判断错误是否值得重试。429、5xx 与网络错误可重试；
// 4xx 配置类错误不可重试。
func Retryable(err error) bool {
	switch {
	case errors.Is(err, ErrRateLimited), errors.Is(err, ErrServerError):
		return true
	case errors.Is(err, ErrUpstream4xx), errors.Is(err, ErrCursorReset):
		return false
	}
	// context 取消不是上游问题。
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	// 其余视为网络/传输错误，可重试。
	return true
}

// Client 是对单个站点的抓取客户端。
//
// 节流按站点 RPS 做请求间最小间隔，并尊重 ctx，使终止流程不会被
// sleep 卡住。
type Client struct {
	site   config.Site
	http   *http.Client
	sleep  time.Duration
	lastAt time.Time
}

// New 构造一个客户端。
func New(site config.Site) *Client {
	return &Client{
		site: site,
		http: &http.Client{Timeout: 15 * time.Second},
		// 固定节流：RPS 为 2 时每请求 sleep 500ms。
		sleep: time.Second / time.Duration(max(site.RPS, 1)),
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// Page 抓取一页。cursor 为空表示第一页。
func (c *Client) Page(ctx context.Context, cursor string) (*model.Page, error) {
	elapsed := time.Since(c.lastAt)
	if elapsed < c.sleep {
		select {
		case <-time.After(c.sleep - elapsed):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	c.lastAt = time.Now()

	u, err := url.Parse(c.site.BaseURL + c.site.Path)
	if err != nil {
		return nil, fmt.Errorf("bad base url: %w", err)
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
		return nil, err
	}
	req.Header.Set("User-Agent", "aqueduct/1.5.0")
	req.Header.Set("X-Worker", "collector")
	for k, v := range c.site.Headers {
		req.Header.Set(k, v)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request %s: %w", u.String(), err)
	}
	defer resp.Body.Close()

	switch {
	case resp.StatusCode == http.StatusTooManyRequests:
		retryAfter := parseRetryAfter(resp.Header.Get("Retry-After"))
		io.Copy(io.Discard, resp.Body)
		base := fmt.Errorf("%w: %s", ErrRateLimited, u.String())
		if retryAfter > 0 {
			return nil, &RetryAfterError{Err: base, RetryAfter: retryAfter}
		}
		return nil, base
	case resp.StatusCode == http.StatusGone || resp.StatusCode == http.StatusBadRequest:
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		// 游标可能因上游重建索引而失效。交由调用方决定是否重置游标，
		// 不能简单当作「重试直到成功」，否则会无限重采。
		return nil, fmt.Errorf("%w: %d %s", ErrCursorReset, resp.StatusCode, string(body))
	case resp.StatusCode >= 500:
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("%w: %d %s", ErrServerError, resp.StatusCode, string(body))
	case resp.StatusCode >= 400:
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("%w: %d %s", ErrUpstream4xx, resp.StatusCode, string(body))
	}

	var payload wirePage
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, fmt.Errorf("decode page: %w", err)
	}
	return payload.toPage(c.site.ID), nil
}

// parseRetryAfter 支持秒数与 HTTP-date 两种格式。上限截断，避免上游
// 给出过长的等待导致站点长期停滞。
func parseRetryAfter(v string) time.Duration {
	if v == "" {
		return 0
	}
	if n, err := strconv.Atoi(v); err == nil {
		if n < 0 {
			return 0
		}
		d := time.Duration(n) * time.Second
		if d > 5*time.Minute {
			d = 5 * time.Minute
		}
		return d
	}
	if t, err := http.ParseTime(v); err == nil {
		d := time.Until(t)
		if d < 0 {
			return 0
		}
		if d > 5*time.Minute {
			d = 5 * time.Minute
		}
		return d
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
