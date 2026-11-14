package fetcher

import (
	"context"
	"encoding/json"
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

// ErrRateLimited 表示目标站点返回 429。
var ErrRateLimited = fmt.Errorf("rate limited")

// ErrServerError 表示目标站点返回 5xx。
var ErrServerError = fmt.Errorf("upstream server error")

// Client 是对单个站点的抓取客户端。
//
// 历史实现：节流靠一个固定 sleep，没有全局配额，也没有退避。
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
	// 固定 sleep 节流。
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
	req.Header.Set("User-Agent", "aqueduct/1.4.0")
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
		// 直接返回错误，不重试。
		io.Copy(io.Discard, resp.Body)
		return nil, fmt.Errorf("%w: %s", ErrRateLimited, u.String())
	case resp.StatusCode >= 500:
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("%w: %d %s", ErrServerError, resp.StatusCode, string(body))
	case resp.StatusCode >= 400:
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("upstream %d: %s", resp.StatusCode, string(body))
	}

	var payload wirePage
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, fmt.Errorf("decode page: %w", err)
	}
	return payload.toPage(c.site.ID), nil
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
		var ts time.Time
		if it.UpdatedAt != "" {
			parsed, err := time.Parse(time.RFC3339, it.UpdatedAt)
			if err != nil {
				// 非法时间视为「上游未提供时间」，保持零值。
				// 采集层不得用本机时间伪造业务时间，否则会覆盖更晚的真实事实。
				log.Printf("warn: bad updated_at %q on %s/%s; treating as missing",
					it.UpdatedAt, siteID, it.SourceID)
			} else {
				ts = parsed
			}
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
