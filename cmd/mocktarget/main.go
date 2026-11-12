// mocktarget 模拟上游数据站点。
//
// 目标：让采集器的题目在完全离线的环境下可复现、可注入故障、可精确回放。
//
// 端点：
//
//	GET  /api/v1/health
//	GET  /{site}/items?page|  cursor & page_size   分页拉取
//	GET  /admin/state                               查看当前数据集指纹
//	POST /admin/shift?ratio=0.3                     按确定性规则改价/改库存
//	POST /admin/fault?site=supplier&kind=429&rate=0.2&seed=7
//	POST /admin/fault/clear
//	POST /admin/reset                               重置故障与数据
//
// 所有随机行为都由 seed 决定，同样的 seed + 同样的请求序列
// 得到完全一致的响应，因此可以离线回放并断言最终库内状态。
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type item struct {
	SourceID   string            `json:"source_id"`
	Title      string            `json:"title"`
	PriceCents int64             `json:"price_cents"`
	Stock      int               `json:"stock"`
	UpdatedAt  string            `json:"updated_at"`
	Extra      map[string]string `json:"extra,omitempty"`
}

type site struct {
	id      string
	mode    string // cursor | page | single
	items   []item
	version int
}

type fault struct {
	site string
	kind string // 429 | 500 | timeout
	rate float64
	seed int64
	rnd  *rand.Rand
	mu   sync.Mutex
}

type server struct {
	mu       sync.RWMutex
	sites    map[string]*site
	fault    *fault
	requests map[string]int
	baseTime time.Time
}

func main() {
	addr := flag.String("addr", ":9100", "listen address")
	seed := flag.Int64("seed", 20261112, "dataset seed")
	sites := flag.String("sites", "supplier,catalog,stock,notice", "comma separated site ids")
	flag.Parse()

	base := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	s := &server{
		sites:    map[string]*site{},
		requests: map[string]int{},
		baseTime: base,
	}
	for i, id := range strings.Split(*sites, ",") {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		mode := "cursor"
		if i%3 == 1 {
			mode = "page"
		}
		if i%4 == 3 {
			mode = "single"
		}
		s.sites[id] = newSite(id, mode, *seed+int64(i)*991, base)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]any{"status": "ok", "sites": s.siteIDs()})
	})
	mux.HandleFunc("/admin/state", s.handleState)
	mux.HandleFunc("/admin/shift", s.handleShift)
	mux.HandleFunc("/admin/fault", s.handleFault)
	mux.HandleFunc("/admin/fault/clear", s.handleFaultClear)
	mux.HandleFunc("/admin/reset", s.handleReset)
	mux.HandleFunc("/", s.handleItems)

	log.Printf("mocktarget listening on %s, sites=%d", *addr, len(s.sites))
	if err := http.ListenAndServe(*addr, logMW(mux)); err != nil {
		fmt.Fprintf(os.Stderr, "mocktarget: %v\n", err)
		os.Exit(1)
	}
}

func newSite(id, mode string, seed int64, base time.Time) *site {
	rnd := rand.New(rand.NewSource(seed))
	n := 120 + rnd.Intn(80)
	items := make([]item, 0, n)
	prefix := map[string]string{
		"supplier": "SUP",
		"catalog":  "SPU",
		"stock":    "STK",
		"notice":   "NTC",
	}[id]
	if prefix == "" {
		prefix = strings.ToUpper(id[:min(3, len(id))])
	}
	titles := []string{"华东区域集采", "二级供应商备货", "季度框架协议", "临期折价批次", "新品试销"}
	for i := 0; i < n; i++ {
		items = append(items, item{
			SourceID:   fmt.Sprintf("%s-%05d", prefix, i+1),
			Title:      fmt.Sprintf("%s %s #%d", id, titles[rnd.Intn(len(titles))], i+1),
			PriceCents: int64(1000 + rnd.Intn(90000)),
			Stock:      rnd.Intn(500),
			UpdatedAt:  base.Add(time.Duration(i) * 17 * time.Minute).Format(time.RFC3339),
			Extra:      map[string]string{"region": []string{"east", "north", "south"}[rnd.Intn(3)]},
		})
	}
	return &site{id: id, mode: mode, items: items, version: 1}
}

func (s *server) siteIDs() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.sites))
	for id := range s.sites {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// handleItems 是所有业务数据端点的入口：/{site}/items
func (s *server) handleItems(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 2 || parts[len(parts)-1] != "items" {
		writeJSON(w, 404, map[string]string{"error": "not found"})
		return
	}
	siteID := parts[0]

	if f := s.injectFault(siteID); f != "" {
		switch f {
		case "429":
			w.Header().Set("Retry-After", "1")
			writeJSON(w, 429, map[string]string{"error": "slow down"})
			return
		case "500":
			writeJSON(w, 500, map[string]string{"error": "upstream exploded"})
			return
		case "timeout":
			time.Sleep(20 * time.Second)
			writeJSON(w, 504, map[string]string{"error": "upstream timeout"})
			return
		}
	}

	s.mu.RLock()
	st, ok := s.sites[siteID]
	if !ok {
		s.mu.RUnlock()
		writeJSON(w, 404, map[string]string{"error": "unknown site " + siteID})
		return
	}
	s.mu.RUnlock()

	pageSize := 50
	if v := r.URL.Query().Get("page_size"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 500 {
			pageSize = n
		}
	}

	switch st.mode {
	case "page":
		pageNum := 1
		if v := r.URL.Query().Get("page"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 {
				pageNum = n
			}
		}
		s.servePage(w, st, (pageNum-1)*pageSize, pageSize, strconv.Itoa(pageNum+1), (pageNum)*pageSize < len(st.items))
	case "single":
		s.servePage(w, st, 0, len(st.items), "", false)
	default: // cursor
		offset := 0
		if cur := r.URL.Query().Get("cursor"); cur != "" {
			if n, err := strconv.Atoi(cur); err == nil && n >= 0 {
				offset = n
			}
		}
		end := offset + pageSize
		s.servePage(w, st, offset, pageSize, strconv.Itoa(end), end < len(st.items))
	}
}

func (s *server) servePage(w http.ResponseWriter, st *site, offset, size int, next string, hasMore bool) {
	s.mu.RLock()
	items := st.items
	version := st.version
	s.mu.RUnlock()

	if offset > len(items) {
		offset = len(items)
	}
	end := offset + size
	if end > len(items) {
		end = len(items)
	}
	// 返回副本，避免并发读写。
	pageItems := make([]item, end-offset)
	copy(pageItems, items[offset:end])

	s.mu.Lock()
	s.requests[st.id]++
	s.mu.Unlock()

	writeJSON(w, 200, map[string]any{
		"items":       pageItems,
		"next_cursor": next,
		"has_more":    hasMore,
		"dataset_ver": version,
		"site":        st.id,
		"mode":        st.mode,
	})
}

func (s *server) injectFault(siteID string) string {
	s.mu.RLock()
	f := s.fault
	s.mu.RUnlock()
	if f == nil || f.rate <= 0 {
		return ""
	}
	if f.site != "*" && f.site != siteID {
		return ""
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.rnd.Float64() < f.rate {
		return f.kind
	}
	return ""
}

func (s *server) handleState(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := map[string]any{}
	for id, st := range s.sites {
		var sum int64
		for _, it := range st.items {
			sum += it.PriceCents
		}
		out[id] = map[string]any{
			"items":       len(st.items),
			"version":     st.version,
			"price_sum":   sum,
			"mode":        st.mode,
			"requests":    s.requests[id],
			"fault":       faultDesc(s.fault, id),
		}
	}
	writeJSON(w, 200, out)
}

func faultDesc(f *fault, site string) string {
	if f == nil || f.rate <= 0 {
		return ""
	}
	if f.site != "*" && f.site != site {
		return ""
	}
	return fmt.Sprintf("%s@%.2f", f.kind, f.rate)
}

// handleShift 按确定性规则修改一部分商品的价格与库存。
//
// 用于演示"内容改写"场景：
// shift 之后重新采集，若 ItemKey 含可变字段，行数会膨胀。
func (s *server) handleShift(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, 405, map[string]string{"error": "POST only"})
		return
	}
	ratio := 0.3
	if v := r.URL.Query().Get("ratio"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f > 0 && f <= 1 {
			ratio = f
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, st := range s.sites {
		st.version++
		rnd := rand.New(rand.NewSource(int64(st.version)*7919 + int64(len(st.items))))
		for i := range st.items {
			if rnd.Float64() < ratio {
				// 价格上调 5%~15%，库存减少。
				delta := 105 + rnd.Intn(11)
				st.items[i].PriceCents = st.items[i].PriceCents * int64(delta) / 100
				if st.items[i].Stock > 0 {
					st.items[i].Stock -= rnd.Intn(5) + 1
					if st.items[i].Stock < 0 {
						st.items[i].Stock = 0
					}
				}
				st.items[i].UpdatedAt = s.baseTime.Add(time.Duration(st.version) * time.Hour).Format(time.RFC3339)
			}
		}
	}
	out := map[string]any{"shifted": true, "ratio": ratio, "versions": map[string]int{}}
	for id, st := range s.sites {
		out["versions"].(map[string]int)[id] = st.version
	}
	writeJSON(w, 200, out)
}

func (s *server) handleFault(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, 405, map[string]string{"error": "POST only"})
		return
	}
	q := r.URL.Query()
	siteID := q.Get("site")
	if siteID == "" {
		siteID = "*"
	}
	kind := q.Get("kind")
	if kind != "429" && kind != "500" && kind != "timeout" {
		writeJSON(w, 400, map[string]string{"error": "kind must be 429|500|timeout"})
		return
	}
	rate := 0.2
	if v := q.Get("rate"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			rate = f
		}
	}
	seed := time.Now().UnixNano()
	if v := q.Get("seed"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			seed = n
		}
	}
	s.mu.Lock()
	s.fault = &fault{site: siteID, kind: kind, rate: rate, seed: seed, rnd: rand.New(rand.NewSource(seed))}
	s.mu.Unlock()
	writeJSON(w, 200, map[string]any{"site": siteID, "kind": kind, "rate": rate, "seed": seed})
}

func (s *server) handleFaultClear(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.fault = nil
	s.mu.Unlock()
	writeJSON(w, 200, map[string]bool{"cleared": true})
}

func (s *server) handleReset(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fault = nil
	s.requests = map[string]int{}
	seed := int64(20261112)
	for i, id := range []string{"supplier", "catalog", "stock", "notice"} {
		if st, ok := s.sites[id]; ok {
			st.items = newSite(id, st.mode, seed+int64(i)*991, s.baseTime).items
			st.version = 1
		}
	}
	writeJSON(w, 200, map[string]bool{"reset": true})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func logMW(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		if strings.HasPrefix(r.URL.Path, "/admin") {
			log.Printf("%s %s %s", r.Method, r.URL.Path, time.Since(start).Round(time.Millisecond))
		}
	})
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
