package model

import (
	"crypto/sha256"
	"encoding/hex"
	"time"
)

// 运行状态。除 succeeded/failed 外，abandoned 表示「因进程终止或失去租约
// 而主动放弃」——它既不代表上游故障，也不代表数据丢失：已提交的页和游标
// 都保留在库内，下一个持有租约的副本会从检查点继续。
const (
	RunStatusRunning   = "running"
	RunStatusSucceeded = "succeeded"
	RunStatusFailed    = "failed"
	RunStatusAbandoned = "abandoned"
)

// Record 是采集落库的最小单元。
//
// SourceID 是目标站点给出的业务主键（例如供应商编码、商品 SPU）。
// ItemKey 是落库时使用的唯一键，由 Identity() 计算。
type Record struct {
	SiteID     string            `json:"site_id"`
	SourceID   string            `json:"source_id"`
	ItemKey    string            `json:"item_key"`
	Title      string            `json:"title"`
	PriceCents int64             `json:"price_cents"`
	Stock      int               `json:"stock"`
	UpdatedAt  time.Time         `json:"updated_at"`
	Payload    map[string]string `json:"payload,omitempty"`
}

// Identity 计算库内唯一键。
//
// 键只由站点与上游业务主键决定，与价格、库存等内容字段无关。
// 这样同一实体的内容变化会原地更新（UPSERT ... DO UPDATE），而不是
// 生成新行。这一点对多副本接管尤其重要：接管方从检查点重放某一页时，
// 即便与中止方发生过重叠写入，也只会有同一行被重复更新，不会产生重复数据。
func (r Record) Identity() string {
	sum := sha256.Sum256([]byte(r.SiteID + "|" + r.SourceID))
	return hex.EncodeToString(sum[:8])
}

// Page 是目标站点一页响应的解析结果。
type Page struct {
	Items      []Record
	NextCursor string
	HasMore    bool
}

// Run 是一次采集执行的记录。
//
// 命名提示：数据库列是 items_wrote，JSON 字段是 items_written。
// 三者（Go 字段 / JSON / 列名）不完全一致，改动时需同时调整
// internal/store 的 SQL 与本结构体的 tag。
type Run struct {
	ID         string     `json:"run_id"`
	SiteID     string     `json:"site_id"`
	WorkerID   string     `json:"worker_id"`
	StartedAt  time.Time  `json:"started_at"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
	Status     string     `json:"status"`
	ItemsSeen  int        `json:"items_seen"`
	ItemsWrote int        `json:"items_written"`
	LastError  string     `json:"last_error,omitempty"`
}

// SiteStats 是单站点的落库统计，供 /stats 与验证脚本使用。
type SiteStats struct {
	SiteID         string `json:"site_id"`
	Rows           int    `json:"rows"`
	DistinctSource int    `json:"distinct_source_ids"`
	MaxUpdatedAt   string `json:"max_updated_at,omitempty"`
}

// Stats 是全局统计。
type Stats struct {
	Sites     []SiteStats       `json:"sites"`
	TotalRows int               `json:"total_rows"`
	TotalRuns int               `json:"total_runs"`
	LastRunAt string            `json:"last_run_at,omitempty"`
	Cursors   map[string]string `json:"cursors"`
}

// DuplicateRatio 返回重复率：1 - 去重业务主键数 / 总行数。
func (s SiteStats) DuplicateRatio() float64 {
	if s.Rows == 0 {
		return 0
	}
	return 1 - float64(s.DistinctSource)/float64(s.Rows)
}
