package model

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"time"
)

// Record 是采集落库的最小单元。
//
// SourceID 是目标站点给出的业务主键（例如供应商编码、商品 SPU）。
// ItemKey 是当前状态表 records 里的主键，由 Identity() 计算。
//
// UpdatedAt 表示上游声明的业务更新时间，是「业务事实时间」。
// 上游缺失或时间格式非法时，这个字段为零值：采集层不得用本机时间伪造，
// 否则会覆盖掉时间更晚的真实业务事实。落库时零值转为 NULL，
// 系统观测时间由 store 层单独记录在 observed_at 上。
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

// Identity 计算来源对象的规范键。
//
// 身份只由 (site_id, source_id) 决定。价格、库存等可变属性不参与哈希，
// 因此调价不会让同一个来源对象获得新的 ItemKey。
//
// 兼容性：迁移前已经存在的对象在 current 表里保留其原始 item_key，
// 以免下游持有的旧键失效；这个规范键只用于迁移后新注册的对象。
// 「一个来源对象只有一个当前状态」由 (site_id, source_id) 唯一约束保证，
// 不依赖 item_key 的具体取值。
func (r Record) Identity() string {
	sum := sha256.Sum256([]byte(r.SiteID + "\x1f" + r.SourceID))
	return hex.EncodeToString(sum[:8])
}

// ContentHash 计算业务内容指纹。
//
// 只包含会构成「新版本」的可变属性（标题、价格、库存、extras），
// 不包含任何时间字段。内容相同时重复采集不会产生新版本。
func (r Record) ContentHash() string {
	h := sha256.New()
	writeField(h, "title", r.Title)
	writeField(h, "price_cents", strconv.FormatInt(r.PriceCents, 10))
	writeField(h, "stock", strconv.Itoa(r.Stock))

	keys := make([]string, 0, len(r.Payload))
	for k := range r.Payload {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		writeField(h, "payload:"+k, r.Payload[k])
	}
	return hex.EncodeToString(h.Sum(nil))
}

// writeField 用长度前缀写入字段，避免分隔符拼接带来的歧义。
func writeField(h interface{ Write([]byte) (int, error) }, name, value string) {
	fmt.Fprintf(h, "%d:%s=%d:%s;", len(name), name, len(value), value)
}

// HasUpdatedAt 报告上游是否给出了可用的业务时间。
func (r Record) HasUpdatedAt() bool { return !r.UpdatedAt.IsZero() }

// Version 是一个来源对象在某个可见时间区间内的快照。
//
// Revision 在单个 (site_id, source_id) 内严格递增，是稳定顺序的唯一依据；
// Revision 不跨对象、不可重排。ValidFrom 与 ValidTo 是半开区间
// [ValidFrom, ValidTo)，ValidTo 为 nil 表示仍然有效。
//
// 当一次变化与上一次共享同一个可见时间戳时，区间长度为 0；
// 按时间点查询时用 revision 作为同一时间戳下的裁决顺序。
type Version struct {
	SiteID      string            `json:"site_id"`
	SourceID    string            `json:"source_id"`
	ItemKey     string            `json:"item_key"`
	Title       string            `json:"title"`
	PriceCents  int64             `json:"price_cents"`
	Stock       int               `json:"stock"`
	UpdatedAt   *time.Time        `json:"updated_at,omitempty"`
	Payload     map[string]string `json:"payload,omitempty"`
	ContentHash string            `json:"content_hash"`
	Revision    int64             `json:"revision"`
	ObservedAt  time.Time         `json:"observed_at"`
	ValidFrom   time.Time         `json:"valid_from"`
	ValidTo     *time.Time        `json:"valid_to,omitempty"`
	FromLegacy  bool              `json:"from_legacy,omitempty"`
}

// Page 是目标站点一页响应的解析结果。
type Page struct {
	Items      []Record
	NextCursor string
	HasMore    bool
}

// Decision 是一次观测与当前版本冲突判定的结果。
type Decision int

const (
	// Accept 表示观测代表更新的业务事实，应生成新版本。
	Accept Decision = iota
	// RejectStale 表示观测的业务时间不晚于当前版本（倒退或同时间戳的冲突内容）。
	RejectStale
	// RejectMissingTime 表示观测缺少业务时间，而当前版本有更可靠的时间。
	RejectMissingTime
)

func (d Decision) String() string {
	switch d {
	case Accept:
		return "accept"
	case RejectStale:
		return "stale"
	case RejectMissingTime:
		return "missing_time"
	default:
		return "unknown"
	}
}

// DecideUpdate 判定一次内容已变化的观测是否应成为新版本。
//
// 零值时间表示上游没有给出可用的业务时间（缺失或格式非法）。
// 规则：
//   - 双方都有时间：只有严格更新才接受；倒退与同一时间戳的冲突内容都拒绝，
//     保证结果与到达顺序无关。
//   - 观测有时间、当前没有：接受（证据更强）。
//   - 观测没有时间、当前有时间：拒绝（不能用未知时间覆盖已知事实）。
//   - 双方都没有时间：接受，只能按观测顺序排定。
func DecideUpdate(currentUpdatedAt, observedUpdatedAt time.Time) Decision {
	curKnown := !currentUpdatedAt.IsZero()
	obsKnown := !observedUpdatedAt.IsZero()
	switch {
	case obsKnown && curKnown:
		if observedUpdatedAt.After(currentUpdatedAt) {
			return Accept
		}
		return RejectStale
	case obsKnown && !curKnown:
		return Accept
	case !obsKnown && curKnown:
		return RejectMissingTime
	default:
		return Accept
	}
}

// ApplyResult 是一批记录落库后的处理统计。
type ApplyResult struct {
	// Written 是产生新版本的对象数。
	Written int
	// Stale 是内容变化但被冲突规则拒绝的观测数。
	Stale int
	// Unchanged 是内容与当前版本一致、仅刷新可见时间的观测数。
	Unchanged int
}

// Run 是一次采集执行的记录。
//
// 命名注意：数据库列是 items_wrote，JSON 字段是 items_written。
// ItemsWrote 现在统计「新写入的版本数」，而非插入的行数。
// ItemsStale 是收到但被冲突规则拒绝的观测数，
// ItemsUnchanged 是内容与当前版本一致、只刷新了可见时间的观测数。
type Run struct {
	ID             string     `json:"run_id"`
	SiteID         string     `json:"site_id"`
	StartedAt      time.Time  `json:"started_at"`
	FinishedAt     *time.Time `json:"finished_at,omitempty"`
	Status         string     `json:"status"`
	ItemsSeen      int        `json:"items_seen"`
	ItemsWrote     int        `json:"items_written"`
	ItemsStale     int        `json:"items_stale"`
	ItemsUnchanged int        `json:"items_unchanged"`
}

// SiteStats 是单站点的落库统计，供 /stats 与验证脚本使用。
//
// Rows 与 DistinctSource 都是 current 视图的统计。
// 迁移完成后二者应相等（每个来源对象只有一行当前状态）。
// Versions 是历史版本行数。
type SiteStats struct {
	SiteID         string `json:"site_id"`
	Rows           int    `json:"rows"`
	DistinctSource int    `json:"distinct_source_ids"`
	MaxUpdatedAt   string `json:"max_updated_at,omitempty"`
	Versions       int    `json:"versions"`
}

// MigrationInfo 描述当前/历史双写模型的迁移进度。
//
// PendingSites 非空时，这些站点的历史视图尚未建立；
// current 视图仍然可读，行为与迁移前一致。
type MigrationInfo struct {
	CompletedSites int      `json:"completed_sites"`
	PendingSites   []string `json:"pending_sites"`
	TemporalReady  bool     `json:"temporal_reads_ready"`
}

// Stats 是全局统计。
type Stats struct {
	Sites     []SiteStats       `json:"sites"`
	TotalRows int               `json:"total_rows"`
	TotalRuns int               `json:"total_runs"`
	LastRunAt string            `json:"last_run_at,omitempty"`
	Cursors   map[string]string `json:"cursors"`
	Migration MigrationInfo     `json:"migration"`
}

// DuplicateRatio 返回重复率：1 - 去重业务主键数 / 总行数。
func (s SiteStats) DuplicateRatio() float64 {
	if s.Rows == 0 {
		return 0
	}
	return 1 - float64(s.DistinctSource)/float64(s.Rows)
}
