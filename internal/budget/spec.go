// Package budget 实现站点级分布式请求预算治理。
//
// 核心模型：每个站点在共享存储（Redis）上维护同一个令牌桶与在途计数，
// 所有采集实例的长期速率、短时突发、并发在途与重试消耗都从同一份配额中扣减。
// 连续收到上游限流时通过 AIMD（乘性收缩、加性恢复）平稳调节有效速率；
// 共享存储不可用时降级为实例内本地限流，降级语义由 Spec 明确约束。
package budget

import (
	"context"
	"time"
)

// AIMD 与轮询参数。集中常量便于评审与调参。
const (
	// ShrinkFactor 是每次上游限流对有效速率的乘性收缩系数。
	ShrinkFactor = 0.7
	// MinEffect 是有效速率相对配置速率的最低比例。
	MinEffect = 0.1
	// RecoverStep 是每次成功翻页后有效速率的加性恢复步长（相对配置速率）。
	RecoverStep = 0.05
	// DefaultRetryAfter 是上游未给 Retry-After 时使用的冷却时长。
	DefaultRetryAfter = time.Second
	// PollMin / PollMax 是预算暂不可用时客户端的轮询间隔上下限。
	PollMin = 25 * time.Millisecond
	PollMax = time.Second
	// StateTTL 是预算状态与在途计数的兜底过期时间，防止崩溃实例泄漏在途名额。
	StateTTL = 2 * time.Minute
	// ProbeInterval 是降级状态下探测共享存储恢复的间隔。
	ProbeInterval = 2 * time.Second
)

// Mode 是预算治理的工作模式。
type Mode string

const (
	// ModeRedis 表示以共享存储上的分布式配额为准。
	ModeRedis Mode = "redis"
	// ModeFallback 表示共享存储暂时失联，实例按 FallbackFraction 本地限流。
	ModeFallback Mode = "fallback"
	// ModeLocal 表示未配置共享存储，实例独立按全量配额本地限流（历史单机语义）。
	ModeLocal Mode = "local"
)

// AcquireStatus 描述一次预算申请的结果类别，用于运行人员区分现场。
type AcquireStatus string

const (
	// StatusGranted 正常拿到预算。
	StatusGranted AcquireStatus = "granted"
	// StatusQuotaWait 预算暂不足、经历了正常配额等待后拿到。
	StatusQuotaWait AcquireStatus = "quota_wait"
	// StatusFallback 预算后端失联，本次走本地降级配额。
	StatusFallback AcquireStatus = "fallback"
)

// Spec 是一个站点的预算规格（已填充默认值、已校验）。
type Spec struct {
	// Rate 是集群共享的长期可持续速率（请求/秒）。
	Rate float64
	// Burst 是令牌桶容量，约束短时突发。
	Burst int
	// MaxInflight 是集群范围内该站点的并发在途请求上限。
	MaxInflight int
	// MaxAttempts 是单页最大尝试次数（含首次）；1 表示不重试。
	MaxAttempts int
	// FallbackFraction 是共享存储失联时单实例降级速率占全量配额的比例。
	FallbackFraction float64
	// Version 单调递增的配置版本，用于共享状态识别配置变更。
	Version int64
}

// Grant 是一次成功申请的凭证与上下文。
type Grant struct {
	SiteID  string
	Attempt int
	Waited  time.Duration
	Status  AcquireStatus
	// reqID 是该次在途名额在共享存储中的标识，Release 时据此归还。
	reqID string
}

// requestID 暴露在途名额标识，包外不可见。
func (g Grant) requestID() string { return g.reqID }

// Snapshot 是一个站点预算的可观测快照，供管理接口与日志使用。
type Snapshot struct {
	SiteID         string    `json:"site_id"`
	Mode           Mode      `json:"mode"`
	ConfiguredRate float64   `json:"configured_rate"`
	EffectiveRate  float64   `json:"effective_rate"`
	Burst          int       `json:"burst"`
	Tokens         float64   `json:"tokens"`
	Inflight       int       `json:"inflight"`
	MaxInflight    int       `json:"max_inflight"`
	CooldownUntil  time.Time `json:"cooldown_until,omitempty"`
	Acquires       int64     `json:"acquires_total"`
	Waits          int64     `json:"waits_total"`
	WaitDuration   float64   `json:"wait_seconds_total"`
	Limited        int64     `json:"limited_total"`
	Fallback       int64     `json:"fallback_total"`
}

// Governor 是单个站点的预算治理器。
//
// 一次 HTTP 尝试的标准用法：
//
//	grant, err := gov.Acquire(ctx, attempt) // 阻塞直至拿到预算或 ctx 结束
//	if err != nil { return err }
//	resp, err := doHTTP(ctx)
//	gov.Release(ctx)                       // 归还在途名额
//	if limited { gov.MarkLimited(ctx, ra) } else if ok { gov.MarkSuccess(ctx) }
type Governor interface {
	SiteID() string
	Spec() Spec
	// Acquire 阻塞直到同时拿到令牌与在途名额。每次 HTTP 尝试（含重试）都必须申请。
	Acquire(ctx context.Context, attempt int) (Grant, error)
	// Release 归还在途名额，与成功的 Acquire 配对，必须在 HTTP 返回后调用。
	Release(ctx context.Context, grant Grant)
	// MarkLimited 上报一次上游限流（429），驱动乘性收缩与冷却。
	MarkLimited(ctx context.Context, retryAfter time.Duration)
	// MarkSuccess 上报一次成功翻页，驱动加性恢复。
	MarkSuccess(ctx context.Context)
	// Snapshot 读取当前可观测状态。
	Snapshot(ctx context.Context) Snapshot
	// Close 释放后台资源。
	Close() error
}
