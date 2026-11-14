package budget

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// acquireScript 在一次往返内完成「补令牌 + 冷却判定 + 在途判定 + 预占」。
//
// KEYS[1]=状态哈希  KEYS[2]=在途有序集合
// ARGV: nowMs, ttlMs, configuredRate, version, burst, maxInflight, reqID,
//
//	shrinkMinRatio(0.1), stateTTLSec
//
// 返回: {granted(0|1), waitMs, tokens, effectiveRate, inflight, cooldownMs}
var acquireScript = redis.NewScript(`
local now = tonumber(ARGV[1])
local ttlMs = tonumber(ARGV[2])
local rate = tonumber(ARGV[3])
local version = tonumber(ARGV[4])
local burst = tonumber(ARGV[5])
local maxInflight = tonumber(ARGV[6])
local reqID = ARGV[7]
local ttlSec = tonumber(ARGV[8])

local stored = redis.call('HGETALL', KEYS[1])
local st = {}
for i = 1, #stored, 2 do st[stored[i]] = stored[i+1] end

-- 键缺失或配置版本变化：整桶重置为满，有效速率回到配置值。
if (next(st) == nil) or (tonumber(st.version or 0) ~= version) then
  st = {tokens = burst, updated = now, eff = rate, cooldown = 0, version = version}
end

-- 惰性清理崩溃实例遗留的在途条目。
redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', now - 1)

local eff = tonumber(st.eff)
local updated = tonumber(st.updated)
local tokens = tonumber(st.tokens) + math.max(0, now - updated) / 1000.0 * eff
if tokens > burst then tokens = burst end

local cooldown = tonumber(st.cooldown or 0)
local inflight = redis.call('ZCARD', KEYS[2])

local granted = 0
local waitMs = 0
if now >= cooldown and tokens >= 1 and inflight < maxInflight then
  tokens = tokens - 1
  redis.call('ZADD', KEYS[2], now + ttlMs, reqID)
  granted = 1
else
  if now < cooldown then
    waitMs = cooldown - now
  else
    waitMs = math.ceil((1 - tokens) / eff * 1000)
  end
  if waitMs < 25 then waitMs = 25 end
end

redis.call('HSET', KEYS[1], 'tokens', tokens, 'updated', now, 'eff', eff,
           'cooldown', cooldown, 'version', version)
redis.call('EXPIRE', KEYS[1], ttlSec)
redis.call('EXPIRE', KEYS[2], ttlSec)
-- 浮点字段以字符串返回：直接 return Lua number 会被截成整数。
return {granted, waitMs, tostring(tokens), tostring(eff), inflight, cooldown}
`)

// releaseScript 归还在途名额。
//
// KEYS[1]=状态哈希  KEYS[2]=在途有序集合
// ARGV: reqID, ttlSec
var releaseScript = redis.NewScript(`
redis.call('ZREM', KEYS[2], ARGV[1])
redis.call('EXPIRE', KEYS[2], tonumber(ARGV[2]))
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
return redis.call('ZCARD', KEYS[2])
`)

// limitedScript 处理一次上游限流：乘性收缩有效速率并设置冷却截止。
//
// KEYS[1]=状态哈希
// ARGV: nowMs, ttlSec, configuredRate, version, retryAfterMs,
//
//	shrinkFactor, minRatio
var limitedScript = redis.NewScript(`
local now = tonumber(ARGV[1])
local ttlSec = tonumber(ARGV[2])
local rate = tonumber(ARGV[3])
local version = tonumber(ARGV[4])
local retryAfter = tonumber(ARGV[5])
local shrinkFactor = tonumber(ARGV[6])
local minRatio = tonumber(ARGV[7])

local stored = redis.call('HGETALL', KEYS[1])
local st = {}
for i = 1, #stored, 2 do st[stored[i]] = stored[i+1] end
if (next(st) == nil) or (tonumber(st.version or 0) ~= version) then
  st = {tokens = 0, updated = now, eff = rate, cooldown = 0, version = version}
end

local oldEff = tonumber(st.eff)
local newEff = oldEff * shrinkFactor
local floor = rate * minRatio
if newEff < floor then newEff = floor end
local cooldown = math.max(tonumber(st.cooldown or 0), now + retryAfter)

redis.call('HSET', KEYS[1], 'tokens', tonumber(st.tokens), 'updated', now,
           'eff', newEff, 'cooldown', cooldown, 'version', version)
redis.call('EXPIRE', KEYS[1], ttlSec)
return {tostring(newEff), cooldown}
`)

// successScript 处理一次成功翻页：有效速率加性恢复，上限为配置速率。
//
// KEYS[1]=状态哈希
// ARGV: nowMs, ttlSec, configuredRate, version, recoverStep
var successScript = redis.NewScript(`
local now = tonumber(ARGV[1])
local ttlSec = tonumber(ARGV[2])
local rate = tonumber(ARGV[3])
local version = tonumber(ARGV[4])
local step = tonumber(ARGV[5])

local stored = redis.call('HGETALL', KEYS[1])
local st = {}
for i = 1, #stored, 2 do st[stored[i]] = stored[i+1] end
if (next(st) == nil) or (tonumber(st.version or 0) ~= version) then
  st = {tokens = 0, updated = now, eff = rate, cooldown = 0, version = version}
end

local newEff = math.min(rate, tonumber(st.eff) + rate * step)
redis.call('HSET', KEYS[1], 'tokens', tonumber(st.tokens), 'updated', now,
           'eff', newEff, 'cooldown', tonumber(st.cooldown or 0), 'version', version)
redis.call('EXPIRE', KEYS[1], ttlSec)
return tostring(newEff)
`)

// snapshotScript 读取观测快照。
//
// 注意：HGETALL 取回的字段是字符串，Lua 5.1 的 tonumber 会把 "3.57"
// 规整成整数 3，因此这里原样返回字符串，由 Go 侧 ParseFloat 精确保留小数。
//
// KEYS[1]=状态哈希  KEYS[2]=在途有序集合  ARGV: nowMs
var snapshotScript = redis.NewScript(`
redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', tonumber(ARGV[1]) - 1)
local stored = redis.call('HGETALL', KEYS[1])
local st = {}
for i = 1, #stored, 2 do st[stored[i]] = stored[i+1] end
local inflight = redis.call('ZCARD', KEYS[2])
return {st['tokens'] or '0', st['eff'] or '0', st['cooldown'] or '0',
        inflight, st['version'] or '0'}
`)

const scriptTimeout = 2 * time.Second

type redisGovernor struct {
	rdb       *redis.Client
	siteID    string
	workerID  string
	keyState  string
	keyFlight string

	spec atomic.Value // Spec

	rngMu sync.Mutex
	rng   *rand.Rand

	modeMu  sync.Mutex
	mode    Mode
	local   *localLimiter
	probeAt time.Time
	reqSeq  atomic.Uint64

	acquires  atomic.Int64
	waits     atomic.Int64
	waitNanos atomic.Int64
	limited   atomic.Int64
	fallback  atomic.Int64
}

// NewRedisGovernor 构造一个基于共享存储的站点治理器。
func NewRedisGovernor(rdb *redis.Client, workerID, siteID string, spec Spec) Governor {
	g := &redisGovernor{
		rdb:       rdb,
		siteID:    siteID,
		workerID:  workerID,
		keyState:  fmt.Sprintf("aq:budget:%s:state", siteID),
		keyFlight: fmt.Sprintf("aq:budget:%s:inflight", siteID),
		rng:       rand.New(rand.NewSource(time.Now().UnixNano())),
		mode:      ModeRedis,
	}
	g.spec.Store(spec)
	return g
}

// NewLocalGovernor 构造一个纯本地治理器（未配置共享存储时使用，保持单机历史语义）。
func NewLocalGovernor(siteID string, spec Spec) Governor {
	return &localOnlyGovernor{
		siteID:  siteID,
		spec:    spec,
		limiter: newLocalLimiter(spec.Rate, spec.Burst, spec.MaxInflight),
	}
}

func (g *redisGovernor) SiteID() string { return g.siteID }

func (g *redisGovernor) Spec() Spec { return g.spec.Load().(Spec) }

// UpdateSpec 热更新规格。版本变化后，下一次 Acquire 的 Lua 会自动重置共享桶。
func (g *redisGovernor) UpdateSpec(spec Spec) {
	g.spec.Store(spec)
	g.modeMu.Lock()
	if g.local != nil {
		g.local = newLocalLimiter(spec.Rate*spec.FallbackFraction, spec.Burst, spec.MaxInflight)
	}
	g.modeMu.Unlock()
}

func (g *redisGovernor) Close() error { return nil }

func (g *redisGovernor) currentSpec() Spec { return g.spec.Load().(Spec) }

func (g *redisGovernor) callCtx(parent context.Context) (context.Context, context.CancelFunc) {
	deadline, ok := parent.Deadline()
	if ok && time.Until(deadline) < scriptTimeout {
		return context.WithCancel(parent)
	}
	return context.WithTimeout(parent, scriptTimeout)
}

// isInfraError 判断是否为共享存储基础设施故障（需要降级），
// 调用方自身的 ctx 取消不属于基础设施故障。
func isInfraError(ctx context.Context, err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		if ctx.Err() != nil {
			return false
		}
	}
	return true
}

func (g *redisGovernor) jitter(base time.Duration) time.Duration {
	g.rngMu.Lock()
	f := 0.8 + g.rng.Float64()*0.4 // 0.8~1.2，打散多实例同时轮询
	g.rngMu.Unlock()
	d := time.Duration(float64(base) * f)
	if d < PollMin {
		d = PollMin
	}
	if d > PollMax {
		d = PollMax
	}
	return d
}

func (g *redisGovernor) newReqID(attempt int) string {
	seq := g.reqSeq.Add(1)
	return fmt.Sprintf("%s:%s:%d:%d:%s", g.workerID, g.siteID, attempt, seq,
		uuid.NewString()[:8])
}

// Acquire 阻塞直到同时拿到令牌与在途名额。
func (g *redisGovernor) Acquire(ctx context.Context, attempt int) (Grant, error) {
	start := time.Now()
	reqID := g.newReqID(attempt)
	waited := false

	for {
		g.modeMu.Lock()
		mode := g.mode
		probeDue := mode == ModeFallback && time.Now().After(g.probeAt)
		g.modeMu.Unlock()

		if mode == ModeFallback && !probeDue {
			lim := g.local
			if ok, hint := lim.tryAcquire(time.Now()); ok {
				g.fallback.Add(1)
				return g.grant(reqID, attempt, StatusFallback, start, waited), nil
			} else if !sleep(ctx, hint) {
				return Grant{}, ctx.Err()
			}
			waited = true
			continue
		}

		// redis 模式，或 fallback 到期后的探测：都走共享存储申请。
		res, err := g.redisAcquire(ctx, reqID)
		if err != nil {
			if !isInfraError(ctx, err) {
				return Grant{}, ctx.Err()
			}
			g.enterFallback()
			continue
		}
		if mode == ModeFallback {
			g.exitFallback() // 探测成功，恢复分布式治理
		}
		if res.granted {
			g.acquires.Add(1)
			if waited {
				g.waits.Add(1)
				g.waitNanos.Add(time.Since(start).Nanoseconds())
			}
			status := StatusGranted
			if waited {
				status = StatusQuotaWait
			}
			return g.grant(reqID, attempt, status, start, waited), nil
		}
		waited = true
		if !sleep(ctx, g.jitter(time.Duration(res.waitMs)*time.Millisecond)) {
			return Grant{}, ctx.Err()
		}
	}
}

type acquireResult struct {
	granted  bool
	waitMs   int64
	tokens   float64
	eff      float64
	inflight int64
	cooldown int64
}

// luaFloat 兼容 Lua 返回值的多种 Go 表示：int64、float64，
// 以及为保留小数而用 tostring 回传的字符串。
func luaFloat(v any) float64 {
	switch n := v.(type) {
	case int64:
		return float64(n)
	case float64:
		return n
	case string:
		f, _ := strconv.ParseFloat(n, 64)
		return f
	default:
		return 0
	}
}

// luaInt 解析可能以字符串回传的整数字段。
func luaInt(v any) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case string:
		i, _ := strconv.ParseInt(n, 10, 64)
		return i
	default:
		return 0
	}
}

func (g *redisGovernor) redisAcquire(ctx context.Context, reqID string) (acquireResult, error) {
	spec := g.currentSpec()
	now := time.Now()
	callCtx, cancel := g.callCtx(ctx)
	defer cancel()
	vals, err := acquireScript.Run(callCtx, g.rdb,
		[]string{g.keyState, g.keyFlight},
		now.UnixMilli(), int64(StateTTL/time.Millisecond), spec.Rate, spec.Version,
		spec.Burst, spec.MaxInflight, reqID, int64(StateTTL/time.Second),
	).Slice()
	if err != nil {
		return acquireResult{}, err
	}
	return acquireResult{
		granted:  vals[0].(int64) == 1,
		waitMs:   vals[1].(int64),
		tokens:   luaFloat(vals[2]),
		eff:      luaFloat(vals[3]),
		inflight: vals[4].(int64),
		cooldown: vals[5].(int64),
	}, nil
}

func (g *redisGovernor) grant(reqID string, attempt int, status AcquireStatus,
	start time.Time, waited bool) Grant {
	return Grant{
		SiteID:  g.siteID,
		Attempt: attempt,
		Waited:  time.Since(start),
		Status:  status,
		reqID:   reqID,
	}
}

// Release 按授予时的模式归还在途名额。
func (g *redisGovernor) Release(ctx context.Context, grant Grant) {
	if grant.Status == StatusFallback {
		g.modeMu.Lock()
		if g.local != nil {
			g.local.release()
		}
		g.modeMu.Unlock()
		return
	}
	callCtx, cancel := g.callCtx(ctx)
	defer cancel()
	if err := releaseScript.Run(callCtx, g.rdb,
		[]string{g.keyState, g.keyFlight}, grant.requestID(), int64(StateTTL/time.Second),
	).Err(); err != nil && isInfraError(ctx, err) {
		// 归还失败不影响请求正确性：条目有 TTL 兜底，转入降级即可。
		g.enterFallback()
	}
}

// MarkLimited 上报上游限流：乘性收缩 + 冷却。
func (g *redisGovernor) MarkLimited(ctx context.Context, retryAfter time.Duration) {
	g.limited.Add(1)
	if retryAfter <= 0 {
		retryAfter = DefaultRetryAfter
	}
	spec := g.currentSpec()

	g.modeMu.Lock()
	mode := g.mode
	g.modeMu.Unlock()
	if mode == ModeFallback {
		g.local.shrink(ShrinkFactor, spec.Rate*MinEffect*spec.FallbackFraction)
		return
	}

	callCtx, cancel := g.callCtx(ctx)
	defer cancel()
	err := limitedScript.Run(callCtx, g.rdb, []string{g.keyState},
		time.Now().UnixMilli(), int64(StateTTL/time.Second), spec.Rate, spec.Version,
		retryAfter.Milliseconds(), ShrinkFactor, MinEffect,
	).Err()
	if err != nil && isInfraError(ctx, err) {
		g.enterFallback()
		g.local.shrink(ShrinkFactor, spec.Rate*MinEffect*spec.FallbackFraction)
	}
}

// MarkSuccess 上报成功翻页：加性恢复。
func (g *redisGovernor) MarkSuccess(ctx context.Context) {
	spec := g.currentSpec()
	g.modeMu.Lock()
	mode := g.mode
	g.modeMu.Unlock()
	if mode == ModeFallback {
		g.local.recover(RecoverStep, spec.Rate*spec.FallbackFraction)
		return
	}
	callCtx, cancel := g.callCtx(ctx)
	defer cancel()
	err := successScript.Run(callCtx, g.rdb, []string{g.keyState},
		time.Now().UnixMilli(), int64(StateTTL/time.Second), spec.Rate, spec.Version,
		RecoverStep,
	).Err()
	if err != nil && isInfraError(ctx, err) {
		g.enterFallback()
		g.local.recover(RecoverStep, spec.Rate*spec.FallbackFraction)
	}
}

func (g *redisGovernor) enterFallback() {
	g.modeMu.Lock()
	defer g.modeMu.Unlock()
	if g.mode != ModeFallback {
		spec := g.currentSpec()
		g.local = newLocalLimiter(spec.Rate*spec.FallbackFraction, spec.Burst, spec.MaxInflight)
		g.mode = ModeFallback
	}
	g.probeAt = time.Now().Add(ProbeInterval)
}

func (g *redisGovernor) exitFallback() {
	g.modeMu.Lock()
	g.mode = ModeRedis
	g.local = nil
	g.modeMu.Unlock()
}

// Snapshot 返回观测快照；共享存储不可用时返回本地视角，不返回错误。
func (g *redisGovernor) Snapshot(ctx context.Context) Snapshot {
	spec := g.currentSpec()
	snap := Snapshot{
		SiteID:         g.siteID,
		ConfiguredRate: spec.Rate,
		Burst:          spec.Burst,
		MaxInflight:    spec.MaxInflight,
		Acquires:       g.acquires.Load(),
		Waits:          g.waits.Load(),
		WaitDuration:   float64(g.waitNanos.Load()) / float64(time.Second),
		Limited:        g.limited.Load(),
		Fallback:       g.fallback.Load(),
	}

	g.modeMu.Lock()
	mode := g.mode
	local := g.local
	g.modeMu.Unlock()

	if mode == ModeFallback && local != nil {
		snap.Mode = ModeFallback
		snap.EffectiveRate, snap.Tokens, snap.Inflight = local.view()
		return snap
	}

	snap.Mode = ModeRedis
	callCtx, cancel := g.callCtx(ctx)
	defer cancel()
	vals, err := snapshotScript.Run(callCtx, g.rdb,
		[]string{g.keyState, g.keyFlight}, time.Now().UnixMilli()).Slice()
	if err != nil {
		// 读不出来时至少给出模式与计数，避免观测面整体失败。
		snap.Mode = ModeFallback
		snap.EffectiveRate = spec.Rate
		return snap
	}
	snap.Tokens = luaFloat(vals[0])
	snap.EffectiveRate = luaFloat(vals[1])
	cooldownMs := luaInt(vals[2])
	snap.Inflight = int(luaInt(vals[3]))
	if cooldownMs > 0 {
		snap.CooldownUntil = time.UnixMilli(cooldownMs).UTC()
	}
	return snap
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// localOnlyGovernor 用于未配置共享存储的部署：全量配额的纯本地限流，
// 不做 AIMD（保持特性引入前的单机语义）。
type localOnlyGovernor struct {
	siteID  string
	spec    Spec
	limiter *localLimiter

	acquires  atomic.Int64
	waits     atomic.Int64
	waitNanos atomic.Int64
}

func (l *localOnlyGovernor) SiteID() string { return l.siteID }
func (l *localOnlyGovernor) Spec() Spec     { return l.spec }
func (l *localOnlyGovernor) Close() error   { return nil }

func (l *localOnlyGovernor) Acquire(ctx context.Context, attempt int) (Grant, error) {
	start := time.Now()
	if err := l.limiter.Acquire(ctx); err != nil {
		return Grant{}, err
	}
	l.acquires.Add(1)
	waited := time.Since(start)
	if waited > 0 {
		l.waits.Add(1)
		l.waitNanos.Add(waited.Nanoseconds())
	}
	return Grant{SiteID: l.siteID, Attempt: attempt, Waited: waited, Status: StatusGranted}, nil
}

func (l *localOnlyGovernor) Release(context.Context, Grant) {
	l.limiter.release()
}
func (l *localOnlyGovernor) MarkLimited(context.Context, time.Duration) {}
func (l *localOnlyGovernor) MarkSuccess(context.Context)                {}

func (l *localOnlyGovernor) Snapshot(context.Context) Snapshot {
	rate, tokens, inflight := l.limiter.view()
	return Snapshot{
		SiteID:         l.siteID,
		Mode:           ModeLocal,
		ConfiguredRate: l.spec.Rate,
		EffectiveRate:  rate,
		Burst:          l.spec.Burst,
		Tokens:         tokens,
		Inflight:       inflight,
		MaxInflight:    l.spec.MaxInflight,
		Acquires:       l.acquires.Load(),
		Waits:          l.waits.Load(),
		WaitDuration:   float64(l.waitNanos.Load()) / float64(time.Second),
	}
}
