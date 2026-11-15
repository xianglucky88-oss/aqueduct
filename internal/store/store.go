package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"time"

	"github.com/lib/pq"

	"github.com/internal-platform/aqueduct/internal/model"
)

// ErrLeaseLost 表示本次写入没有通过围栏校验：租约已过期、已被其他副本
// 接管，或本进程从未持有该租约。调用方必须立即停止写入本站点。
var ErrLeaseLost = errors.New("lease lost")

// migrateLockKey 是 Migrate 使用的 PostgreSQL advisory lock 键。多副本
// 同时启动时用它串行化 DDL 与一次性数据迁移。
const migrateLockKey int64 = 0x6171756564756374

// Store 封装 PostgreSQL 读写。
type Store struct {
	db *sql.DB
}

// Open 连接数据库并在 ctx 截止前等待其可用。
//
// 连接池上限随副本数一起规划：副本数 * MaxOpenConns 必须小于数据库的
// max_connections 留出余量，否则滚动升级期间新老副本并存会打满连接。
func Open(ctx context.Context, dsn string, maxOpen, maxIdle int) (*Store, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}
	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxIdle)
	db.SetConnMaxLifetime(5 * time.Minute)
	db.SetConnMaxIdleTime(1 * time.Minute)

	var pingErr error
	for {
		pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		pingErr = db.PingContext(pingCtx)
		cancel()
		if pingErr == nil {
			return &Store{db: db}, nil
		}
		select {
		case <-time.After(time.Second):
		case <-ctx.Done():
			db.Close()
			return nil, fmt.Errorf("db not ready: %w (last ping: %v)", ctx.Err(), pingErr)
		}
	}
}

// Close 关闭连接池。
func (s *Store) Close() error { return s.db.Close() }

// DB 暴露底层连接，供只读管理查询使用。
func (s *Store) DB() *sql.DB { return s.db }

// Ping 用于就绪探针。
func (s *Store) Ping(ctx context.Context) error { return s.db.PingContext(ctx) }

// Migrate 建表并执行一次性数据迁移。幂等，可被多副本并发调用。
//
// 一次性步骤 stable_record_keys 把历史的「内容参与哈希」主键改写为
// 「站点 + 业务主键」的稳定主键，并清理同一业务主键的历史重复行。
// records 的数据可以从上游重新采集，因此保留最新一行是安全的。
func (s *Store) Migrate(ctx context.Context) error {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("migrate: acquire conn: %w", err)
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, `SELECT pg_advisory_lock($1)`, migrateLockKey); err != nil {
		return fmt.Errorf("migrate: advisory lock: %w", err)
	}
	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := conn.ExecContext(unlockCtx, `SELECT pg_advisory_unlock($1)`, migrateLockKey); err != nil {
			log.Printf("migrate: advisory unlock: %v", err)
		}
	}()

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("migrate: begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	stmts := []string{
		`CREATE TABLE IF NOT EXISTS records (
			item_key     TEXT PRIMARY KEY,
			site_id      TEXT NOT NULL,
			source_id    TEXT NOT NULL,
			title        TEXT NOT NULL DEFAULT '',
			price_cents  BIGINT NOT NULL DEFAULT 0,
			stock        INTEGER NOT NULL DEFAULT 0,
			updated_at   TIMESTAMPTZ NOT NULL,
			payload      JSONB NOT NULL DEFAULT '{}'::jsonb,
			collected_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
		`CREATE INDEX IF NOT EXISTS records_site_idx ON records (site_id)`,
		`CREATE INDEX IF NOT EXISTS records_source_idx ON records (site_id, source_id)`,
		`CREATE TABLE IF NOT EXISTS runs (
			run_id      TEXT PRIMARY KEY,
			site_id     TEXT NOT NULL,
			started_at  TIMESTAMPTZ NOT NULL,
			finished_at TIMESTAMPTZ,
			status      TEXT NOT NULL,
			items_seen  INTEGER NOT NULL DEFAULT 0,
			items_wrote INTEGER NOT NULL DEFAULT 0
		)`,
		`ALTER TABLE runs ADD COLUMN IF NOT EXISTS worker_id TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE runs ADD COLUMN IF NOT EXISTS last_error TEXT NOT NULL DEFAULT ''`,
		`CREATE INDEX IF NOT EXISTS runs_site_started_idx ON runs (site_id, started_at DESC)`,
		`CREATE INDEX IF NOT EXISTS runs_running_idx ON runs (site_id) WHERE status = 'running'`,
		`CREATE TABLE IF NOT EXISTS cursors (
			site_id     TEXT PRIMARY KEY,
			cursor      TEXT NOT NULL DEFAULT '',
			updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
		`CREATE TABLE IF NOT EXISTS leases (
			site_id     TEXT PRIMARY KEY,
			worker_id   TEXT NOT NULL,
			expires_at  TIMESTAMPTZ NOT NULL
		)`,
		// fence 是单调递增的围栏令牌：每次接管 +1。租约行永不删除，
		// 释放只是把 expires_at 置为 now()，以避免令牌回绕。
		`ALTER TABLE leases ADD COLUMN IF NOT EXISTS fence BIGINT NOT NULL DEFAULT 0`,
		`ALTER TABLE leases ADD COLUMN IF NOT EXISTS acquired_at TIMESTAMPTZ NOT NULL DEFAULT now()`,
		`CREATE TABLE IF NOT EXISTS site_runtime (
			site_id              TEXT PRIMARY KEY,
			next_run_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
			last_success_at      TIMESTAMPTZ,
			consecutive_failures INTEGER NOT NULL DEFAULT 0,
			last_status          TEXT NOT NULL DEFAULT '',
			last_error           TEXT NOT NULL DEFAULT '',
			updated_at           TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
		`CREATE TABLE IF NOT EXISTS schema_steps (
			name       TEXT PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
	}
	for _, stmt := range stmts {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("migrate: %w\n%s", err, stmt)
		}
	}

	if err := migrateStableKeys(ctx, tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("migrate: commit: %w", err)
	}
	return nil
}

// migrateStableKeys 是一次性迁移：去重 + 重算 item_key + 加唯一约束。
func migrateStableKeys(ctx context.Context, tx *sql.Tx) error {
	var applied bool
	err := tx.QueryRowContext(ctx,
		`SELECT EXISTS (SELECT 1 FROM schema_steps WHERE name = 'stable_record_keys')`).Scan(&applied)
	if err != nil {
		return fmt.Errorf("migrate: check step: %w", err)
	}
	if applied {
		return nil
	}

	log.Printf("migrate: applying one-time step stable_record_keys (dedupe + rekey)")
	steps := []string{
		`DELETE FROM records a
		 USING records b
		 WHERE a.site_id = b.site_id
		   AND a.source_id = b.source_id
		   AND (a.updated_at, a.item_key) < (b.updated_at, b.item_key)`,
		`UPDATE records
		 SET item_key = encode(substring(sha256(convert_to(site_id || '|' || source_id, 'UTF8')), 1, 8), 'hex')`,
		`CREATE UNIQUE INDEX IF NOT EXISTS records_site_source_uidx ON records (site_id, source_id)`,
		`INSERT INTO schema_steps (name) VALUES ('stable_record_keys') ON CONFLICT DO NOTHING`,
	}
	for _, stmt := range steps {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("migrate step stable_record_keys: %w\n%s", err, stmt)
		}
	}
	return nil
}

// Upsert 批量写记录，供单轮采集（-mode=once）与测试使用。
func (s *Store) Upsert(ctx context.Context, recs []model.Record) (int, error) {
	return upsert(ctx, s.db, recs)
}

type execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// upsert 以稳定 item_key 为冲突键。内容变化时原地更新，不产生新行。
func upsert(ctx context.Context, ex execer, recs []model.Record) (int, error) {
	if len(recs) == 0 {
		return 0, nil
	}
	const cols = 8
	var b strings.Builder
	b.WriteString(`INSERT INTO records (item_key, site_id, source_id, title, price_cents, stock, updated_at, payload) VALUES `)
	args := make([]any, 0, len(recs)*cols)
	for i, r := range recs {
		if i > 0 {
			b.WriteString(",")
		}
		base := i * cols
		fmt.Fprintf(&b, "($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)",
			base+1, base+2, base+3, base+4, base+5, base+6, base+7, base+8)
		args = append(args, r.ItemKey, r.SiteID, r.SourceID, r.Title,
			r.PriceCents, r.Stock, r.UpdatedAt, toJSON(r.Payload))
	}
	b.WriteString(` ON CONFLICT (site_id, source_id) DO UPDATE SET
		item_key = EXCLUDED.item_key,
		title = EXCLUDED.title,
		price_cents = EXCLUDED.price_cents,
		stock = EXCLUDED.stock,
		updated_at = EXCLUDED.updated_at,
		payload = EXCLUDED.payload,
		collected_at = now()`)

	res, err := ex.ExecContext(ctx, b.String(), args...)
	if err != nil {
		return 0, fmt.Errorf("upsert: %w", err)
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

func toJSON(m map[string]string) string {
	if len(m) == 0 {
		return "{}"
	}
	var b strings.Builder
	b.WriteString("{")
	first := true
	for k, v := range m {
		if !first {
			b.WriteString(",")
		}
		first = false
		fmt.Fprintf(&b, "%q:%q", k, v)
	}
	b.WriteString("}")
	return b.String()
}

// Lease 是一次站点租约的持有凭证。
//
// Fence 必须随每次写入一起提交：数据库在本事务内校验 worker_id、fence 与
// 有效期，三者任一不匹配即拒绝写入。这样即使旧副本在网络分区后「复活」，
// 它的写入也无法落到库中。
type Lease struct {
	SiteID   string
	WorkerID string
	Fence    int64
	Expires  time.Time
}

func intervalArg(d time.Duration) string {
	ms := d.Milliseconds()
	if ms < 1 {
		ms = 1
	}
	return fmt.Sprintf("%d milliseconds", ms)
}

// AcquireLease 尝试为站点获取租约。
//
// 返回 (nil, nil) 表示当前不需要/不能由本副本执行：租约仍被他人持有，
// 或（force=false 时）站点尚未到 next_run_at。返回错误仅代表数据库故障。
//
// 获取成功时同时把 next_run_at 推到 now+ttl，作为「本副本正在处理」的
// 占位，避免其他副本在整轮采集期间每秒重复尝试抢租约。
func (s *Store) AcquireLease(ctx context.Context, siteID, workerID string, ttl time.Duration, force bool) (*Lease, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("acquire lease %s: %w", siteID, err)
	}
	defer func() { _ = tx.Rollback() }()

	var fence int64
	var expires time.Time
	err = tx.QueryRowContext(ctx, `
		INSERT INTO leases (site_id, worker_id, expires_at, fence, acquired_at)
		SELECT $1, $2, now() + $3::interval, 1, now()
		WHERE $4 OR EXISTS (
			SELECT 1 FROM site_runtime WHERE site_id = $1 AND next_run_at <= now()
		)
		ON CONFLICT (site_id) DO UPDATE
		SET worker_id = EXCLUDED.worker_id,
		    expires_at = EXCLUDED.expires_at,
		    fence = leases.fence + 1,
		    acquired_at = now()
		WHERE leases.expires_at < now()
		RETURNING fence, expires_at`,
		siteID, workerID, intervalArg(ttl), force).Scan(&fence, &expires)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("acquire lease %s: %w", siteID, err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO site_runtime (site_id, next_run_at, updated_at)
		VALUES ($1, now() + $2::interval, now())
		ON CONFLICT (site_id) DO UPDATE SET next_run_at = EXCLUDED.next_run_at, updated_at = now()`,
		siteID, intervalArg(ttl)); err != nil {
		return nil, fmt.Errorf("acquire lease %s: hold schedule: %w", siteID, err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("acquire lease %s: commit: %w", siteID, err)
	}
	return &Lease{SiteID: siteID, WorkerID: workerID, Fence: fence, Expires: expires}, nil
}

// RenewLease 续租。返回 false 表示租约已不属于本副本（已被接管或过期）。
func (s *Store) RenewLease(ctx context.Context, lease Lease, ttl time.Duration) (bool, error) {
	res, err := s.db.ExecContext(ctx, `
		UPDATE leases
		SET expires_at = now() + $4::interval
		WHERE site_id = $1 AND worker_id = $2 AND fence = $3 AND expires_at > now()`,
		lease.SiteID, lease.WorkerID, lease.Fence, intervalArg(ttl))
	if err != nil {
		return false, fmt.Errorf("renew lease %s: %w", lease.SiteID, err)
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// ReleaseLease 主动放弃租约，使其他副本可以立即接管。
func (s *Store) ReleaseLease(ctx context.Context, lease Lease) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE leases SET expires_at = now()
		WHERE site_id = $1 AND worker_id = $2 AND fence = $3`,
		lease.SiteID, lease.WorkerID, lease.Fence)
	return err
}

// checkLeaseLocked 在事务内锁定并校验租约。必须最先执行，以串行化同站点写入。
func checkLeaseLocked(ctx context.Context, tx *sql.Tx, lease Lease) error {
	var worker string
	var fence int64
	var valid bool
	err := tx.QueryRowContext(ctx, `
		SELECT worker_id, fence, expires_at > now()
		FROM leases WHERE site_id = $1 FOR UPDATE`, lease.SiteID).
		Scan(&worker, &fence, &valid)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrLeaseLost
	}
	if err != nil {
		return err
	}
	if worker != lease.WorkerID || fence != lease.Fence || !valid {
		return ErrLeaseLost
	}
	return nil
}

// BeginSiteRun 在租约保护下开启一次运行。
//
// 同一站点不可能同时存在两个合法运行：拿到租约意味着上一个持有者要么已
// 正常结束、要么已过期。此时把遗留的 running 运行标记为 abandoned，
// 使 /runs 不会永久停留在「未结束」状态。
func (s *Store) BeginSiteRun(ctx context.Context, lease Lease, run model.Run) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin run: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if err := checkLeaseLocked(ctx, tx, lease); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE runs SET status = $2, finished_at = now(),
			last_error = 'superseded: lease taken over after this run stopped heartbeating'
		WHERE site_id = $1 AND status = 'running' AND run_id <> $3`,
		lease.SiteID, model.RunStatusAbandoned, run.ID); err != nil {
		return fmt.Errorf("abandon stale runs: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO runs (run_id, site_id, worker_id, started_at, status)
		VALUES ($1,$2,$3,$4,$5)`,
		run.ID, run.SiteID, run.WorkerID, run.StartedAt, model.RunStatusRunning); err != nil {
		return fmt.Errorf("insert run: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("begin run commit: %w", err)
	}
	return nil
}

// CommitPage 原子地提交一页：写入记录、推进游标、累加运行计数。
//
// 四件事必须在同一事务内完成，否则进程在页中途退出会留下「数据已写、
// 游标未动」或相反的状态。租约围栏在同一事务里校验，失去租约则整体回滚。
func (s *Store) CommitPage(ctx context.Context, lease Lease, runID string, recs []model.Record, cursor string) (int, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("commit page: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if err := checkLeaseLocked(ctx, tx, lease); err != nil {
		return 0, err
	}
	written, err := upsert(ctx, tx, recs)
	if err != nil {
		return 0, err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO cursors (site_id, cursor, updated_at) VALUES ($1,$2,now())
		ON CONFLICT (site_id) DO UPDATE SET cursor = EXCLUDED.cursor, updated_at = now()`,
		lease.SiteID, cursor); err != nil {
		return 0, fmt.Errorf("save cursor: %w", err)
	}
	res, err := tx.ExecContext(ctx, `
		UPDATE runs SET items_seen = items_seen + $2, items_wrote = items_wrote + $3
		WHERE run_id = $1 AND status = 'running'`, runID, len(recs), written)
	if err != nil {
		return 0, fmt.Errorf("update run counters: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return 0, fmt.Errorf("run %s is not running: %w", runID, ErrLeaseLost)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit page: %w", err)
	}
	return written, nil
}

// ResetCursor 在租约保护下把站点游标重置为空，用于上游判定游标失效时。
func (s *Store) ResetCursor(ctx context.Context, lease Lease) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err := checkLeaseLocked(ctx, tx, lease); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO cursors (site_id, cursor, updated_at) VALUES ($1,'',now())
		ON CONFLICT (site_id) DO UPDATE SET cursor = '', updated_at = now()`,
		lease.SiteID); err != nil {
		return err
	}
	return tx.Commit()
}

// FinishRun 结束一次运行。计数以 CommitPage 累加的库内值为准，这里不改写。
// 使用独立 context，确保进程终止时也能落库。
func (s *Store) FinishRun(ctx context.Context, runID, status, lastErr string) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE runs SET finished_at = now(), status = $2, last_error = $3
		WHERE run_id = $1 AND status = 'running'`, runID, status, lastErr)
	return err
}

// RunCounters 读回某次运行已提交的计数。
func (s *Store) RunCounters(ctx context.Context, runID string, seen, wrote *int) error {
	return s.db.QueryRowContext(ctx,
		`SELECT items_seen, items_wrote FROM runs WHERE run_id = $1`, runID).Scan(seen, wrote)
}

// FinishSite 在租约保护下释放站点并安排下一次运行。
//
// 成功：next_run_at = now + interval。
// 放弃（终止/接管）：next_run_at = now，让其他副本立即接手，避免遗漏。
// 失败：按连续失败次数指数退避，尊重上游 Retry-After，并设置上限。
// 若租约已丢失，不修改 site_runtime（新持有者才是权威）。
func (s *Store) FinishSite(ctx context.Context, lease Lease, opts FinishOptions) (time.Duration, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()

	if err := checkLeaseLocked(ctx, tx, lease); err != nil {
		return 0, err
	}

	var failures int
	err = tx.QueryRowContext(ctx,
		`SELECT consecutive_failures FROM site_runtime WHERE site_id = $1 FOR UPDATE`,
		lease.SiteID).Scan(&failures)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("read site runtime: %w", err)
	}

	now := opts.Now
	if now.IsZero() {
		now = time.Now()
	}
	next := now
	switch opts.Status {
	case model.RunStatusSucceeded:
		next = now.Add(opts.Interval)
	case model.RunStatusAbandoned:
		// 终止不是故障：保持立即到期，由其他副本接管。
		next = now
	default:
		failures++
		delay := BackoffDelay(failures, opts.BackoffBase, opts.BackoffMax)
		if opts.RetryAfter > delay {
			delay = opts.RetryAfter
		}
		if delay > opts.BackoffMax {
			delay = opts.BackoffMax
		}
		next = now.Add(delay)
	}

	// 先释放租约（fence 不变，expires_at 置 now），其他副本可立即接管。
	res, err := tx.ExecContext(ctx, `
		UPDATE leases SET expires_at = now()
		WHERE site_id = $1 AND worker_id = $2 AND fence = $3 AND expires_at > now()`,
		lease.SiteID, lease.WorkerID, lease.Fence)
	if err != nil {
		return 0, fmt.Errorf("release lease: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return 0, ErrLeaseLost
	}

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO site_runtime
			(site_id, next_run_at, last_success_at, consecutive_failures, last_status, last_error, updated_at)
		VALUES ($1,$2,
			CASE WHEN $3 = 'succeeded' THEN now() END,
			CASE WHEN $3 = 'failed' THEN 1 ELSE 0 END,
			$3,$4,now())
		ON CONFLICT (site_id) DO UPDATE SET
			next_run_at = EXCLUDED.next_run_at,
			last_success_at = CASE WHEN EXCLUDED.last_status = 'succeeded'
				THEN now() ELSE site_runtime.last_success_at END,
			consecutive_failures = CASE
				WHEN EXCLUDED.last_status = 'failed' THEN site_runtime.consecutive_failures + 1
				WHEN EXCLUDED.last_status = 'succeeded' THEN 0
				ELSE site_runtime.consecutive_failures END,
			last_status = EXCLUDED.last_status,
			last_error = EXCLUDED.last_error,
			updated_at = now()`,
		lease.SiteID, next, opts.Status, opts.LastError); err != nil {
		return 0, fmt.Errorf("update site runtime: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return next.Sub(now), nil
}

// FinishOptions 描述一次站点运行的收尾参数。
type FinishOptions struct {
	Status      string
	Interval    time.Duration
	BackoffBase time.Duration
	BackoffMax  time.Duration
	RetryAfter  time.Duration
	LastError   string
	Now         time.Time
}

// BackoffDelay 返回第 failures 次连续失败后的退避时长，带 ±20% 抖动，
// 避免多个副本在同一时刻一起重试。
func BackoffDelay(failures int, base, max time.Duration) time.Duration {
	if failures < 1 {
		failures = 1
	}
	d := base
	for i := 1; i < failures; i++ {
		if d >= max {
			break
		}
		d *= 2
	}
	if d > max {
		d = max
	}
	if d <= 0 {
		return time.Second
	}
	jitter := time.Duration(float64(d) * (rand.Float64()*0.4 - 0.2))
	d += jitter
	if d < time.Second {
		d = time.Second
	}
	if d > max {
		d = max
	}
	return d
}

// EnsureSites 为启用的站点建立调度行。新站点立即到期。
func (s *Store) EnsureSites(ctx context.Context, siteIDs []string) error {
	if len(siteIDs) == 0 {
		return nil
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO site_runtime (site_id, next_run_at, updated_at)
		SELECT unnest($1::text[]), now(), now()
		ON CONFLICT (site_id) DO NOTHING`, pq.Array(siteIDs))
	if err != nil {
		return fmt.Errorf("ensure sites: %w", err)
	}
	return nil
}

// DueSites 返回当前到期且无人持有的站点，减少每个副本每轮不必要的
// 租约竞争。租约未过期的站点即使 next_run_at 已过（长轮次仍在进行）
// 也不会被列出。
func (s *Store) DueSites(ctx context.Context, siteIDs []string) ([]string, error) {
	if len(siteIDs) == 0 {
		return nil, nil
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT r.site_id FROM site_runtime r
		WHERE r.site_id = ANY($1)
		  AND r.next_run_at <= now()
		  AND NOT EXISTS (
			SELECT 1 FROM leases l WHERE l.site_id = r.site_id AND l.expires_at > now()
		  )
		ORDER BY r.next_run_at`, pq.Array(siteIDs))
	if err != nil {
		return nil, fmt.Errorf("due sites: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// Cursor 读取站点游标。不存在时返回空串。
func (s *Store) Cursor(ctx context.Context, siteID string) (string, error) {
	var cur string
	err := s.db.QueryRowContext(ctx,
		`SELECT cursor FROM cursors WHERE site_id = $1`, siteID).Scan(&cur)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return cur, err
}

// RecentRuns 返回最近的运行记录。
func (s *Store) RecentRuns(ctx context.Context, limit int) ([]model.Run, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT run_id, site_id, worker_id, started_at, finished_at, status,
		       items_seen, items_wrote, last_error
		FROM runs ORDER BY started_at DESC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []model.Run{}
	for rows.Next() {
		var run model.Run
		var fin *time.Time
		if err := rows.Scan(&run.ID, &run.SiteID, &run.WorkerID, &run.StartedAt, &fin,
			&run.Status, &run.ItemsSeen, &run.ItemsWrote, &run.LastError); err != nil {
			return nil, err
		}
		run.FinishedAt = fin
		out = append(out, run)
	}
	return out, rows.Err()
}

// LeaseInfo 是当前租约的只读视图。
type LeaseInfo struct {
	SiteID    string    `json:"site_id"`
	WorkerID  string    `json:"worker_id"`
	Fence     int64     `json:"fence"`
	ExpiresAt time.Time `json:"expires_at"`
	Held      bool      `json:"held"`
}

// Leases 返回全部站点的租约状态。
func (s *Store) Leases(ctx context.Context) ([]LeaseInfo, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT site_id, worker_id, fence, expires_at, expires_at > now()
		FROM leases ORDER BY site_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []LeaseInfo{}
	for rows.Next() {
		var li LeaseInfo
		if err := rows.Scan(&li.SiteID, &li.WorkerID, &li.Fence, &li.ExpiresAt, &li.Held); err != nil {
			return nil, err
		}
		out = append(out, li)
	}
	return out, rows.Err()
}

// SiteRuntime 是站点调度状态的只读视图。
type SiteRuntime struct {
	SiteID              string     `json:"site_id"`
	NextRunAt           time.Time  `json:"next_run_at"`
	LastSuccessAt       *time.Time `json:"last_success_at,omitempty"`
	ConsecutiveFailures int        `json:"consecutive_failures"`
	LastStatus          string     `json:"last_status"`
	LastError           string     `json:"last_error,omitempty"`
}

// SiteRuntimes 返回全部站点的调度状态。
func (s *Store) SiteRuntimes(ctx context.Context) ([]SiteRuntime, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT site_id, next_run_at, last_success_at, consecutive_failures, last_status, last_error
		FROM site_runtime ORDER BY site_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []SiteRuntime{}
	for rows.Next() {
		var r SiteRuntime
		if err := rows.Scan(&r.SiteID, &r.NextRunAt, &r.LastSuccessAt,
			&r.ConsecutiveFailures, &r.LastStatus, &r.LastError); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// Stats 汇总各站点落库情况。
func (s *Store) Stats(ctx context.Context) (*model.Stats, error) {
	st := &model.Stats{Cursors: map[string]string{}}

	rows, err := s.db.QueryContext(ctx, `
		SELECT site_id, count(*) AS rows, count(DISTINCT source_id) AS distinct_source,
		       coalesce(max(updated_at)::text, '') AS max_updated
		FROM records GROUP BY site_id ORDER BY site_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var ss model.SiteStats
		if err := rows.Scan(&ss.SiteID, &ss.Rows, &ss.DistinctSource, &ss.MaxUpdatedAt); err != nil {
			return nil, err
		}
		st.Sites = append(st.Sites, ss)
		st.TotalRows += ss.Rows
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM runs`).Scan(&st.TotalRuns); err != nil {
		return nil, err
	}
	var last sql.NullString
	if err := s.db.QueryRowContext(ctx, `SELECT max(finished_at)::text FROM runs`).Scan(&last); err != nil {
		return nil, err
	}
	st.LastRunAt = last.String

	curRows, err := s.db.QueryContext(ctx, `SELECT site_id, cursor FROM cursors`)
	if err != nil {
		return nil, err
	}
	defer curRows.Close()
	for curRows.Next() {
		var site, cur string
		if err := curRows.Scan(&site, &cur); err != nil {
			return nil, err
		}
		st.Cursors[site] = cur
	}
	return st, curRows.Err()
}
