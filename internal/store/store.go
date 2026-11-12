package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/lib/pq"

	"github.com/internal-platform/aqueduct/internal/model"
)

// Store 封装 PostgreSQL 读写。
type Store struct {
	db *sql.DB
}

// Open 连接数据库并等待可用。
func Open(ctx context.Context, dsn string) (*Store, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}
	db.SetMaxOpenConns(16)
	db.SetMaxIdleConns(4)
	db.SetConnMaxLifetime(30 * time.Minute)

	var lastErr error
	for i := 0; i < 30; i++ {
		if err := db.PingContext(ctx); err == nil {
			return &Store{db: db}, nil
		} else {
			lastErr = err
		}
		select {
		case <-time.After(time.Second):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return nil, fmt.Errorf("db not ready: %w", lastErr)
}

// Close 关闭连接池。
func (s *Store) Close() error { return s.db.Close() }

// DB 暴露底层连接，供 API 层做只读查询。
func (s *Store) DB() *sql.DB { return s.db }

// Migrate 建表。幂等。
func (s *Store) Migrate(ctx context.Context) error {
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
	}
	for _, stmt := range stmts {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("migrate: %w\n%s", err, stmt)
		}
	}
	return nil
}

// Upsert 批量写记录。
//
// 当前实现使用 ON CONFLICT DO NOTHING：主键冲突即忽略。
// 由于 ItemKey 含可变字段，内容变化会产生新行。
func (s *Store) Upsert(ctx context.Context, recs []model.Record) (int, error) {
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
	b.WriteString(` ON CONFLICT (item_key) DO NOTHING`)

	res, err := s.db.ExecContext(ctx, b.String(), args...)
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

// StartRun 插入一条运行记录。
func (s *Store) StartRun(ctx context.Context, run model.Run) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO runs (run_id, site_id, started_at, status) VALUES ($1,$2,$3,$4)`,
		run.ID, run.SiteID, run.StartedAt, run.Status)
	return err
}

// FinishRun 更新运行记录。
func (s *Store) FinishRun(ctx context.Context, runID, status string, seen, wrote int) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE runs SET finished_at = now(), status = $2, items_seen = $3, items_wrote = $4 WHERE run_id = $1`,
		runID, status, seen, wrote)
	return err
}

// Cursor 读取站点游标。不存在时返回空串。
func (s *Store) Cursor(ctx context.Context, siteID string) (string, error) {
	var cur string
	err := s.db.QueryRowContext(ctx,
		`SELECT cursor FROM cursors WHERE site_id = $1`, siteID).Scan(&cur)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return cur, err
}

// SaveCursor 持久化游标。
func (s *Store) SaveCursor(ctx context.Context, siteID, cursor string) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO cursors (site_id, cursor, updated_at) VALUES ($1,$2,now())
		 ON CONFLICT (site_id) DO UPDATE SET cursor = EXCLUDED.cursor, updated_at = now()`,
		siteID, cursor)
	return err
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

// AcquireLease 尝试获取站点租约。
//
// schema 里有 leases 表，采集主流程目前不调用。
// 两个 worker 并发跑同一站点时都会成功，产生重复采集。
func (s *Store) AcquireLease(ctx context.Context, siteID, workerID string, ttl time.Duration) (bool, error) {
	res, err := s.db.ExecContext(ctx, `
		INSERT INTO leases (site_id, worker_id, expires_at) VALUES ($1,$2,now() + $3::interval)
		ON CONFLICT (site_id) DO UPDATE
		SET worker_id = EXCLUDED.worker_id, expires_at = EXCLUDED.expires_at
		WHERE leases.expires_at < now() OR leases.worker_id = EXCLUDED.worker_id`,
		siteID, workerID, fmt.Sprintf("%d seconds", int(ttl.Seconds())))
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// ReleaseLease 释放站点租约。
func (s *Store) ReleaseLease(ctx context.Context, siteID, workerID string) error {
	_, err := s.db.ExecContext(ctx,
		`DELETE FROM leases WHERE site_id = $1 AND worker_id = $2`, siteID, workerID)
	return err
}
