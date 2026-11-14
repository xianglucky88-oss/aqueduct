package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/lib/pq"

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

// migrationBatch 是每次迁移事务处理的来源对象数。
// 批次小意味着失败恢复时重复的工作量有界。
const migrationBatch = 200

// Migrate 建表、扩展现有表并迁移历史数据。幂等、可重复执行、可按批恢复。
//
// 结构：
//   - records         当前状态，每个 (site_id, source_id) 恰好一行；现有消费者继续读它。
//   - record_versions 历史版本，只追加，按 revision 稳定排序，按 [valid_from, valid_to) 定位。
//   - data_migrations 每站点迁移进度，用于分批中断后的续跑。
//
// 迁移不依赖重新采集：现有重复行按 (updated_at, item_key) 的确定顺序
// 还原成版本链，最新一版留在 records，其余移入 record_versions。
func (s *Store) Migrate(ctx context.Context) error {
	if err := s.migrateSchema(ctx); err != nil {
		return err
	}
	if err := s.backfillAll(ctx); err != nil {
		return err
	}

	// 结算迁移期间残留的重复行：通常来自尚未停机的旧版本进程。
	// 合并只沿用冲突规则，不会让旧观测覆盖新事实。
	for attempt := 0; attempt < 3; attempt++ {
		dupes, err := s.duplicateSites(ctx)
		if err != nil {
			return err
		}
		if len(dupes) == 0 {
			break
		}
		if _, err := s.db.ExecContext(ctx,
			`DELETE FROM data_migrations WHERE site_id = ANY($1)`, pq.Array(dupes)); err != nil {
			return fmt.Errorf("reset dup sites: %w", err)
		}
		if err := s.backfillAll(ctx); err != nil {
			return err
		}
	}

	// 唯一约束是全模型的最后一道闸门：一个来源对象只能有一个当前状态。
	// 建索引失败意味着仍有重复，直接失败而不是静默删除。
	if _, err := s.db.ExecContext(ctx,
		`CREATE UNIQUE INDEX IF NOT EXISTS records_site_source_uidx ON records (site_id, source_id)`); err != nil {
		return fmt.Errorf("create unique index (stop legacy writers if duplicates remain): %w", err)
	}
	return nil
}

func (s *Store) migrateSchema(ctx context.Context) error {
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
		// 当前表新增列：revision 指向历史链，content_hash 用于判等，
		// observed_at 记录首次观测到该事实的系统时间。
		`ALTER TABLE records ADD COLUMN IF NOT EXISTS revision BIGINT NOT NULL DEFAULT 0`,
		`ALTER TABLE records ADD COLUMN IF NOT EXISTS content_hash TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE records ADD COLUMN IF NOT EXISTS observed_at TIMESTAMPTZ NOT NULL DEFAULT now()`,
		// watermark_at 是该来源对象已采纳的最大上游业务时间。
		// 即使内容没变，上游再次给出更新的时间也会抬高水位，
		// 阻止之后带有更早时间的观测覆盖已确认的事实。
		`ALTER TABLE records ADD COLUMN IF NOT EXISTS watermark_at TIMESTAMPTZ`,
		// valid_from 是当前版本的业务可见起点。冗余在 records 上，
		// 使写入判定不必 JOIN 历史表（JOIN 在 READ COMMITTED 下会拿到
		// 已过期的快照，导致区间校验失败）。
		//
		// 一次性加列，并给默认值：迁移窗口内尚未升级的旧写入进程仍能
		// INSERT（拿默认观测时间），由 Migrate 的残留合并重新定序。
		`DO $$
		BEGIN
			IF NOT EXISTS (
				SELECT 1 FROM information_schema.columns
				WHERE table_name = 'records' AND column_name = 'valid_from'
			) THEN
				ALTER TABLE records ADD COLUMN valid_from TIMESTAMPTZ;
				UPDATE records SET valid_from = coalesce(updated_at, observed_at);
				ALTER TABLE records ALTER COLUMN valid_from SET DEFAULT now();
				ALTER TABLE records ALTER COLUMN valid_from SET NOT NULL;
			END IF;
		END $$`,
		// 上游缺失业务时间时必须能落 NULL，而不是伪造本机时间。
		`ALTER TABLE records ALTER COLUMN updated_at DROP NOT NULL`,
		`CREATE TABLE IF NOT EXISTS record_versions (
			site_id      TEXT NOT NULL,
			source_id    TEXT NOT NULL,
			revision     BIGINT NOT NULL,
			item_key     TEXT NOT NULL,
			title        TEXT NOT NULL DEFAULT '',
			price_cents  BIGINT NOT NULL DEFAULT 0,
			stock        INTEGER NOT NULL DEFAULT 0,
			updated_at   TIMESTAMPTZ,
			payload      JSONB NOT NULL DEFAULT '{}'::jsonb,
			content_hash TEXT NOT NULL,
			observed_at  TIMESTAMPTZ NOT NULL,
			valid_from   TIMESTAMPTZ NOT NULL,
			valid_to     TIMESTAMPTZ,
			from_legacy  BOOLEAN NOT NULL DEFAULT false,
			PRIMARY KEY (site_id, source_id, revision),
			CHECK (valid_to IS NULL OR valid_to >= valid_from)
		)`,
		// 单对象历史、单对象 as-of、以及站点级 DISTINCT ON 都走这条索引。
		`CREATE INDEX IF NOT EXISTS record_versions_history_idx
			ON record_versions (site_id, source_id, valid_from DESC, revision DESC)`,
		// 站点维度按时间过滤。
		`CREATE INDEX IF NOT EXISTS record_versions_site_time_idx
			ON record_versions (site_id, valid_from DESC, revision DESC)`,
		`CREATE TABLE IF NOT EXISTS data_migrations (
			site_id        TEXT PRIMARY KEY,
			phase          TEXT NOT NULL,
			last_source_id TEXT NOT NULL DEFAULT '',
			updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
		`CREATE TABLE IF NOT EXISTS runs (
			run_id      TEXT PRIMARY KEY,
			site_id     TEXT NOT NULL,
			started_at  TIMESTAMPTZ NOT NULL,
			finished_at TIMESTAMPTZ,
			status      TEXT NOT NULL,
			items_seen  INTEGER NOT NULL DEFAULT 0,
			items_wrote INTEGER NOT NULL DEFAULT 0
		)`,
		`ALTER TABLE runs ADD COLUMN IF NOT EXISTS items_stale INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE runs ADD COLUMN IF NOT EXISTS items_unchanged INTEGER NOT NULL DEFAULT 0`,
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

// duplicateSites 返回仍有重复 (site_id, source_id) 的站点。
func (s *Store) duplicateSites(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT site_id FROM records
		GROUP BY site_id HAVING count(*) > count(DISTINCT source_id)
		ORDER BY site_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var site string
		if err := rows.Scan(&site); err != nil {
			return nil, err
		}
		out = append(out, site)
	}
	return out, rows.Err()
}

// backfillAll 对所有含数据但尚未完成的站点执行分批迁移。
func (s *Store) backfillAll(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, `SELECT DISTINCT site_id FROM records ORDER BY site_id`)
	if err != nil {
		return err
	}
	var sites []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		sites = append(sites, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for _, site := range sites {
		if err := s.backfillSite(ctx, site); err != nil {
			return fmt.Errorf("backfill site %s: %w", site, err)
		}
	}
	return nil
}

// backfillSite 按来源对象分批迁移。每批一个事务：历史版本、当前行清理与
// 进度标记同生共死，进程崩溃后从 last_source_id 续跑。
func (s *Store) backfillSite(ctx context.Context, site string) error {
	var after string
	var phase string
	err := s.db.QueryRowContext(ctx,
		`SELECT last_source_id, phase FROM data_migrations WHERE site_id = $1`, site).
		Scan(&after, &phase)
	switch {
	case err == sql.ErrNoRows:
		after, phase = "", ""
	case err != nil:
		return err
	}
	if phase == "done" {
		return nil
	}

	for {
		next, done, err := s.migrateBatch(ctx, site, after)
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		after = next
	}
}

// migrateBatch 处理一批 source_id，返回下一批的起点。
func (s *Store) migrateBatch(ctx context.Context, site, after string) (string, bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", false, err
	}
	defer tx.Rollback()

	// 站点级互斥，避免多实例同时迁移同一站点。
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, "migrate:"+site); err != nil {
		return "", false, err
	}

	rows, err := tx.QueryContext(ctx, `
		SELECT source_id FROM records
		WHERE site_id = $1 AND source_id > $2
		GROUP BY source_id ORDER BY source_id LIMIT $3`,
		site, after, migrationBatch)
	if err != nil {
		return "", false, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return "", false, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return "", false, err
	}
	if len(ids) == 0 {
		if err := markSite(ctx, tx, site, "done", ""); err != nil {
			return "", false, err
		}
		return "", true, tx.Commit()
	}

	lastID := ids[len(ids)-1]
	if err := s.migrateGroups(ctx, tx, site, ids); err != nil {
		return "", false, err
	}
	if err := markSite(ctx, tx, site, "running", lastID); err != nil {
		return "", false, err
	}
	return lastID, false, tx.Commit()
}

// markSite 记录进度，不提交事务；调用方负责 Commit。
func markSite(ctx context.Context, tx *sql.Tx, site, phase, last string) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO data_migrations (site_id, phase, last_source_id, updated_at)
		VALUES ($1, $2, $3, now())
		ON CONFLICT (site_id) DO UPDATE
		SET phase = EXCLUDED.phase, last_source_id = EXCLUDED.last_source_id, updated_at = now()`,
		site, phase, last)
	return err
}

// legacyRow 是迁移前 records 里的一行。
type legacyRow struct {
	itemKey     string
	sourceID    string
	title       string
	priceCents  int64
	stock       int
	updatedAt   sql.NullTime
	payload     string
	collectedAt sql.NullTime
}

func (s *Store) migrateGroups(ctx context.Context, tx *sql.Tx, site string, ids []string) error {
	rows, err := tx.QueryContext(ctx, `
		SELECT item_key, source_id, title, price_cents, stock, updated_at,
		       payload::text, collected_at
		FROM records
		WHERE site_id = $1 AND source_id = ANY($2)
		ORDER BY source_id, updated_at ASC NULLS FIRST, item_key ASC
		FOR UPDATE`, site, pq.Array(ids))
	if err != nil {
		return err
	}
	bySource := map[string][]legacyRow{}
	var order []string
	for rows.Next() {
		var r legacyRow
		if err := rows.Scan(&r.itemKey, &r.sourceID, &r.title, &r.priceCents,
			&r.stock, &r.updatedAt, &r.payload, &r.collectedAt); err != nil {
			rows.Close()
			return err
		}
		if _, ok := bySource[r.sourceID]; !ok {
			order = append(order, r.sourceID)
		}
		bySource[r.sourceID] = append(bySource[r.sourceID], r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	for _, sourceID := range order {
		if err := s.migrateOne(ctx, tx, site, sourceID, bySource[sourceID]); err != nil {
			return err
		}
	}
	return nil
}

// migrateOne 把单个来源对象的旧行还原为版本链。
//
// 已建立历史链的对象走「合并残留」路径：把旧版本进程在迁移后写入的
// 重复行按冲突规则并入现有链，不会覆盖较新的业务事实。
// 尚未建立历史链的对象走「整链重建」路径。
func (s *Store) migrateOne(ctx context.Context, tx *sql.Tx, site, sourceID string, group []legacyRow) error {
	var existing int
	if err := tx.QueryRowContext(ctx,
		`SELECT count(*) FROM record_versions WHERE site_id = $1 AND source_id = $2`,
		site, sourceID).Scan(&existing); err != nil {
		return err
	}
	if existing == 0 {
		return s.rebuildChain(ctx, tx, site, sourceID, group)
	}
	return s.mergeResidual(ctx, tx, site, sourceID, group)
}

// rebuildChain 从旧行重建版本链。
//
// 顺序规则：(updated_at, item_key) 升序，NULL 时间排在最前（视为最旧）。
// 未知时间行的可见起点对齐到其后第一个已知时间的行，因此同一时点的
// 查询会由 revision 裁决到最后一行，已知时间优先于未知时间。
// 最后一行成为当前状态。全程确定，重复执行结果一致。
func (s *Store) rebuildChain(ctx context.Context, tx *sql.Tx, site, sourceID string, group []legacyRow) error {
	n := len(group)
	observed := make([]time.Time, n)
	validFrom := make([]time.Time, n)

	knownAt := func(i int) (time.Time, bool) {
		t := group[i].updatedAt
		return t.Time, t.Valid && !t.Time.IsZero()
	}
	// 先找每行之后最近的已知时间，作为未知行的对齐点。
	nextKnown := make([]time.Time, n) // 从右往左填充；零值表示没有
	var carry time.Time
	for i := n - 1; i >= 0; i-- {
		if t, ok := knownAt(i); ok {
			carry = t
			nextKnown[i] = time.Time{}
			continue
		}
		nextKnown[i] = carry
	}

	for i, r := range group {
		obs := r.collectedAt.Time
		if !r.collectedAt.Valid || obs.IsZero() {
			obs = time.Now().UTC()
		}
		observed[i] = obs

		if t, ok := knownAt(i); ok {
			validFrom[i] = t
		} else if !nextKnown[i].IsZero() {
			validFrom[i] = nextKnown[i]
		} else {
			validFrom[i] = obs
		}
		if i > 0 && validFrom[i].Before(validFrom[i-1]) {
			validFrom[i] = validFrom[i-1]
		}
	}

	for i, r := range group {
		var validTo any
		if i+1 < n {
			validTo = validFrom[i+1]
		}
		payload := decodePayload(r.payload)
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO record_versions
				(site_id, source_id, revision, item_key, title, price_cents, stock,
				 updated_at, payload, content_hash, observed_at, valid_from, valid_to, from_legacy)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,true)
			ON CONFLICT (site_id, source_id, revision) DO NOTHING`,
			site, sourceID, int64(i+1), r.itemKey, r.title, r.priceCents, r.stock,
			nullableTime(r.updatedAt), toJSON(payload), contentHash(r), observed[i],
			validFrom[i], validTo); err != nil {
			return err
		}
	}

	winner := group[n-1]
	// 水位取整组已知业务时间的最大值，而不是只看最后一行：
	// 时间未知的行可能排在最后，但它不能抹掉更早确认过的时间。
	var watermark sql.NullTime
	for _, r := range group {
		if r.updatedAt.Valid && !r.updatedAt.Time.IsZero() &&
			(!watermark.Valid || r.updatedAt.Time.After(watermark.Time)) {
			watermark = r.updatedAt
		}
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE records
		SET revision = $3, content_hash = $4, observed_at = $5, watermark_at = $6,
		    valid_from = $7
		WHERE site_id = $1 AND source_id = $2`,
		site, sourceID, int64(n), contentHash(winner), observed[n-1],
		nullableTime(watermark), validFrom[n-1]); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `
		DELETE FROM records
		WHERE site_id = $1 AND source_id = $2 AND item_key <> $3`,
		site, sourceID, winner.itemKey)
	return err
}

// mergeResidual 处理历史链已建立后残留在 records 里的重复行。
//
// 典型来源是迁移尚未完成时仍在运行的旧版本进程：旧实现只会 INSERT 新行，
// 不会改写已有的当前行。因此链头对应的那一行保持原样，其余重复行都是
// 可能的观测，按业务时间顺序重新走一遍在线冲突判定：
// 更旧的观测直接丢弃，更新的观测才产生新版本。较新的业务事实永不被覆盖。
func (s *Store) mergeResidual(ctx context.Context, tx *sql.Tx, site, sourceID string, group []legacyRow) error {
	var headItemKey string
	var headValidFrom time.Time
	err := tx.QueryRowContext(ctx, `
		SELECT item_key, valid_from FROM record_versions
		WHERE site_id = $1 AND source_id = $2
		ORDER BY revision DESC LIMIT 1`, site, sourceID).
		Scan(&headItemKey, &headValidFrom)
	switch {
	case err == sql.ErrNoRows:
		return s.rebuildChain(ctx, tx, site, sourceID, group)
	case err != nil:
		return err
	}

	// 链头对应的当前行是权威行，先把它锁定。
	var canonical currentRow
	err = tx.QueryRowContext(ctx, `
		SELECT `+currentCols+currentFrom+`
		 WHERE r.site_id = $1 AND r.source_id = $2 AND r.item_key = $3
		 FOR UPDATE OF r`, site, sourceID, headItemKey).
		Scan(&canonical.sourceID, &canonical.itemKey, &canonical.title,
			&canonical.priceCents, &canonical.stock, &canonical.updatedAt,
			&canonical.payload, &canonical.contentHash, &canonical.revision,
			&canonical.validFrom, &canonical.watermark, &canonical.observed)
	if err != nil {
		// 权威行缺失（人工干预等）：以旧行的最佳观测重建。
		if _, delErr := tx.ExecContext(ctx, `
			DELETE FROM records WHERE site_id = $1 AND source_id = $2`,
			site, sourceID); delErr != nil {
			return delErr
		}
		return s.rebuildObservation(ctx, tx, site, sourceID, group)
	}

	if _, err := tx.ExecContext(ctx, `
		DELETE FROM records
		WHERE site_id = $1 AND source_id = $2 AND item_key <> $3`,
		site, sourceID, headItemKey); err != nil {
		return err
	}

	var res model.ApplyResult
	for _, r := range group {
		if r.itemKey == headItemKey {
			continue
		}
		obsTime := r.collectedAt.Time
		if !r.collectedAt.Valid || obsTime.IsZero() {
			obsTime = headValidFrom
		}
		rec := model.Record{
			SiteID:     site,
			SourceID:   sourceID,
			ItemKey:    r.itemKey,
			Title:      r.title,
			PriceCents: r.priceCents,
			Stock:      r.stock,
			UpdatedAt:  r.updatedAt.Time,
			Payload:    decodePayload(r.payload),
		}
		cur, err := lockOne(ctx, tx, site, sourceID)
		if err != nil {
			return err
		}
		if err := applyExisting(ctx, tx, rec, cur, obsTime, &res); err != nil {
			return err
		}
	}
	return nil
}

// rebuildObservation 在权威行缺失时，把组内最佳观测写成当前行。
func (s *Store) rebuildObservation(ctx context.Context, tx *sql.Tx, site, sourceID string, group []legacyRow) error {
	winner := group[len(group)-1]
	obs := winner.collectedAt.Time
	if !winner.collectedAt.Valid || obs.IsZero() {
		obs = time.Now().UTC()
	}
	_, err := insertNew(ctx, tx, model.Record{
		SiteID:     site,
		SourceID:   sourceID,
		ItemKey:    winner.itemKey,
		Title:      winner.title,
		PriceCents: winner.priceCents,
		Stock:      winner.stock,
		UpdatedAt:  winner.updatedAt.Time,
		Payload:    decodePayload(winner.payload),
	}, obs)
	return err
}

// Apply 把一批观测写入当前表与历史表。
//
// 同一来源对象在一个批次内出现多次时，先按冲突规则折叠为一次观测。
// 单站点在一个事务内完成，已存在的当前行用 SELECT ... FOR UPDATE 串行化；
// 新对象的并发插入由 (site_id, source_id) 唯一约束兜底。
func (s *Store) Apply(ctx context.Context, recs []model.Record) (model.ApplyResult, error) {
	var res model.ApplyResult
	if len(recs) == 0 {
		return res, nil
	}
	bySite := map[string][]model.Record{}
	for _, r := range recs {
		bySite[r.SiteID] = append(bySite[r.SiteID], r)
	}
	sites := make([]string, 0, len(bySite))
	for site := range bySite {
		sites = append(sites, site)
	}
	sort.Strings(sites)
	for _, site := range sites {
		part, err := s.applySite(ctx, site, bySite[site])
		if err != nil {
			return res, err
		}
		res.Written += part.Written
		res.Stale += part.Stale
		res.Unchanged += part.Unchanged
	}
	return res, nil
}

func (s *Store) applySite(ctx context.Context, site string, recs []model.Record) (model.ApplyResult, error) {
	var res model.ApplyResult
	folded, stale := foldRecords(recs)
	res.Stale += stale
	if len(folded) == 0 {
		return res, nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return res, err
	}
	defer tx.Rollback()

	sourceIDs := make([]string, 0, len(folded))
	for _, r := range folded {
		sourceIDs = append(sourceIDs, r.SourceID)
	}
	existing, err := lockCurrent(ctx, tx, site, sourceIDs)
	if err != nil {
		return res, err
	}

	now := time.Now().UTC()
	for _, rec := range folded {
		cur, ok := existing[rec.SourceID]
		if !ok {
			written, err := insertNew(ctx, tx, rec, now)
			if err != nil {
				return res, err
			}
			if written {
				res.Written++
				continue
			}
			// 并发插入抢先：退化为对已存在行的更新判定。
			cur, err = lockOne(ctx, tx, site, rec.SourceID)
			if err != nil {
				return res, err
			}
		}
		if err := applyExisting(ctx, tx, rec, cur, now, &res); err != nil {
			return res, err
		}
	}
	return res, tx.Commit()
}

// currentRow 是 records 里的一行当前状态。
// validFrom 取自对应历史版本的可见起点。
type currentRow struct {
	sourceID    string
	itemKey     string
	title       string
	priceCents  int64
	stock       int
	updatedAt   sql.NullTime
	payload     string
	contentHash string
	revision    int64
	validFrom   time.Time
	// watermark 是已确认的最大业务时间；只由上游给出过的业务时间抬高，
	// 不由本机观测时间抬高（否则未知时间的行会挡住之后更早的真实事实）。
	watermark sql.NullTime
	observed  time.Time
}

const currentCols = `
	r.source_id, r.item_key, r.title, r.price_cents, r.stock, r.updated_at,
	r.payload::text, r.content_hash, r.revision,
	r.valid_from, r.watermark_at, r.observed_at`

// currentFrom 只读 records：JOIN 历史表在并发下会看到过期快照。
const currentFrom = `
	FROM records r`

func lockCurrent(ctx context.Context, tx *sql.Tx, site string, sourceIDs []string) (map[string]currentRow, error) {
	rows, err := tx.QueryContext(ctx,
		`SELECT `+currentCols+currentFrom+`
		 WHERE r.site_id = $1 AND r.source_id = ANY($2)
		 FOR UPDATE OF r`, site, pq.Array(sourceIDs))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]currentRow{}
	for rows.Next() {
		var r currentRow
		if err := rows.Scan(&r.sourceID, &r.itemKey, &r.title, &r.priceCents,
			&r.stock, &r.updatedAt, &r.payload, &r.contentHash, &r.revision,
			&r.validFrom, &r.watermark, &r.observed); err != nil {
			return nil, err
		}
		out[r.sourceID] = r
	}
	return out, rows.Err()
}

func lockOne(ctx context.Context, tx *sql.Tx, site, sourceID string) (currentRow, error) {
	var r currentRow
	err := tx.QueryRowContext(ctx,
		`SELECT `+currentCols+currentFrom+`
		 WHERE r.site_id = $1 AND r.source_id = $2
		 FOR UPDATE OF r`, site, sourceID).
		Scan(&r.sourceID, &r.itemKey, &r.title, &r.priceCents, &r.stock,
			&r.updatedAt, &r.payload, &r.contentHash, &r.revision, &r.validFrom,
			&r.watermark, &r.observed)
	return r, err
}

// insertNew 创建来源对象的第一版。返回 false 表示并发插入抢先。
func insertNew(ctx context.Context, tx *sql.Tx, rec model.Record, now time.Time) (bool, error) {
	itemKey := rec.ItemKey
	if itemKey == "" {
		itemKey = rec.Identity()
	}
	validFrom := rec.UpdatedAt
	if validFrom.IsZero() {
		validFrom = now
	}
	res, err := tx.ExecContext(ctx, `
		INSERT INTO records
			(item_key, site_id, source_id, title, price_cents, stock, updated_at,
			 payload, revision, content_hash, observed_at, watermark_at, valid_from)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,1,$9,$10,$7,$11)
		ON CONFLICT DO NOTHING`,
		itemKey, rec.SiteID, rec.SourceID, rec.Title, rec.PriceCents, rec.Stock,
		nullableTimeFrom(rec.UpdatedAt), toJSON(rec.Payload), rec.ContentHash(), now,
		validFrom)
	if err != nil {
		return false, fmt.Errorf("insert current: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return false, nil
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO record_versions
			(site_id, source_id, revision, item_key, title, price_cents, stock,
			 updated_at, payload, content_hash, observed_at, valid_from, valid_to, from_legacy)
		VALUES ($1,$2,1,$3,$4,$5,$6,$7,$8,$9,$10,$11,NULL,false)
		ON CONFLICT (site_id, source_id, revision) DO NOTHING`,
		rec.SiteID, rec.SourceID, itemKey, rec.Title, rec.PriceCents, rec.Stock,
		nullableTimeFrom(rec.UpdatedAt), toJSON(rec.Payload), rec.ContentHash(),
		now, validFrom); err != nil {
		return false, fmt.Errorf("insert version: %w", err)
	}
	return true, nil
}

// applyExisting 对已存在的当前行做冲突判定与版本推进。
//
// 判定的基线是当前版本的业务时间与水位（watermark_at）的较大者：
// 水位只增不减，记录该来源已确认的最大业务时间。因此，即使内容未变，
// 上游给出的更晚时间也会抬高水位，之后携带更早时间的观测都无法覆盖。
func applyExisting(ctx context.Context, tx *sql.Tx, rec model.Record, cur currentRow, now time.Time, res *model.ApplyResult) error {
	hash := rec.ContentHash()
	baseline := cur.updatedAt.Time
	if cur.watermark.Valid && cur.watermark.Time.After(baseline) {
		baseline = cur.watermark.Time
	}

	if hash == cur.contentHash {
		// 业务事实未变：只刷新观测时间与水位，不制造版本。
		watermark := cur.watermark
		if rec.HasUpdatedAt() && (!watermark.Valid || rec.UpdatedAt.After(watermark.Time)) {
			watermark = sql.NullTime{Time: rec.UpdatedAt, Valid: true}
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE records SET observed_at = $3, watermark_at = $4
			WHERE site_id = $1 AND source_id = $2`,
			rec.SiteID, rec.SourceID, now, nullTime(watermark)); err != nil {
			return err
		}
		res.Unchanged++
		return nil
	}

	if model.DecideUpdate(baseline, rec.UpdatedAt) != model.Accept {
		// 倒退或缺失的业务时间不得覆盖已有事实。
		res.Stale++
		return nil
	}

	// 版本边界：新版本可见起点取业务时间，且不早于上一版，保证区间单调；
	// 同一时间戳下的冲突由 revision 裁决。
	newFrom := rec.UpdatedAt
	if newFrom.IsZero() {
		newFrom = now
	}
	if newFrom.Before(cur.validFrom) {
		newFrom = cur.validFrom
	}
	newRev := cur.revision + 1
	var watermark sql.NullTime
	switch {
	case rec.HasUpdatedAt() && cur.watermark.Valid:
		if rec.UpdatedAt.After(cur.watermark.Time) {
			watermark = sql.NullTime{Time: rec.UpdatedAt, Valid: true}
		} else {
			watermark = cur.watermark
		}
	case rec.HasUpdatedAt():
		watermark = sql.NullTime{Time: rec.UpdatedAt, Valid: true}
	default:
		watermark = cur.watermark
	}

	if _, err := tx.ExecContext(ctx, `
		UPDATE record_versions SET valid_to = $4
		WHERE site_id = $1 AND source_id = $2 AND revision = $3 AND valid_to IS NULL`,
		rec.SiteID, rec.SourceID, cur.revision, newFrom); err != nil {
		return err
	}
	// 同一来源对象的 item_key 保持不变，旧键对下游继续有效。
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO record_versions
			(site_id, source_id, revision, item_key, title, price_cents, stock,
			 updated_at, payload, content_hash, observed_at, valid_from, valid_to, from_legacy)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,NULL,false)
		ON CONFLICT (site_id, source_id, revision) DO NOTHING`,
		rec.SiteID, rec.SourceID, newRev, cur.itemKey, rec.Title, rec.PriceCents, rec.Stock,
		nullableTimeFrom(rec.UpdatedAt), toJSON(rec.Payload), hash, now, newFrom); err != nil {
		return fmt.Errorf("insert version: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE records
		SET title=$3, price_cents=$4, stock=$5, updated_at=$6, payload=$7,
		    content_hash=$8, revision=$9, observed_at=$10, watermark_at=$11,
		    valid_from=$12
		WHERE site_id=$1 AND source_id=$2`,
		rec.SiteID, rec.SourceID, rec.Title, rec.PriceCents, rec.Stock,
		nullableTimeFrom(rec.UpdatedAt), toJSON(rec.Payload), hash, newRev, now,
		nullTime(watermark), newFrom); err != nil {
		return err
	}
	res.Written++
	return nil
}

// foldRecords 折叠同批次内重复的来源对象，结果与到达顺序无关。
//
// 排序键：有业务时间优先，其次时间更晚优先，最后内容哈希作确定 tie-break。
// 缺失时间的观测永远排在已知时间之后，因此不能覆盖较新的业务事实。
// 返回值 stale 是内容不同、被更优观测取代的观测数，计入 items_stale；
// 内容完全相同的重复观测不计入，避免把正常的分页重复误报成冲突。
func foldRecords(recs []model.Record) ([]model.Record, int) {
	type key struct{ site, source string }
	chosen := map[key]model.Record{}
	hashes := map[key]map[string]bool{}
	var order []key
	stale := 0
	for _, r := range recs {
		k := key{r.SiteID, r.SourceID}
		h := r.ContentHash()
		cur, ok := chosen[k]
		if !ok {
			chosen[k] = r
			hashes[k] = map[string]bool{h: true}
			order = append(order, k)
			continue
		}
		if hashes[k][h] {
			continue // 完全相同的重复观测
		}
		hashes[k][h] = true
		if better(r, cur) {
			chosen[k] = r
		}
		stale++
	}
	out := make([]model.Record, 0, len(order))
	for _, k := range order {
		out = append(out, chosen[k])
	}
	return out, stale
}

// better 判定 a 是否应优先于 b 成为候选观测。
func better(a, b model.Record) bool {
	aKnown, bKnown := a.HasUpdatedAt(), b.HasUpdatedAt()
	if aKnown != bKnown {
		return aKnown
	}
	if aKnown && !a.UpdatedAt.Equal(b.UpdatedAt) {
		return a.UpdatedAt.After(b.UpdatedAt)
	}
	return a.ContentHash() > b.ContentHash()
}

func contentHash(r legacyRow) string {
	return model.Record{
		Title:      r.title,
		PriceCents: r.priceCents,
		Stock:      r.stock,
		Payload:    decodePayload(r.payload),
	}.ContentHash()
}

func nullableTime(t sql.NullTime) any {
	if !t.Valid || t.Time.IsZero() {
		return nil
	}
	return t.Time
}

// nullTime 保留 NULL 语义，用于需要区分「未知」与「已确认」的水位列。
func nullTime(t sql.NullTime) any {
	if !t.Valid || t.Time.IsZero() {
		return nil
	}
	return t.Time
}

func nullableTimeFrom(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}

func decodePayload(raw string) map[string]string {
	if raw == "" || raw == "{}" {
		return nil
	}
	var m map[string]string
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return nil
	}
	return m
}

// Upsert 保留旧签名，转调 Apply。
//
// Deprecated: 使用 Apply 以获得完整的冲突统计。
func (s *Store) Upsert(ctx context.Context, recs []model.Record) (int, error) {
	res, err := s.Apply(ctx, recs)
	return res.Written, err
}

func toJSON(m map[string]string) string {
	if len(m) == 0 {
		return "{}"
	}
	b, err := json.Marshal(m)
	if err != nil {
		return "{}"
	}
	return string(b)
}

// StartRun 插入一条运行记录。
func (s *Store) StartRun(ctx context.Context, run model.Run) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO runs (run_id, site_id, started_at, status) VALUES ($1,$2,$3,$4)`,
		run.ID, run.SiteID, run.StartedAt, run.Status)
	return err
}

// FinishRun 更新运行记录。
func (s *Store) FinishRun(ctx context.Context, run model.Run) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE runs SET finished_at = $2, status = $3, items_seen = $4,
		 items_wrote = $5, items_stale = $6, items_unchanged = $7
		 WHERE run_id = $1`,
		run.ID, run.FinishedAt, run.Status, run.ItemsSeen, run.ItemsWrote,
		run.ItemsStale, run.ItemsUnchanged)
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
		SELECT r.site_id, count(*) AS rows, count(DISTINCT r.source_id) AS distinct_source,
		       coalesce(max(r.updated_at)::text, '') AS max_updated,
		       coalesce(v.versions, 0) AS versions
		FROM records r
		LEFT JOIN (
			SELECT site_id, count(*) AS versions FROM record_versions GROUP BY site_id
		) v ON v.site_id = r.site_id
		GROUP BY r.site_id, v.versions ORDER BY r.site_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var ss model.SiteStats
		if err := rows.Scan(&ss.SiteID, &ss.Rows, &ss.DistinctSource, &ss.MaxUpdatedAt, &ss.Versions); err != nil {
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
	if err := curRows.Err(); err != nil {
		return nil, err
	}

	// 迁移是否完成，以「实际状态」为准而不是「有没有标记」：
	//   - 仍有重复当前行的站点，说明回填尚未收敛；
	//   - data_migrations 里 phase 不是 done 的站点，说明回填中断。
	// 迁移后才首次出现的新站点没有标记，也不需要标记，天然视为就绪。
	pending := map[string]bool{}
	for _, ss := range st.Sites {
		if ss.Rows != ss.DistinctSource {
			pending[ss.SiteID] = true
		}
	}
	doneRows, err := s.db.QueryContext(ctx,
		`SELECT site_id FROM data_migrations WHERE phase <> 'done'`)
	if err != nil {
		return nil, err
	}
	defer doneRows.Close()
	for doneRows.Next() {
		var site string
		if err := doneRows.Scan(&site); err != nil {
			return nil, err
		}
		pending[site] = true
	}
	if err := doneRows.Err(); err != nil {
		return nil, err
	}
	st.Migration.CompletedSites = s.countDone(ctx)
	for site := range pending {
		st.Migration.PendingSites = append(st.Migration.PendingSites, site)
	}
	sort.Strings(st.Migration.PendingSites)
	st.Migration.TemporalReady = len(st.Migration.PendingSites) == 0
	return st, nil
}

func (s *Store) countDone(ctx context.Context) int {
	var n int
	if err := s.db.QueryRowContext(ctx,
		`SELECT count(*) FROM data_migrations WHERE phase = 'done'`).Scan(&n); err != nil {
		return 0
	}
	return n
}

// Current 读取来源对象的当前状态。
func (s *Store) Current(ctx context.Context, siteID, sourceID string) (*model.Version, error) {
	var v model.Version
	var updated sql.NullTime
	var watermark sql.NullTime
	var observed time.Time
	var payload string
	err := s.db.QueryRowContext(ctx,
		`SELECT `+currentCols+currentFrom+`
		 WHERE r.site_id = $1 AND r.source_id = $2`, siteID, sourceID).
		Scan(&v.SourceID, &v.ItemKey, &v.Title, &v.PriceCents, &v.Stock,
			&updated, &payload, &v.ContentHash, &v.Revision, &v.ValidFrom,
			&watermark, &observed)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	v.SiteID = siteID
	v.ObservedAt = observed
	if updated.Valid {
		t := updated.Time
		v.UpdatedAt = &t
	}
	v.Payload = decodePayload(payload)
	return &v, nil
}

// History 按 revision 倒序返回来源对象的版本链。
func (s *Store) History(ctx context.Context, siteID, sourceID string, limit int) ([]model.Version, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT site_id, source_id, item_key, title, price_cents, stock, updated_at,
		       payload::text, content_hash, revision, observed_at, valid_from, valid_to, from_legacy
		FROM record_versions
		WHERE site_id = $1 AND source_id = $2
		ORDER BY revision DESC LIMIT $3`, siteID, sourceID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanVersions(rows)
}

// AsOf 返回在业务时点 at 可见的版本。
//
// 判定：valid_from <= at 的版本中，valid_from 最大、revision 最大者。
// 零长度区间（上游时间戳相同）也能按 revision 得到确定结果。
func (s *Store) AsOf(ctx context.Context, siteID, sourceID string, at time.Time) (*model.Version, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT site_id, source_id, item_key, title, price_cents, stock, updated_at,
		       payload::text, content_hash, revision, observed_at, valid_from, valid_to, from_legacy
		FROM record_versions
		WHERE site_id = $1 AND source_id = $2 AND valid_from <= $3
		ORDER BY valid_from DESC, revision DESC LIMIT 1`, siteID, sourceID, at)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	vs, err := scanVersions(rows)
	if err != nil || len(vs) == 0 {
		return nil, err
	}
	return &vs[0], nil
}

// SnapshotAsOf 返回站点在业务时点 at 的全量可见状态。
//
// 成本：扫描该站点 valid_from <= at 的版本并为每个来源取最新。
// 走 record_versions_history_idx；大站点用 limit/offset 分页读取。
func (s *Store) SnapshotAsOf(ctx context.Context, siteID string, at time.Time, limit, offset int) ([]model.Version, error) {
	if limit <= 0 || limit > 5000 {
		limit = 500
	}
	if offset < 0 {
		offset = 0
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT * FROM (
			SELECT DISTINCT ON (source_id)
				site_id, source_id, item_key, title, price_cents, stock, updated_at,
				payload::text, content_hash, revision, observed_at, valid_from, valid_to, from_legacy
			FROM record_versions
			WHERE site_id = $1 AND valid_from <= $2
			ORDER BY source_id, valid_from DESC, revision DESC
		) s ORDER BY source_id LIMIT $3 OFFSET $4`, siteID, at, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanVersions(rows)
}

func scanVersions(rows *sql.Rows) ([]model.Version, error) {
	var out []model.Version
	for rows.Next() {
		var v model.Version
		var updated, validTo sql.NullTime
		var payload string
		if err := rows.Scan(&v.SiteID, &v.SourceID, &v.ItemKey, &v.Title, &v.PriceCents,
			&v.Stock, &updated, &payload, &v.ContentHash, &v.Revision, &v.ObservedAt,
			&v.ValidFrom, &validTo, &v.FromLegacy); err != nil {
			return nil, err
		}
		if updated.Valid {
			t := updated.Time
			v.UpdatedAt = &t
		}
		if validTo.Valid {
			t := validTo.Time
			v.ValidTo = &t
		}
		v.Payload = decodePayload(payload)
		out = append(out, v)
	}
	return out, rows.Err()
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
