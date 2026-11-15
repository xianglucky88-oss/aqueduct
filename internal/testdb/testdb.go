// Package testdb 为集成测试准备相互隔离的数据库 schema。
//
// 集成测试以真实 PostgreSQL 验证租约、围栏与检查点语义。Go 会并行运行
// 不同的包，若共用 public schema，各包的 TRUNCATE 会互相清掉数据。
// 这里给每个测试包分配独立 schema：通过 lib/pq 支持的 search_path DSN
// 参数切换，使 DDL 与 DML 天然隔离，无需加锁或串行执行。
package testdb

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"

	_ "github.com/lib/pq"
)

// DSN 返回带独立 search_path 的连接串，并确保 schema 已存在。
// 未设置 AQUEDUCT_TEST_DATABASE_URL 时跳过当前测试。
func DSN(t *testing.T, schema string) string {
	t.Helper()
	base := os.Getenv("AQUEDUCT_TEST_DATABASE_URL")
	if base == "" {
		t.Skip("AQUEDUCT_TEST_DATABASE_URL not set; skipping PostgreSQL integration test")
	}

	// 先连默认 schema 创建目标 schema。
	admin, err := sql.Open("postgres", base)
	if err != nil {
		t.Fatalf("testdb: open admin: %v", err)
	}
	defer admin.Close()
	if _, err := admin.Exec(`CREATE SCHEMA IF NOT EXISTS ` + quoteIdent(schema)); err != nil {
		t.Fatalf("testdb: create schema %s: %v", schema, err)
	}

	sep := "?"
	if strings.Contains(base, "?") {
		sep = "&"
	}
	return fmt.Sprintf("%s%ssearch_path=%s", base, sep, schema)
}

// quoteIdent 只允许测试内写死的标识符，仍做最小校验避免拼接错误。
func quoteIdent(s string) string {
	clean := strings.Map(func(r rune) rune {
		if r == '_' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			return r
		}
		return -1
	}, s)
	if clean == "" {
		clean = "test"
	}
	return `"` + clean + `"`
}
