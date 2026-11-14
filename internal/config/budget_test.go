package config

import (
	"os"
	"testing"
	"time"
)

func TestBudgetDefaultsPreserveLegacySemantics(t *testing.T) {
	// 既有站点不写 budget 块：rate 必须等于 rps，且默认不重试。
	var b *Budget
	got, err := b.Resolve(4)
	if err != nil {
		t.Fatalf("nil budget resolve: %v", err)
	}
	if got.Rate != 4 {
		t.Fatalf("default rate = %v, want 4 (legacy rps)", got.Rate)
	}
	if got.Burst != 4 {
		t.Fatalf("default burst = %d, want ceil(rate)", got.Burst)
	}
	if got.MaxInflight != 1 {
		t.Fatalf("default max_inflight = %d, want 1", got.MaxInflight)
	}
	if got.MaxAttempts != 1 {
		t.Fatalf("default max_attempts = %d, want 1 (no retry, legacy behavior)", got.MaxAttempts)
	}
	if got.FallbackFraction != 0.5 {
		t.Fatalf("default fallback fraction = %v, want 0.5", got.FallbackFraction)
	}
}

func TestBudgetExplicitFieldsKept(t *testing.T) {
	b := &Budget{Rate: 10, Burst: 20, MaxInflight: 3, MaxAttempts: 5, FallbackFraction: 0.25}
	got, err := b.Resolve(4)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got.Rate != 10 || got.Burst != 20 || got.MaxInflight != 3 ||
		got.MaxAttempts != 5 || got.FallbackFraction != 0.25 {
		t.Fatalf("explicit fields altered: %+v", got)
	}
}

func TestBudgetValidation(t *testing.T) {
	cases := []struct {
		name string
		b    *Budget
		rps  int
	}{
		{"zero rate with zero rps", &Budget{Rate: -1}, 2},
		{"bad fallback", &Budget{FallbackFraction: 1.5}, 2},
		{"zero inflight explicit", &Budget{MaxInflight: -1}, 2},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := c.b.Resolve(c.rps); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestLoadRejectsInvalidBudget(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/sites.yaml"
	content := []byte(`
sites:
  - id: broken
    enabled: true
    rps: 2
    budget:
      rate: -3
`)
	if err := writeFile(path, content); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("expected Load to reject negative budget rate")
	}
}

func TestLegacySiteFieldsUnchanged(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/sites.yaml"
	content := []byte(`
database_url: "postgres://x"
redis_url: "redis://y"
sites:
  - id: supplier
    base_url: http://example.invalid
    path: /items
    mode: cursor
    page_size: 50
    interval: 30s
    rps: 4
    enabled: true
`)
	if err := writeFile(path, content); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	s := cfg.Sites[0]
	if s.RPS != 4 || s.Interval != 30*time.Second || s.PageSize != 50 || s.Mode != "cursor" {
		t.Fatalf("legacy fields changed: %+v", s)
	}
	rb, err := s.Budget.Resolve(s.RPS)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if rb.Rate != 4 || rb.MaxAttempts != 1 {
		t.Fatalf("legacy site must keep rate=rps and no retry, got %+v", rb)
	}
}

func writeFile(path string, b []byte) error {
	return os.WriteFile(path, b, 0o600)
}
