package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Site 是一个目标站点的采集配置。
type Site struct {
	ID       string            `yaml:"id"`
	BaseURL  string            `yaml:"base_url"`
	Path     string            `yaml:"path"`
	Mode     string            `yaml:"mode"` // cursor | page | single
	PageSize int               `yaml:"page_size"`
	Interval time.Duration     `yaml:"interval"`
	RPS      int               `yaml:"rps"`
	Headers  map[string]string `yaml:"headers"`
	Enabled  bool              `yaml:"enabled"`
	// Budget 是可选的站点级请求预算配置。
	// 留空时由 Resolve 推导，推导结果保持本字段引入前的行为。
	Budget *Budget `yaml:"budget,omitempty"`
}

// Budget 是单个站点的分布式请求预算。
//
// 所有字段均可留空；零值经 Resolve 推导，推导规则保证：
// 不写 budget 块的既有站点，长期速率与本特性引入前完全一致（rate=rps），
// 且默认不自动重试（max_attempts=1）。
type Budget struct {
	// Rate 是集群所有实例共享的长期可持续速率（请求/秒）。零值取站点 rps。
	Rate float64 `yaml:"rate,omitempty"`
	// Burst 是令牌桶容量，约束短时突发。零值取 ceil(rate)。
	Burst int `yaml:"burst,omitempty"`
	// MaxInflight 是集群所有实例对该站点的并发在途请求上限。零值取 1。
	MaxInflight int `yaml:"max_inflight,omitempty"`
	// MaxAttempts 是单页请求的最大尝试次数（含首次）。
	// 1 表示不重试，即历史行为；>1 时每次重试都重新申请预算。
	MaxAttempts int `yaml:"max_attempts,omitempty"`
	// FallbackFraction 是预算后端（Redis）不可用时，单实例降级速率相对
	// 全量配额的比例，取值 (0,1]。零值取 0.5。
	FallbackFraction float64 `yaml:"fallback_fraction,omitempty"`
}

// ResolvedBudget 是填充默认值并校验后的预算规格。
type ResolvedBudget struct {
	Rate             float64
	Burst            int
	MaxInflight      int
	MaxAttempts      int
	FallbackFraction float64
}

// Resolve 按站点配置补全默认值并校验。
//
// 默认值刻意复刻历史语义：rate 取 rps、不重试、串行在途 1。
func (b *Budget) Resolve(fallbackRPS int) (ResolvedBudget, error) {
	out := ResolvedBudget{}
	if b != nil {
		out.Rate = b.Rate
		out.Burst = b.Burst
		out.MaxInflight = b.MaxInflight
		out.MaxAttempts = b.MaxAttempts
		out.FallbackFraction = b.FallbackFraction
	}
	if out.Rate == 0 {
		if fallbackRPS <= 0 {
			fallbackRPS = 2
		}
		out.Rate = float64(fallbackRPS)
	}
	if out.Burst == 0 {
		out.Burst = int(out.Rate + 0.999999)
	}
	if out.MaxInflight == 0 {
		out.MaxInflight = 1
	}
	if out.MaxAttempts == 0 {
		out.MaxAttempts = 1
	}
	if out.FallbackFraction == 0 {
		out.FallbackFraction = 0.5
	}
	if out.Rate <= 0 {
		return out, fmt.Errorf("budget.rate must be positive, got %v", out.Rate)
	}
	if out.Burst < 1 {
		return out, fmt.Errorf("budget.burst must be >= 1, got %d", out.Burst)
	}
	if out.MaxInflight < 1 {
		return out, fmt.Errorf("budget.max_inflight must be >= 1, got %d", out.MaxInflight)
	}
	if out.MaxAttempts < 1 {
		return out, fmt.Errorf("budget.max_attempts must be >= 1, got %d", out.MaxAttempts)
	}
	if out.FallbackFraction <= 0 || out.FallbackFraction > 1 {
		return out, fmt.Errorf("budget.fallback_fraction must be in (0,1], got %v", out.FallbackFraction)
	}
	return out, nil
}

// Config 是完整配置。
type Config struct {
	DatabaseURL string  `yaml:"database_url"`
	RedisURL    string  `yaml:"redis_url"`
	Sites       []Site  `yaml:"sites"`
	WorkerID    string  `yaml:"worker_id"`
}

// Load 读取 YAML 配置，并允许用环境变量覆盖数据库连接。
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var cfg Config
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if v := os.Getenv("DATABASE_URL"); v != "" {
		cfg.DatabaseURL = v
	}
	if v := os.Getenv("REDIS_URL"); v != "" {
		cfg.RedisURL = v
	}
	if v := os.Getenv("WORKER_ID"); v != "" {
		cfg.WorkerID = v
	}
	if cfg.WorkerID == "" {
		host, _ := os.Hostname()
		cfg.WorkerID = host
	}
	for i := range cfg.Sites {
		if cfg.Sites[i].PageSize == 0 {
			cfg.Sites[i].PageSize = 50
		}
		if cfg.Sites[i].RPS == 0 {
			cfg.Sites[i].RPS = 2
		}
		if cfg.Sites[i].Interval == 0 {
			cfg.Sites[i].Interval = 5 * time.Minute
		}
		if cfg.Sites[i].Mode == "" {
			cfg.Sites[i].Mode = "cursor"
		}
		if _, err := cfg.Sites[i].Budget.Resolve(cfg.Sites[i].RPS); err != nil {
			return nil, fmt.Errorf("site %s: %w", cfg.Sites[i].ID, err)
		}
	}
	return &cfg, nil
}

// EnabledSites 返回启用的站点。
func (c *Config) EnabledSites() []Site {
	out := make([]Site, 0, len(c.Sites))
	for _, s := range c.Sites {
		if s.Enabled {
			out = append(out, s)
		}
	}
	return out
}

// SiteByID 查找站点。
func (c *Config) SiteByID(id string) (Site, bool) {
	for _, s := range c.Sites {
		if s.ID == id {
			return s, true
		}
	}
	return Site{}, false
}
