package config

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Site 是一个目标站点的采集配置。
//
// Interval 是「两次整轮采集开始时间」之间的目标间隔。租约、调度与退避
// 都由 Config 的全局参数控制，站点之间互不共享 ticker。
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
}

// Config 是完整配置。运行参数均可由环境变量覆盖，便于以同一个镜像
// 在不同环境（本地 compose / Kubernetes）部署。
type Config struct {
	DatabaseURL string `yaml:"database_url"`
	RedisURL    string `yaml:"redis_url"`
	WorkerID    string `yaml:"worker_id"`
	APIAddr     string `yaml:"api_addr"`

	// LeaseTTL 是站点租约的有效期。持有者必须在到期前续租，
	// 否则其他副本可以接管该站点。
	LeaseTTL time.Duration `yaml:"lease_ttl"`
	// LeaseRenewInterval 是租约续期间隔，必须显著小于 LeaseTTL。
	LeaseRenewInterval time.Duration `yaml:"lease_renew_interval"`
	// ShutdownGrace 是收到 SIGTERM 后允许在途工作收尾的最长时间。
	// 必须小于 Kubernetes 的 terminationGracePeriodSeconds。
	ShutdownGrace time.Duration `yaml:"shutdown_grace"`
	// SchedulerPoll 是调度器检查「是否有到期站点」的轮询间隔。
	SchedulerPoll time.Duration `yaml:"scheduler_poll"`
	// MaxConcurrentSites 限制单个副本同时采集的站点数量，
	// 避免一个副本抢占全部租约。
	MaxConcurrentSites int `yaml:"max_concurrent_sites"`
	// ReadinessCacheTTL 是就绪探针依赖检查结果的缓存时间，
	// 避免每次探测都打数据库。
	ReadinessCacheTTL time.Duration `yaml:"readiness_cache_ttl"`
	// DBMaxOpenConns 是单副本数据库连接池上限。多副本部署时
	// 需要保证 副本数 * DBMaxOpenConns 不超过数据库上限。
	DBMaxOpenConns int `yaml:"db_max_open_conns"`
	DBMaxIdleConns int `yaml:"db_max_idle_conns"`
	// MaxPages 是单轮采集的页数硬上限。超过后本轮以失败结束，
	// 已提交的页作为检查点保留，下一次从检查点继续。
	MaxPages int `yaml:"max_pages"`

	Sites []Site `yaml:"sites"`
}

// Defaults 返回一份可直接用于校验的默认运行参数。
func Defaults() Config {
	return Config{
		APIAddr:            ":8081",
		LeaseTTL:           45 * time.Second,
		LeaseRenewInterval: 15 * time.Second,
		ShutdownGrace:      20 * time.Second,
		SchedulerPoll:      time.Second,
		MaxConcurrentSites: 2,
		ReadinessCacheTTL:  2 * time.Second,
		DBMaxOpenConns:     8,
		DBMaxIdleConns:     2,
		MaxPages:           200,
	}
}

// Load 读取 YAML 配置，应用默认值与环境变量覆盖，最后做整体校验。
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	cfg := Defaults()
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	applyEnv(&cfg)
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func applyEnv(cfg *Config) {
	// 连接串与身份优先来自环境变量，配置文件只保留非敏感默认值。
	if v := os.Getenv("DATABASE_URL"); v != "" {
		cfg.DatabaseURL = v
	}
	if v := os.Getenv("REDIS_URL"); v != "" {
		cfg.RedisURL = v
	}
	if v := os.Getenv("WORKER_ID"); v != "" {
		cfg.WorkerID = v
	} else if v := os.Getenv("POD_NAME"); v != "" {
		// Kubernetes 下建议用 downward API 注入 Pod 名；
		// 同一个 Pod 名在重启前后保持一致，有利于租约归属判断。
		cfg.WorkerID = v
	}
	if cfg.WorkerID == "" {
		host, _ := os.Hostname()
		cfg.WorkerID = host
	}
	if v := os.Getenv("API_ADDR"); v != "" {
		cfg.APIAddr = v
	}
	envDuration("LEASE_TTL", &cfg.LeaseTTL)
	envDuration("LEASE_RENEW_INTERVAL", &cfg.LeaseRenewInterval)
	envDuration("SHUTDOWN_GRACE", &cfg.ShutdownGrace)
	envDuration("SCHEDULER_POLL", &cfg.SchedulerPoll)
	envDuration("READINESS_CACHE_TTL", &cfg.ReadinessCacheTTL)
	envInt("MAX_CONCURRENT_SITES", &cfg.MaxConcurrentSites)
	envInt("DB_MAX_OPEN_CONNS", &cfg.DBMaxOpenConns)
	envInt("DB_MAX_IDLE_CONNS", &cfg.DBMaxIdleConns)
	envInt("MAX_PAGES", &cfg.MaxPages)
}

func envDuration(name string, dst *time.Duration) {
	if v := os.Getenv(name); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			*dst = d
		}
	}
}

func envInt(name string, dst *int) {
	if v := os.Getenv(name); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			*dst = n
		}
	}
}

// Validate 校验运行参数与站点配置。配置错误属于不可恢复错误，
// 应在进程启动阶段快速失败，而不是在健康检查里反复重试。
func (c *Config) Validate() error {
	if strings.TrimSpace(c.DatabaseURL) == "" {
		return fmt.Errorf("database_url is required")
	}
	if c.WorkerID == "" {
		return fmt.Errorf("worker_id is required (set WORKER_ID or POD_NAME)")
	}
	if c.APIAddr == "" {
		return fmt.Errorf("api_addr must not be empty")
	}
	if c.LeaseTTL < 15*time.Second {
		return fmt.Errorf("lease_ttl %s too short: must be >= 15s", c.LeaseTTL)
	}
	if c.LeaseRenewInterval <= 0 || c.LeaseRenewInterval >= c.LeaseTTL/2 {
		return fmt.Errorf("lease_renew_interval %s must be in (0, lease_ttl/2)", c.LeaseRenewInterval)
	}
	if c.ShutdownGrace <= 0 || c.ShutdownGrace >= c.LeaseTTL {
		// 关闭窗口必须小于租约，否则进程退出时租约已过期，
		// 无法在退出前完成一次「干净释放」。
		return fmt.Errorf("shutdown_grace %s must be in (0, lease_ttl=%s)", c.ShutdownGrace, c.LeaseTTL)
	}
	if c.SchedulerPoll < 100*time.Millisecond || c.SchedulerPoll > 30*time.Second {
		return fmt.Errorf("scheduler_poll %s out of range [100ms, 30s]", c.SchedulerPoll)
	}
	if c.MaxConcurrentSites < 1 {
		return fmt.Errorf("max_concurrent_sites must be >= 1")
	}
	if c.ReadinessCacheTTL <= 0 || c.ReadinessCacheTTL > 10*time.Second {
		return fmt.Errorf("readiness_cache_ttl must be in (0, 10s]")
	}
	if c.DBMaxOpenConns < 1 || c.DBMaxIdleConns < 0 || c.DBMaxIdleConns > c.DBMaxOpenConns {
		return fmt.Errorf("db connection pool bounds invalid")
	}
	if c.MaxPages < 1 || c.MaxPages > 10000 {
		return fmt.Errorf("max_pages must be in [1, 10000]")
	}

	seen := make(map[string]struct{}, len(c.Sites))
	for i := range c.Sites {
		s := &c.Sites[i]
		s.ID = strings.TrimSpace(s.ID)
		if s.ID == "" {
			return fmt.Errorf("site[%d]: id is required", i)
		}
		if _, ok := seen[s.ID]; ok {
			return fmt.Errorf("site id %q duplicated", s.ID)
		}
		seen[s.ID] = struct{}{}
		switch s.Mode {
		case "cursor", "page", "single":
		default:
			return fmt.Errorf("site %q: unknown mode %q (want cursor|page|single)", s.ID, s.Mode)
		}
		if s.PageSize < 1 || s.PageSize > 500 {
			return fmt.Errorf("site %q: page_size must be in [1,500]", s.ID)
		}
		if s.Interval < time.Second {
			return fmt.Errorf("site %q: interval must be >= 1s", s.ID)
		}
		if s.RPS < 1 {
			return fmt.Errorf("site %q: rps must be >= 1", s.ID)
		}
		u, err := url.Parse(s.BaseURL)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return fmt.Errorf("site %q: base_url must be an absolute http(s) URL", s.ID)
		}
		if !strings.HasPrefix(s.Path, "/") {
			return fmt.Errorf("site %q: path must start with /", s.ID)
		}
	}
	return nil
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
