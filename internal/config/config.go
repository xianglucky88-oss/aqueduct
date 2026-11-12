package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Site 是一个目标站点的采集配置。
type Site struct {
	ID        string        `yaml:"id"`
	BaseURL   string        `yaml:"base_url"`
	Path      string        `yaml:"path"`
	Mode      string        `yaml:"mode"` // cursor | page | single
	PageSize  int           `yaml:"page_size"`
	Interval  time.Duration `yaml:"interval"`
	RPS       int           `yaml:"rps"`
	Headers   map[string]string `yaml:"headers"`
	Enabled   bool          `yaml:"enabled"`
}

// Config 是完整配置。
type Config struct {
	DatabaseURL string `yaml:"database_url"`
	RedisURL    string `yaml:"redis_url"`
	Sites       []Site `yaml:"sites"`
	WorkerID    string `yaml:"worker_id"`
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
