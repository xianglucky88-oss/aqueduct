package budget

import (
	"context"
	"testing"

	"github.com/internal-platform/aqueduct/internal/config"
)

func TestRegistryLocalModeWithoutRedis(t *testing.T) {
	cfg := &config.Config{
		WorkerID: "w1",
		Sites: []config.Site{
			{ID: "a", RPS: 3, Enabled: true},
			{ID: "b", RPS: 2, Enabled: true,
				Budget: &config.Budget{Rate: 8, MaxAttempts: 3, MaxInflight: 2}},
		},
	}
	reg, err := NewRegistry(cfg, nil)
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	defer reg.Close()

	ga, ok := reg.Get("a")
	if !ok {
		t.Fatal("governor a missing")
	}
	if ga.Snapshot(context.Background()).Mode != ModeLocal {
		t.Fatal("nil redis must produce local mode")
	}
	// 未写 budget 的站点：rate 保持 rps、不重试。
	if ga.Spec().Rate != 3 || ga.Spec().MaxAttempts != 1 {
		t.Fatalf("legacy defaults changed: %+v", ga.Spec())
	}
	gb := reg.Must("b")
	if gb.Spec().Rate != 8 || gb.Spec().MaxAttempts != 3 || gb.Spec().MaxInflight != 2 {
		t.Fatalf("explicit budget not applied: %+v", gb.Spec())
	}
	if len(reg.Snapshots(context.Background())) != 2 {
		t.Fatal("snapshot count mismatch")
	}
}

func TestRegistryReloadUpdatesSpec(t *testing.T) {
	rdb := testRedis(t)
	cfg := &config.Config{WorkerID: "w1", Sites: []config.Site{{ID: "a", RPS: 3}}}
	reg, err := NewRegistry(cfg, rdb)
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	defer reg.Close()

	old := reg.Must("a").Spec().Version
	cfg2 := &config.Config{WorkerID: "w1", Sites: []config.Site{
		{ID: "a", RPS: 3, Budget: &config.Budget{Rate: 9, Burst: 4}},
	}}
	if err := reg.Reload(cfg2); err != nil {
		t.Fatalf("reload: %v", err)
	}
	gov := reg.Must("a")
	if gov.Spec().Rate != 9 || gov.Spec().Burst != 4 {
		t.Fatalf("reload did not apply new spec: %+v", gov.Spec())
	}
	if gov.Spec().Version == old {
		t.Fatal("config change must produce a new version")
	}
}

func TestRegistryUnknownSite(t *testing.T) {
	reg, err := NewRegistry(&config.Config{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := reg.Get("ghost"); ok {
		t.Fatal("unknown site must not be found")
	}
}

func TestSpecVersionStableAcrossRestartsAndSensitiveToChange(t *testing.T) {
	base := config.Site{ID: "a", RPS: 4, Budget: &config.Budget{
		Rate: 4, Burst: 6, MaxInflight: 2, MaxAttempts: 3, FallbackFraction: 0.5}}

	// 同一份配置反复派生必须得到相同版本（重启不应误触发换桶）。
	v1 := specVersion(base)
	v2 := specVersion(base)
	if v1 != v2 {
		t.Fatalf("version not stable: %d vs %d", v1, v2)
	}
	if v1 == 0 {
		t.Fatal("version must be non-zero")
	}

	// 任一预算字段变化都必须换版本。
	mutated := base
	mutated.Budget = &config.Budget{
		Rate: 5, Burst: 6, MaxInflight: 2, MaxAttempts: 3, FallbackFraction: 0.5}
	if specVersion(mutated) == v1 {
		t.Fatal("rate change must produce a new version")
	}
	mutated.Budget.Burst = 7
	if specVersion(mutated) == v1 {
		t.Fatal("burst change must produce a new version")
	}
}
