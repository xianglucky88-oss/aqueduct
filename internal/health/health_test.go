package health

import (
	"context"
	"errors"
	"testing"
	"time"
)

type fakeDep struct {
	err error
}

func (f *fakeDep) Ping(ctx context.Context) error { return f.err }

func TestStartupGatesReadiness(t *testing.T) {
	s := New(&fakeDep{}, nil, time.Second, time.Second)
	if r := s.Startup(); r.Status != "starting" {
		t.Fatalf("before MarkStarted: want starting, got %s", r.Status)
	}
	if r := s.Ready(context.Background()); Healthy(r) {
		t.Fatalf("not started must not be ready: %+v", r)
	}
	s.MarkStarted()
	if r := s.Startup(); r.Status != "ok" {
		t.Fatalf("after MarkStarted: want ok, got %s", r.Status)
	}
	if r := s.Ready(context.Background()); !Healthy(r) {
		t.Fatalf("started and db ok must be ready: %+v", r)
	}
}

func TestLiveIgnoresDependencyFailure(t *testing.T) {
	s := New(&fakeDep{err: errors.New("db down")}, nil, time.Second, time.Second)
	s.MarkStarted()
	if r := s.Live(); r.Status != "ok" {
		t.Fatalf("livez must stay ok when db is down, got %s", r.Status)
	}
	if r := s.Ready(context.Background()); Healthy(r) {
		t.Fatalf("readyz must fail when db is down: %+v", r)
	}
}

func TestDrainingFailsReadiness(t *testing.T) {
	s := New(&fakeDep{}, nil, time.Second, time.Second)
	s.MarkStarted()
	s.MarkDraining()
	if r := s.Ready(context.Background()); r.Status != "draining" || Healthy(r) {
		t.Fatalf("draining instance must not be ready: %+v", r)
	}
	if r := s.Live(); r.Status != "ok" {
		t.Fatalf("draining instance is still alive: %+v", r)
	}
}

func TestCacheDegradedNotFatal(t *testing.T) {
	cache := &fakeDep{err: errors.New("redis down")}
	s := New(&fakeDep{}, cache, time.Second, time.Second)
	s.MarkStarted()
	r := s.Ready(context.Background())
	if r.Status != "degraded" {
		t.Fatalf("want degraded when only cache is down, got %s", r.Status)
	}
	if !Healthy(r) {
		t.Fatalf("degraded must still receive traffic: %+v", r)
	}
}

func TestCacheAbsentIsOK(t *testing.T) {
	s := New(&fakeDep{}, nil, time.Second, time.Second)
	s.MarkStarted()
	r := s.Ready(context.Background())
	if r.Status != "ok" || !Healthy(r) {
		t.Fatalf("nil cache must be ok: %+v", r)
	}
	if !r.Checks["cache"].OK {
		t.Fatal("cache check should be ok when not configured")
	}
}
