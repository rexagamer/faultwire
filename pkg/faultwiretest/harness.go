package faultwiretest

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/rexagamer/faultwire/pkg/engine"
	"github.com/rexagamer/faultwire/pkg/proxy"
)

// Option configures test harness behavior.
type Option func(*proxy.Config)

func WithSeed(seed int64) Option {
	return func(c *proxy.Config) {
		c.Inbound.UpdateConfig(engine.Config{Seed: seed})
	}
}

func WithDropRate(rate float64) Option {
	return func(c *proxy.Config) {
		cfg := c.Inbound.GetConfig()
		cfg.DropRate = rate
		c.Inbound.UpdateConfig(cfg)
	}
}

func WithCorruptRate(rate float64) Option {
	return func(c *proxy.Config) {
		cfg := c.Inbound.GetConfig()
		cfg.CorruptRate = rate
		c.Inbound.UpdateConfig(cfg)
	}
}

func WithDelay(minDelay, maxDelay time.Duration, rate float64) Option {
	return func(c *proxy.Config) {
		cfg := c.Inbound.GetConfig()
		cfg.DelayRate = rate
		cfg.MinDelay = minDelay
		cfg.MaxDelay = maxDelay
		c.Inbound.UpdateConfig(cfg)
	}
}

// Harness wraps a running test proxy server.
type Harness struct {
	server *proxy.Server
	cancel context.CancelFunc
}

// NewServer initializes an in-memory FaultWire server automatically tied to testing.TB lifecycle.
func NewServer(t testing.TB, upstreamAddr string, opts ...Option) *Harness {
	t.Helper()

	inboundEng := engine.New(engine.Config{})
	outboundEng := engine.New(engine.Config{})

	cfg := proxy.Config{
		ListenAddr:   "127.0.0.1:0",
		UpstreamAddr: upstreamAddr,
		Inbound:      inboundEng,
		Outbound:     outboundEng,
		DialTimeout:  2 * time.Second,
	}

	for _, opt := range opts {
		opt(&cfg)
	}

	srv, err := proxy.New(cfg)
	if err != nil {
		t.Fatalf("failed to create faultwire test server: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		_ = srv.Start(ctx)
	}()

	// Wait until listening
	for i := 0; i < 50; i++ {
		if srv.Addr() != "" {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	h := &Harness{server: srv, cancel: cancel}
	t.Cleanup(func() {
		h.Close()
	})

	return h
}

func (h *Harness) Addr() string {
	return h.server.Addr()
}

func (h *Harness) Reset() {
	h.server.InboundEngine().Reset()
	h.server.OutboundEngine().Reset()
}

func (h *Harness) Close() {
	h.cancel()
	_ = h.server.Close()
}

// Dial connects directly to the proxy instance.
func (h *Harness) Dial() (net.Conn, error) {
	return net.DialTimeout("tcp", h.Addr(), time.Second)
}
