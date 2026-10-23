package faultwire

import (
	"time"

	"github.com/rexagamer/faultwire/pkg/engine"
	"github.com/rexagamer/faultwire/pkg/proxy"
)

// Re-export core types for root import ergonomics.
type (
	Action       = engine.Action
	Config       = engine.Config
	Decision     = engine.Decision
	Metrics      = engine.Metrics
	Engine       = engine.Engine
	ServerConfig = proxy.Config
	Server       = proxy.Server
)

const (
	ActionPass     = engine.ActionPass
	ActionDrop     = engine.ActionDrop
	ActionCorrupt  = engine.ActionCorrupt
	ActionDelay    = engine.ActionDelay
	ActionFragment = engine.ActionFragment
)

// NewEngine initializes a deterministic fault-injection engine.
func NewEngine(cfg Config) *Engine {
	return engine.New(cfg)
}

// NewServer initializes a fault-injecting TCP proxy server.
func NewServer(cfg ServerConfig) (*Server, error) {
	return proxy.New(cfg)
}

// Profile defines standard pre-configured chaos presets.
type Profile string

const (
	ProfileMobileFlaky      Profile = "mobile-flaky"
	ProfileCrossRegionWAN   Profile = "cross-region-wan"
	ProfileNICHardwareFault Profile = "nic-fault"
	ProfileHighJitter       Profile = "high-jitter"
)

// LoadProfile returns a battle-tested engine configuration for common failure modes.
func LoadProfile(p Profile, seed int64) Config {
	switch p {
	case ProfileMobileFlaky:
		return Config{
			Seed:         seed,
			DropRate:     0.10,
			CorruptRate:  0.03,
			DelayRate:    0.35,
			FragmentRate: 0.15,
			MinDelay:     150 * time.Millisecond,
			MaxDelay:     600 * time.Millisecond,
		}
	case ProfileCrossRegionWAN:
		return Config{
			Seed:      seed,
			DropRate:  0.02,
			DelayRate: 0.90,
			MinDelay:  120 * time.Millisecond,
			MaxDelay:  220 * time.Millisecond,
		}
	case ProfileNICHardwareFault:
		return Config{
			Seed:         seed,
			DropRate:     0.15,
			CorruptRate:  0.30,
			FragmentRate: 0.20,
		}
	case ProfileHighJitter:
		return Config{
			Seed:      seed,
			DelayRate: 0.50,
			MinDelay:  10 * time.Millisecond,
			MaxDelay:  800 * time.Millisecond,
		}
	default:
		return Config{Seed: seed}
	}
}
