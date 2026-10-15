package engine

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// Action defines the chaos behavior applied to an intercepted TCP chunk.
type Action int

const (
	ActionPass Action = iota
	ActionDrop
	ActionCorrupt
	ActionDelay
	ActionFragment
)

func (a Action) String() string {
	switch a {
	case ActionPass:
		return "PASS"
	case ActionDrop:
		return "DROP (TCP RST)"
	case ActionCorrupt:
		return "CORRUPT (BIT-FLIP)"
	case ActionDelay:
		return "DELAY (JITTER)"
	case ActionFragment:
		return "FRAGMENT (TCP SLICE)"
	default:
		return "UNKNOWN"
	}
}

// Config holds the deterministic chaos parameters.
type Config struct {
	Seed         int64
	DropRate     float64       // 0.0 to 1.0
	CorruptRate  float64       // 0.0 to 1.0
	DelayRate    float64       // 0.0 to 1.0
	FragmentRate float64       // 0.0 to 1.0
	MinDelay     time.Duration // Minimum latency injection
	MaxDelay     time.Duration // Maximum latency injection
}

// Decision contains the pre-computed fault parameters for a single chunk.
type Decision struct {
	Action        Action
	Delay         time.Duration
	CorruptOffset int
	CorruptMask   byte
	SplitOffset   int
}

// Metrics tracks real-time proxy telemetry using lock-free atomic counters.
type Metrics struct {
	BytesForwarded  atomic.Uint64
	ActiveConns     atomic.Int64
	TotalConns      atomic.Uint64
	ChunksEvaluated atomic.Uint64
	Passes          atomic.Uint64
	Drops           atomic.Uint64
	Corruptions     atomic.Uint64
	Delays          atomic.Uint64
	Fragments       atomic.Uint64
}

// Engine drives deterministic fault injection.
type Engine struct {
	mu      sync.Mutex
	rng     *rand.Rand
	cfg     Config
	Metrics Metrics
}

// New creates a seeded FaultWire decision engine.
func New(cfg Config) *Engine {
	if cfg.MinDelay <= 0 {
		cfg.MinDelay = 50 * time.Millisecond
	}
	if cfg.MaxDelay < cfg.MinDelay {
		cfg.MaxDelay = cfg.MinDelay
	}
	return &Engine{
		rng: rand.New(rand.NewSource(cfg.Seed)),
		cfg: cfg,
	}
}

// Reset re-initializes the PRNG with its original seed for deterministic replay.
func (e *Engine) Reset() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.rng.Seed(e.cfg.Seed)
}

// Evaluate determines the exact fault to apply for a given byte chunk length.
func (e *Engine) Evaluate(chunkLen int) Decision {
	e.Metrics.ChunksEvaluated.Add(1)
	if chunkLen <= 0 {
		return Decision{Action: ActionPass}
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	roll := e.rng.Float64()
	cursor := e.cfg.DropRate

	// 1. Check Connection Drop
	if roll < cursor {
		e.Metrics.Drops.Add(1)
		return Decision{Action: ActionDrop}
	}

	// 2. Check Bit-Level Payload Corruption
	cursor += e.cfg.CorruptRate
	if roll < cursor {
		offset := e.rng.Intn(chunkLen)
		// Flip 1 random bit (bits 0-6 so ASCII characters stay printable/visible in demos)
		bit := e.rng.Intn(7)
		e.Metrics.Corruptions.Add(1)
		return Decision{
			Action:        ActionCorrupt,
			CorruptOffset: offset,
			CorruptMask:   byte(1 << bit),
		}
	}

	// 3. Check Latency Jitter Injection
	cursor += e.cfg.DelayRate
	if roll < cursor {
		delay := e.cfg.MinDelay
		if diff := e.cfg.MaxDelay - e.cfg.MinDelay; diff > 0 {
			delay += time.Duration(e.rng.Int63n(int64(diff)))
		}
		e.Metrics.Delays.Add(1)
		return Decision{
			Action: ActionDelay,
			Delay:  delay,
		}
	}

	// 4. Check TCP Packet Fragmentation
	cursor += e.cfg.FragmentRate
	if chunkLen > 1 && roll < cursor {
		split := 1 + e.rng.Intn(chunkLen-1)
		e.Metrics.Fragments.Add(1)
		return Decision{
			Action:      ActionFragment,
			SplitOffset: split,
			Delay:       10 * time.Millisecond,
		}
	}

	// 5. Healthy Pass-Through
	e.Metrics.Passes.Add(1)
	return Decision{Action: ActionPass}
}
