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
	Seed         int64         `json:"seed"`
	DropRate     float64       `json:"drop_rate"`     // 0.0 to 1.0
	CorruptRate  float64       `json:"corrupt_rate"`  // 0.0 to 1.0
	DelayRate    float64       `json:"delay_rate"`    // 0.0 to 1.0
	FragmentRate float64       `json:"fragment_rate"` // 0.0 to 1.0
	MinDelay     time.Duration `json:"min_delay"`     // Minimum latency injection
	MaxDelay     time.Duration `json:"max_delay"`     // Maximum latency injection
}

// Decision contains pre-computed fault instructions for a chunk.
type Decision struct {
	Action        Action
	Delay         time.Duration
	CorruptOffset int
	CorruptMask   byte
	SplitOffset   int
}

// Metrics tracks real-time proxy telemetry using lock-free atomic counters.
type Metrics struct {
	BytesForwarded  atomic.Uint64 `json:"bytes_forwarded"`
	ActiveConns     atomic.Int64  `json:"active_conns"`
	TotalConns      atomic.Uint64 `json:"total_conns"`
	ChunksEvaluated atomic.Uint64 `json:"chunks_evaluated"`
	Passes          atomic.Uint64 `json:"passes"`
	Drops           atomic.Uint64 `json:"drops"`
	Corruptions     atomic.Uint64 `json:"corruptions"`
	Delays          atomic.Uint64 `json:"delays"`
	Fragments       atomic.Uint64 `json:"fragments"`
}

// Snapshot exports metrics into a clean serializable structure.
type MetricsSnapshot struct {
	BytesForwarded  uint64 `json:"bytes_forwarded"`
	ActiveConns     int64  `json:"active_conns"`
	TotalConns      uint64 `json:"total_conns"`
	ChunksEvaluated uint64 `json:"chunks_evaluated"`
	Passes          uint64 `json:"passes"`
	Drops           uint64 `json:"drops"`
	Corruptions     uint64 `json:"corruptions"`
	Delays          uint64 `json:"delays"`
	Fragments       uint64 `json:"fragments"`
}

// Engine executes deterministic fault decisions.
type Engine struct {
	mu      sync.Mutex
	rng     *rand.Rand
	cfg     Config
	Metrics Metrics
}

// New creates an Engine instance.
func New(cfg Config) *Engine {
	normalizeConfig(&cfg)
	return &Engine{
		rng: rand.New(rand.NewSource(cfg.Seed)),
		cfg: cfg,
	}
}

func normalizeConfig(cfg *Config) {
	if cfg.MinDelay <= 0 {
		cfg.MinDelay = 50 * time.Millisecond
	}
	if cfg.MaxDelay < cfg.MinDelay {
		cfg.MaxDelay = cfg.MinDelay
	}
}

// UpdateConfig updates the engine parameters safely in flight.
func (e *Engine) UpdateConfig(newCfg Config) {
	normalizeConfig(&newCfg)
	e.mu.Lock()
	defer e.mu.Unlock()
	e.cfg = newCfg
	if newCfg.Seed != 0 {
		e.rng = rand.New(rand.NewSource(newCfg.Seed))
	}
}

// GetConfig returns a copy of current engine parameters.
func (e *Engine) GetConfig() Config {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.cfg
}

// Reset rewinds the internal PRNG stream back to the initial seed.
func (e *Engine) Reset() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.rng.Seed(e.cfg.Seed)
}

// Snapshot returns atomic telemetry values.
func (e *Engine) Snapshot() MetricsSnapshot {
	return MetricsSnapshot{
		BytesForwarded:  e.Metrics.BytesForwarded.Load(),
		ActiveConns:     e.Metrics.ActiveConns.Load(),
		TotalConns:      e.Metrics.TotalConns.Load(),
		ChunksEvaluated: e.Metrics.ChunksEvaluated.Load(),
		Passes:          e.Metrics.Passes.Load(),
		Drops:           e.Metrics.Drops.Load(),
		Corruptions:     e.Metrics.Corruptions.Load(),
		Delays:          e.Metrics.Delays.Load(),
		Fragments:       e.Metrics.Fragments.Load(),
	}
}

// Evaluate calculates which fault action applies to an incoming byte chunk.
func (e *Engine) Evaluate(chunkLen int) Decision {
	e.Metrics.ChunksEvaluated.Add(1)
	if chunkLen <= 0 {
		return Decision{Action: ActionPass}
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	roll := e.rng.Float64()
	cursor := e.cfg.DropRate

	// 1. Connection Drop (TCP RST)
	if roll < cursor {
		e.Metrics.Drops.Add(1)
		return Decision{Action: ActionDrop}
	}

	// 2. Bitwise Payload Corruption
	cursor += e.cfg.CorruptRate
	if roll < cursor {
		offset := e.rng.Intn(chunkLen)
		bit := e.rng.Intn(7) // Bits 0-6 keep printable ASCII visible
		e.Metrics.Corruptions.Add(1)
		return Decision{
			Action:        ActionCorrupt,
			CorruptOffset: offset,
			CorruptMask:   byte(1 << bit),
		}
	}

	// 3. Latency Jitter
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

	// 4. TCP Packet Slicing (Fragmentation)
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

	// 5. Normal Forwarding
	e.Metrics.Passes.Add(1)
	return Decision{Action: ActionPass}
}
