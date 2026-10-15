package engine

import (
	"sync"
	"testing"
	"time"
)

func TestDeterministicReplay(t *testing.T) {
	cfg := Config{
		Seed:         9999,
		DropRate:     0.15,
		CorruptRate:  0.20,
		DelayRate:    0.25,
		FragmentRate: 0.15,
		MinDelay:     10 * time.Millisecond,
		MaxDelay:     200 * time.Millisecond,
	}

	eng := New(cfg)
	const iterations = 1000

	run1 := make([]Decision, iterations)
	for i := 0; i < iterations; i++ {
		run1[i] = eng.Evaluate(128)
	}

	// Reset to initial seed and replay
	eng.Reset()

	for i := 0; i < iterations; i++ {
		d := eng.Evaluate(128)
		if d != run1[i] {
			t.Fatalf("Determinism broken at step %d: got %+v, want %+v", i, d, run1[i])
		}
	}
}

func TestThreadSafetyUnderConcurrency(t *testing.T) {
	eng := New(Config{
		Seed:        42,
		DropRate:    0.1,
		CorruptRate: 0.1,
		DelayRate:   0.1,
	})

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = eng.Evaluate(64)
			}
		}()
	}
	wg.Wait()

	if got := eng.Metrics.ChunksEvaluated.Load(); got != 5000 {
		t.Fatalf("expected 5000 chunks evaluated, got %d", got)
	}
}
