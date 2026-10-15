package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"faultwire/pkg/engine"
	"faultwire/pkg/proxy"
)

func main() {
	listenAddr := flag.String("listen", ":8000", "Local TCP address to bind proxy")
	upstreamAddr := flag.String("upstream", "127.0.0.1:9000", "Target upstream TCP address")
	seed := flag.Int64("seed", 1337, "Deterministic PRNG seed")
	dropRate := flag.Float64("drop", 0.0, "Probability of dropping connection with TCP RST (0.0 - 1.0)")
	corruptRate := flag.Float64("corrupt", 0.0, "Probability of bit-flipping payload bytes (0.0 - 1.0)")
	delayRate := flag.Float64("delay", 0.0, "Probability of injecting latency jitter (0.0 - 1.0)")
	fragmentRate := flag.Float64("fragment", 0.0, "Probability of slicing TCP packets (0.0 - 1.0)")
	minDelay := flag.Duration("min-delay", 50*time.Millisecond, "Minimum latency jitter duration")
	maxDelay := flag.Duration("max-delay", 400*time.Millisecond, "Maximum latency jitter duration")
	verbose := flag.Bool("verbose", true, "Log every chaos event in real time")
	flag.Parse()

	eng := engine.New(engine.Config{
		Seed:         *seed,
		DropRate:     *dropRate,
		CorruptRate:  *corruptRate,
		DelayRate:    *delayRate,
		FragmentRate: *fragmentRate,
		MinDelay:     *minDelay,
		MaxDelay:     *maxDelay,
	})

	srv, err := proxy.New(proxy.Config{
		ListenAddr:   *listenAddr,
		UpstreamAddr: *upstreamAddr,
		DialTimeout:  3 * time.Second,
		Verbose:      *verbose,
	}, eng)
	if err != nil {
		log.Fatalf("[FATAL] %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Printf("[INIT] Seed=%d | Drop=%.0f%% | Corrupt=%.0f%% | Delay=%.0f%% (%v-%v) | Fragment=%.0f%%",
		*seed, *dropRate*100, *corruptRate*100, *delayRate*100, *minDelay, *maxDelay, *fragmentRate*100)

	// Background Telemetry Reporter
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				m := &eng.Metrics
				log.Printf("[TELEMETRY] Active=%d | TotalConns=%d | Bytes=%d | Pass=%d | Drop=%d | Corrupt=%d | Delay=%d | Frag=%d",
					m.ActiveConns.Load(), m.TotalConns.Load(), m.BytesForwarded.Load(),
					m.Passes.Load(), m.Drops.Load(), m.Corruptions.Load(), m.Delays.Load(), m.Fragments.Load())
			case <-ctx.Done():
				return
			}
		}
	}()

	go func() {
		if err := srv.Start(ctx); err != nil {
			log.Printf("[ERROR] %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("[SHUTDOWN] Interrupt received, draining active connections...")
	_ = srv.Close()
	log.Println("[SHUTDOWN] Clean exit.")
}
