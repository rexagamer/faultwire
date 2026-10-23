package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rexagamer/faultwire"
)

type ConfigFile struct {
	Listen      string            `json:"listen"`
	Upstream    string            `json:"upstream"`
	Control     string            `json:"control"`
	Seed        int64             `json:"seed"`
	Profile     string            `json:"profile"`
	RateLimitKB int64             `json:"rate_limit_kb"`
	Inbound     *faultwire.Config `json:"inbound"`
	Outbound    *faultwire.Config `json:"outbound"`
}

func main() {
	listenAddr := flag.String("listen", ":8000", "Local TCP address to bind proxy")
	upstreamAddr := flag.String("upstream", "127.0.0.1:9000", "Target upstream TCP address")
	ctrlAddr := flag.String("control", ":8081", "Address for HTTP REST Control Plane (empty to disable)")
	configPath := flag.String("config", "", "Path to optional JSON configuration file")
	profileName := flag.String("profile", "", "Preset profile: mobile-flaky | cross-region-wan | nic-fault | high-jitter")
	seed := flag.Int64("seed", 1337, "Deterministic PRNG seed")
	dropRate := flag.Float64("drop", 0.0, "Probability of dropping connection with TCP RST (0.0 - 1.0)")
	corruptRate := flag.Float64("corrupt", 0.0, "Probability of bit-flipping payload bytes (0.0 - 1.0)")
	delayRate := flag.Float64("delay", 0.0, "Probability of injecting latency jitter (0.0 - 1.0)")
	fragmentRate := flag.Float64("fragment", 0.0, "Probability of slicing TCP packets (0.0 - 1.0)")
	minDelay := flag.Duration("min-delay", 50*time.Millisecond, "Minimum latency jitter duration")
	maxDelay := flag.Duration("max-delay", 400*time.Millisecond, "Maximum latency jitter duration")
	bandwidthKB := flag.Int64("bandwidth-kb", 0, "Max bandwidth throttle in KB/sec (0 = unlimited)")
	verbose := flag.Bool("verbose", true, "Log every chaos event in real time")
	flag.Parse()

	inboundCfg := faultwire.Config{
		Seed:         *seed,
		DropRate:     *dropRate,
		CorruptRate:  *corruptRate,
		DelayRate:    *delayRate,
		FragmentRate: *fragmentRate,
		MinDelay:     *minDelay,
		MaxDelay:     *maxDelay,
	}

	if *profileName != "" {
		inboundCfg = faultwire.LoadProfile(faultwire.Profile(*profileName), *seed)
		log.Printf("[INIT] Applying preset chaos profile: %s", *profileName)
	}

	outboundCfg := faultwire.Config{Seed: *seed + 1}
	rateLimitBytes := *bandwidthKB * 1024

	// Load file config overrides if specified
	if *configPath != "" {
		data, err := os.ReadFile(*configPath)
		if err != nil {
			log.Fatalf("[FATAL] Failed to read config file: %v", err)
		}
		var fileCfg ConfigFile
		if err := json.Unmarshal(data, &fileCfg); err != nil {
			log.Fatalf("[FATAL] Invalid config JSON: %v", err)
		}
		if fileCfg.Listen != "" {
			*listenAddr = fileCfg.Listen
		}
		if fileCfg.Upstream != "" {
			*upstreamAddr = fileCfg.Upstream
		}
		if fileCfg.Control != "" {
			*ctrlAddr = fileCfg.Control
		}
		if fileCfg.RateLimitKB > 0 {
			rateLimitBytes = fileCfg.RateLimitKB * 1024
		}
		if fileCfg.Profile != "" {
			inboundCfg = faultwire.LoadProfile(faultwire.Profile(fileCfg.Profile), fileCfg.Seed)
		}
		if fileCfg.Inbound != nil {
			inboundCfg = *fileCfg.Inbound
		}
		if fileCfg.Outbound != nil {
			outboundCfg = *fileCfg.Outbound
		}
	}

	inboundEng := faultwire.NewEngine(inboundCfg)
	outboundEng := faultwire.NewEngine(outboundCfg)

	srv, err := faultwire.NewServer(faultwire.ServerConfig{
		ListenAddr:     *listenAddr,
		UpstreamAddr:   *upstreamAddr,
		ControlAddr:    *ctrlAddr,
		BandwidthLimit: rateLimitBytes,
		Inbound:        inboundEng,
		Outbound:       outboundEng,
		DialTimeout:    3 * time.Second,
		Verbose:        *verbose,
	})
	if err != nil {
		log.Fatalf("[FATAL] %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Printf("[INIT] Seed=%d | Control=%s | Drop=%.0f%% | Corrupt=%.0f%% | Delay=%.0f%% (%v-%v) | Throttle=%d KB/s",
		inboundCfg.Seed, *ctrlAddr, inboundCfg.DropRate*100, inboundCfg.CorruptRate*100,
		inboundCfg.DelayRate*100, inboundCfg.MinDelay, inboundCfg.MaxDelay, *bandwidthKB)

	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				m := inboundEng.Snapshot()
				log.Printf("[TELEMETRY] Conns=%d | Bytes=%d | Pass=%d | Drop=%d | Corrupt=%d | Delay=%d | Frag=%d",
					m.ActiveConns, m.BytesForwarded, m.Passes, m.Drops, m.Corruptions, m.Delays, m.Fragments)
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
	log.Println("[SHUTDOWN] Signal received, draining active connections...")
	_ = srv.Close()
	log.Println("[SHUTDOWN] Clean exit.")
}
