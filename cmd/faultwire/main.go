package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rexagamer/faultwire/pkg/proxy"
)

func main() {
	listenAddr := flag.String("listen", ":8000", "Local TCP address to bind proxy on")
	upstreamAddr := flag.String("upstream", "127.0.0.1:9000", "Target upstream TCP address")
	dialTimeout := flag.Duration("dial-timeout", 3*time.Second, "Upstream connection timeout")
	bufferSize := flag.Int("buffer-size", 32768, "I/O chunk buffer size in bytes")
	flag.Parse()

	cfg := proxy.Config{
		ListenAddr:   *listenAddr,
		UpstreamAddr: *upstreamAddr,
		DialTimeout:  *dialTimeout,
		BufferSize:   *bufferSize,
	}

	server, err := proxy.New(cfg)
	if err != nil {
		log.Fatalf("[FATAL] Initialization error: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		if err := server.Start(ctx); err != nil {
			log.Printf("[ERROR] Server halted: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("[INFO] Interrupt received, draining sockets...")

	// Allow maximum 10 seconds for graceful connection drainage
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() {
		_ = server.Close()
		close(done)
	}()

	select {
	case <-done:
		log.Println("[INFO] Clean exit.")
	case <-shutdownCtx.Done():
		log.Println("[WARN] Shutdown timed out. Forcing termination.")
	}
}
