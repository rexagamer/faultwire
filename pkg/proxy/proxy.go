package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/rexagamer/faultwire/pkg/engine"
)

// Config defines the configuration for the FaultWire proxy server.
type Config struct {
	ListenAddr     string
	UpstreamAddr   string
	ControlAddr    string // Optional address for HTTP REST Control Plane (e.g., ":8081")
	DialTimeout    time.Duration
	BufferSize     int
	BandwidthLimit int64 // In bytes per second (0 = unlimited)
	Inbound        *engine.Engine
	Outbound       *engine.Engine
	Verbose        bool
}

// Server represents the fault-injecting TCP proxy.
type Server struct {
	cfg        Config
	inbound    *engine.Engine
	outbound   *engine.Engine
	pool       *BufferPool
	limiter    *TokenBucket
	listener   net.Listener
	ctrlServer *http.Server
	activeConn sync.WaitGroup
	mu         sync.Mutex
	closed     bool
}

// New initializes a proxy server.
func New(cfg Config) (*Server, error) {
	if cfg.ListenAddr == "" {
		return nil, errors.New("listen address cannot be empty")
	}
	if cfg.UpstreamAddr == "" {
		return nil, errors.New("upstream address cannot be empty")
	}
	if cfg.Inbound == nil {
		cfg.Inbound = engine.New(engine.Config{})
	}
	if cfg.Outbound == nil {
		cfg.Outbound = engine.New(engine.Config{})
	}
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = 5 * time.Second
	}
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = DefaultBufferSize
	}

	return &Server{
		cfg:      cfg,
		inbound:  cfg.Inbound,
		outbound: cfg.Outbound,
		pool:     NewBufferPool(cfg.BufferSize),
		limiter:  NewTokenBucket(cfg.BandwidthLimit),
	}, nil
}

// Addr returns the bound listener address.
func (s *Server) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}

// InboundEngine returns the client-to-upstream fault engine.
func (s *Server) InboundEngine() *engine.Engine {
	return s.inbound
}

// OutboundEngine returns the upstream-to-client fault engine.
func (s *Server) OutboundEngine() *engine.Engine {
	return s.outbound
}

// Start launches the TCP proxy and optional control plane listener.
func (s *Server) Start(ctx context.Context) error {
	lc := net.ListenConfig{}
	ln, err := lc.Listen(ctx, "tcp", s.cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("failed to bind proxy to %s: %w", s.cfg.ListenAddr, err)
	}

	s.mu.Lock()
	s.listener = ln
	s.mu.Unlock()

	if s.cfg.ControlAddr != "" {
		s.startControlPlane(ctx)
	}

	if s.cfg.Verbose {
		log.Printf("[FAULTWIRE] Listening on %s -> Upstream %s", ln.Addr().String(), s.cfg.UpstreamAddr)
	}

	go func() {
		<-ctx.Done()
		_ = s.Close()
	}()

	for {
		clientConn, err := ln.Accept()
		if err != nil {
			s.mu.Lock()
			isClosed := s.closed
			s.mu.Unlock()
			if isClosed {
				return nil
			}
			continue
		}

		s.inbound.Metrics.TotalConns.Add(1)
		s.inbound.Metrics.ActiveConns.Add(1)
		s.activeConn.Add(1)

		go func(c net.Conn) {
			defer s.activeConn.Done()
			defer s.inbound.Metrics.ActiveConns.Add(-1)
			s.handleConnection(ctx, c)
		}(clientConn)
	}
}

func (s *Server) handleConnection(ctx context.Context, clientConn net.Conn) {
	defer clientConn.Close()

	d := net.Dialer{Timeout: s.cfg.DialTimeout}
	upstreamConn, err := d.DialContext(ctx, "tcp", s.cfg.UpstreamAddr)
	if err != nil {
		if s.cfg.Verbose {
			log.Printf("[FAULTWIRE] Upstream connection failed: %v", err)
		}
		return
	}
	defer upstreamConn.Close()

	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(2)

	// Inbound: Client -> Upstream
	go func() {
		defer wg.Done()
		defer cancel()
		s.interceptPipe(connCtx, upstreamConn, clientConn, "CLIENT->UPSTREAM", s.inbound)
	}()

	// Outbound: Upstream -> Client
	go func() {
		defer wg.Done()
		defer cancel()
		s.interceptPipe(connCtx, clientConn, upstreamConn, "UPSTREAM->CLIENT", s.outbound)
	}()

	wg.Wait()
}

func (s *Server) interceptPipe(ctx context.Context, dst, src net.Conn, direction string, eng *engine.Engine) {
	bufPtr := s.pool.Get()
	defer s.pool.Put(bufPtr)
	buf := *bufPtr

	for {
		n, err := src.Read(buf)
		if n > 0 {
			chunk := buf[:n]

			// Apply Bandwidth Throttling if active
			if s.limiter != nil {
				if !s.limiter.Wait(ctx, n) {
					return
				}
			}

			decision := eng.Evaluate(n)
			switch decision.Action {
			case engine.ActionDrop:
				if s.cfg.Verbose {
					log.Printf("[CHAOS] %s | Severing connection with TCP RST (%d bytes dropped)", direction, n)
				}
				abortWithRST(src)
				abortWithRST(dst)
				return

			case engine.ActionCorrupt:
				orig := chunk[decision.CorruptOffset]
				chunk[decision.CorruptOffset] ^= decision.CorruptMask
				if s.cfg.Verbose {
					log.Printf("[CHAOS] %s | Bit-flip at offset %d: 0x%02x -> 0x%02x", direction, decision.CorruptOffset, orig, chunk[decision.CorruptOffset])
				}
				if _, wErr := dst.Write(chunk); wErr != nil {
					return
				}
				eng.Metrics.BytesForwarded.Add(uint64(n))

			case engine.ActionDelay:
				if s.cfg.Verbose {
					log.Printf("[CHAOS] %s | Injecting latency jitter: %v", direction, decision.Delay)
				}
				if !sleepContext(ctx, decision.Delay) {
					return
				}
				if _, wErr := dst.Write(chunk); wErr != nil {
					return
				}
				eng.Metrics.BytesForwarded.Add(uint64(n))

			case engine.ActionFragment:
				if s.cfg.Verbose {
					log.Printf("[CHAOS] %s | Slicing packet of %d bytes at offset %d", direction, n, decision.SplitOffset)
				}
				part1 := chunk[:decision.SplitOffset]
				part2 := chunk[decision.SplitOffset:]
				if _, wErr := dst.Write(part1); wErr != nil {
					return
				}
				if !sleepContext(ctx, decision.Delay) {
					return
				}
				if _, wErr := dst.Write(part2); wErr != nil {
					return
				}
				eng.Metrics.BytesForwarded.Add(uint64(n))

			case engine.ActionPass:
				if _, wErr := dst.Write(chunk); wErr != nil {
					return
				}
				eng.Metrics.BytesForwarded.Add(uint64(n))
			}
		}

		if err != nil {
			if errors.Is(err, io.EOF) {
				if tcpDst, ok := dst.(*net.TCPConn); ok {
					_ = tcpDst.CloseWrite()
				}
			}
			return
		}
	}
}

func (s *Server) startControlPlane(ctx context.Context) {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK\n"))
	})

	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		payload := map[string]any{
			"inbound":  s.inbound.Snapshot(),
			"outbound": s.outbound.Snapshot(),
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(payload)
	})

	mux.HandleFunc("POST /reset", func(w http.ResponseWriter, r *http.Request) {
		s.inbound.Reset()
		s.outbound.Reset()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"engines_reset"}` + "\n"))
	})

	mux.HandleFunc("POST /config", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Inbound  *engine.Config `json:"inbound"`
			Outbound *engine.Config `json:"outbound"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if body.Inbound != nil {
			s.inbound.UpdateConfig(*body.Inbound)
		}
		if body.Outbound != nil {
			s.outbound.UpdateConfig(*body.Outbound)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"config_updated"}` + "\n"))
	})

	s.ctrlServer = &http.Server{
		Addr:    s.cfg.ControlAddr,
		Handler: mux,
	}

	go func() {
		if err := s.ctrlServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("[FAULTWIRE-CTRL] Server error: %v", err)
		}
	}()
}

// abortWithRST configures SO_LINGER to 0, forcing immediate TCP RST issuance.
func abortWithRST(conn net.Conn) {
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		_ = tcpConn.SetLinger(0)
	}
	_ = conn.Close()
}

func sleepContext(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// Close stops the proxy and waits for ongoing connections to drain.
func (s *Server) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	var err error
	if s.listener != nil {
		err = s.listener.Close()
	}
	if s.ctrlServer != nil {
		_ = s.ctrlServer.Close()
	}
	s.mu.Unlock()

	s.activeConn.Wait()
	return err
}
