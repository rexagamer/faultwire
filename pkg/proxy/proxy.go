package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"faultwire/pkg/engine"
)

// Config defines the runtime configuration for the FaultWire proxy server.
type Config struct {
	ListenAddr   string
	UpstreamAddr string
	DialTimeout  time.Duration
	BufferSize   int
	Verbose      bool
}

// Server represents the fault-injecting TCP proxy.
type Server struct {
	cfg        Config
	engine     *engine.Engine
	pool       *BufferPool
	listener   net.Listener
	activeConn sync.WaitGroup
	mu         sync.Mutex
	closed     bool
}

// New initializes a new FaultWire proxy server.
func New(cfg Config, eng *engine.Engine) (*Server, error) {
	if cfg.ListenAddr == "" {
		return nil, errors.New("listen address cannot be empty")
	}
	if cfg.UpstreamAddr == "" {
		return nil, errors.New("upstream address cannot be empty")
	}
	if eng == nil {
		return nil, errors.New("fault engine cannot be nil")
	}
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = 5 * time.Second
	}
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = DefaultBufferSize
	}

	return &Server{
		cfg:    cfg,
		engine: eng,
		pool:   NewBufferPool(cfg.BufferSize),
	}, nil
}

// Addr returns the bound listener network address (useful when binding to port :0 in tests).
func (s *Server) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}

// Start binds the TCP port and accepts connections until ctx is canceled.
func (s *Server) Start(ctx context.Context) error {
	lc := net.ListenConfig{}
	ln, err := lc.Listen(ctx, "tcp", s.cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("failed to bind to %s: %w", s.cfg.ListenAddr, err)
	}

	s.mu.Lock()
	s.listener = ln
	s.mu.Unlock()

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

		s.engine.Metrics.TotalConns.Add(1)
		s.engine.Metrics.ActiveConns.Add(1)
		s.activeConn.Add(1)

		go func(c net.Conn) {
			defer s.activeConn.Done()
			defer s.engine.Metrics.ActiveConns.Add(-1)
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
			log.Printf("[FAULTWIRE] Upstream dial failed: %v", err)
		}
		return
	}
	defer upstreamConn.Close()

	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(2)

	// Client -> Upstream (Chaos Enabled)
	go func() {
		defer wg.Done()
		defer cancel()
		s.interceptPipe(connCtx, upstreamConn, clientConn, "CLIENT->UPSTREAM", true)
	}()

	// Upstream -> Client (Pass-through so upstream responses return cleanly)
	go func() {
		defer wg.Done()
		defer cancel()
		s.interceptPipe(connCtx, clientConn, upstreamConn, "UPSTREAM->CLIENT", false)
	}()

	wg.Wait()
}

func (s *Server) interceptPipe(ctx context.Context, dst, src net.Conn, direction string, injectFaults bool) {
	bufPtr := s.pool.Get()
	defer s.pool.Put(bufPtr)
	buf := *bufPtr

	for {
		n, err := src.Read(buf)
		if n > 0 {
			chunk := buf[:n]

			if !injectFaults {
				if _, wErr := dst.Write(chunk); wErr != nil {
					return
				}
				s.engine.Metrics.BytesForwarded.Add(uint64(n))
			} else {
				decision := s.engine.Evaluate(n)

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
						log.Printf("[CHAOS] %s | Bit-flip at byte[%d]: 0x%02x -> 0x%02x", direction, decision.CorruptOffset, orig, chunk[decision.CorruptOffset])
					}
					if _, wErr := dst.Write(chunk); wErr != nil {
						return
					}
					s.engine.Metrics.BytesForwarded.Add(uint64(n))

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
					s.engine.Metrics.BytesForwarded.Add(uint64(n))

				case engine.ActionFragment:
					if s.cfg.Verbose {
						log.Printf("[CHAOS] %s | Fragmenting %d bytes at offset %d", direction, n, decision.SplitOffset)
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
					s.engine.Metrics.BytesForwarded.Add(uint64(n))

				case engine.ActionPass:
					if _, wErr := dst.Write(chunk); wErr != nil {
						return
					}
					s.engine.Metrics.BytesForwarded.Add(uint64(n))
				}
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

// abortWithRST sets SO_LINGER to 0, forcing the OS kernel to send an abrupt TCP RST packet.
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

// Close stops accepting new connections and waits for active connections to finish.
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
	s.mu.Unlock()

	s.activeConn.Wait()
	return err
}
