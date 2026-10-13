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
)

// Config defines the parameters for the pass-through proxy.
type Config struct {
	ListenAddr     string
	UpstreamAddr   string
	DialTimeout    time.Duration
	BufferSize     int
}

// Server represents the core TCP proxy instance.
type Server struct {
	cfg        Config
	pool       *BufferPool
	listener   net.Listener
	activeConn sync.WaitGroup
	mu         sync.Mutex
	closed     bool
}

// New creates an unstarted proxy server instance.
func New(cfg Config) (*Server, error) {
	if cfg.ListenAddr == "" {
		return nil, errors.New("listen address cannot be empty")
	}
	if cfg.UpstreamAddr == "" {
		return nil, errors.New("upstream address cannot be empty")
	}
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = 5 * time.Second
	}
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = DefaultBufferSize
	}

	return &Server{
		cfg:  cfg,
		pool: NewBufferPool(cfg.BufferSize),
	}, nil
}

// Start initiates the listener and blocks until ctx is canceled or the listener encounters an error.
func (s *Server) Start(ctx context.Context) error {
	lc := net.ListenConfig{}
	ln, err := lc.Listen(ctx, "tcp", s.cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("failed to bind to %s: %w", s.cfg.ListenAddr, err)
	}

	s.mu.Lock()
	s.listener = ln
	s.mu.Unlock()

	log.Printf("[PROXY] Listening on %s -> Forwarding to %s", s.cfg.ListenAddr, s.cfg.UpstreamAddr)

	// Background worker to close listener when the context is cancelled
	go func() {
		<-ctx.Done()
		s.Close()
	}()

	for {
		clientConn, err := ln.Accept()
		if err != nil {
			s.mu.Lock()
			isClosed := s.closed
			s.mu.Unlock()
			if isClosed {
				return nil // Clean termination
			}
			log.Printf("[PROXY] Accept error: %v", err)
			continue
		}

		s.activeConn.Add(1)
		go func(c net.Conn) {
			defer s.activeConn.Done()
			s.handleConnection(ctx, c)
		}(clientConn)
	}
}

// handleConnection establishes the upstream socket and joins both endpoints.
func (s *Server) handleConnection(ctx context.Context, clientConn net.Conn) {
	defer clientConn.Close()

	d := net.Dialer{Timeout: s.cfg.DialTimeout}
	upstreamConn, err := d.DialContext(ctx, "tcp", s.cfg.UpstreamAddr)
	if err != nil {
		log.Printf("[PROXY] Dial upstream failed (%s): %v", s.cfg.UpstreamAddr, err)
		return
	}
	defer upstreamConn.Close()

	// Coordinate bidirectional completion
	var wg sync.WaitGroup
	wg.Add(2)

	// Stream 1: Client -> Upstream
	go func() {
		defer wg.Done()
		s.pipe(upstreamConn, clientConn)
	}()

	// Stream 2: Upstream -> Client
	go func() {
		defer wg.Done()
		s.pipe(clientConn, upstreamConn)
	}()

	wg.Wait()
}

// pipe transfers data from src to dst using a pooled buffer, correctly propagating TCP FIN flags.
func (s *Server) pipe(dst, src net.Conn) {
	bufPtr := s.pool.Get()
	defer s.pool.Put(bufPtr)

	// Copy data using the pooled buffer
	_, _ = io.CopyBuffer(dst, src, *bufPtr)

	// Propagate EOF cleanly if supported by underlying TCP socket
	if tcpDst, ok := dst.(*net.TCPConn); ok {
		_ = tcpDst.CloseWrite()
	}
}

// Close gracefully stops the listener and drains active connections.
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

	// Wait for active in-flight pipes to exit
	s.activeConn.Wait()
	log.Printf("[PROXY] All active connections drained. Shutdown complete.")
	return err
}