package faultwire_test

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/rexagamer/faultwire"
)

// Example demonstrates launching a deterministic proxy and executing verified chaos.
func Example() {
	// 1. Mock Echo Server
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	defer ln.Close()

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				s, _ := bufio.NewReader(c).ReadString('\n')
				_, _ = c.Write([]byte(s))
			}(conn)
		}
	}()

	// 2. Initialize FaultWire Engine with fixed Seed
	eng := faultwire.NewEngine(faultwire.Config{
		Seed:      42,
		DelayRate: 1.0, // Force 100% latency injection
		MinDelay:  20 * time.Millisecond,
		MaxDelay:  30 * time.Millisecond,
	})

	srv, err := faultwire.NewServer(faultwire.ServerConfig{
		ListenAddr:   "127.0.0.1:0",
		UpstreamAddr: ln.Addr().String(),
		Inbound:      eng,
	})
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = srv.Start(ctx)
	}()
	time.Sleep(30 * time.Millisecond) // Wait for bind

	// 3. Connect client through proxy
	conn, err := net.Dial("tcp", srv.Addr())
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	start := time.Now()
	_, _ = conn.Write([]byte("PING\n"))
	resp, _ := bufio.NewReader(conn).ReadString('\n')
	elapsed := time.Since(start)

	isDelayed := elapsed >= 20*time.Millisecond
	fmt.Printf("Received: %s\n", strings.TrimSpace(resp))
	fmt.Printf("Delayed: %t\n", isDelayed)

	// Output:
	// Received: PING
	// Delayed: true
}
