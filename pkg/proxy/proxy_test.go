package proxy

import (
	"bufio"
	"context"
	"net"
	"testing"
	"time"

	"github.com/rexagamer/faultwire/pkg/engine"
)

func TestBidirectionalProxy(t *testing.T) {
	// 1. Upstream Echo Server
	upstreamLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer upstreamLn.Close()

	go func() {
		for {
			conn, err := upstreamLn.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				s, _ := bufio.NewReader(c).ReadString('\n')
				_, _ = c.Write([]byte("ECHO:" + s))
			}(conn)
		}
	}()

	// 2. Start Proxy with clean pass-through
	srv, err := New(Config{
		ListenAddr:   "127.0.0.1:0",
		UpstreamAddr: upstreamLn.Addr().String(),
		Inbound:      engine.New(engine.Config{}),
		Outbound:     engine.New(engine.Config{}),
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = srv.Start(ctx)
	}()
	time.Sleep(20 * time.Millisecond)

	// 3. Dial Proxy
	conn, err := net.Dial("tcp", srv.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	_, err = conn.Write([]byte("HELLO\n"))
	if err != nil {
		t.Fatal(err)
	}

	reply, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}

	if reply != "ECHO:HELLO\n" {
		t.Fatalf("expected 'ECHO:HELLO\\n', got %q", reply)
	}
}
