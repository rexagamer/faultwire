// Package faultwire provides a high-performance, deterministic Layer 4 TCP fault-injection
// proxy engineered for distributed systems testing, chaos engineering, and resilience verification.
//
// By operating transparently on raw TCP byte streams without third-party dependencies,
// FaultWire is agnostic to upper-layer application protocols (HTTP/1.1, HTTP/2, gRPC,
// PostgreSQL, Redis, Kafka, and custom binary formats).
//
// # Key Capabilities
//
//   - Deterministic Chaos Replay: Seed-based pseudo-random number generator (PRNG) guaranteeing
//     100% reproducible fault sequences across automated test runs.
//   - Bi-Directional Fault Injection: Independent fault profiles for inbound (Client -> Upstream)
//     and outbound (Upstream -> Client) data paths.
//   - Kernel-Level TCP RST Aborts: Forces OS socket SO_LINGER to 0, terminating connections
//     abruptly with an immediate RST packet rather than a standard FIN handshake.
//   - Payload Bit-Flipping: Simulates noisy hardware, NIC corruption, and bit rot via in-place
//     byte XOR mutations.
//   - Bandwidth Throttling: Token-bucket rate limiting to simulate high-latency, constrained pipes.
//   - Runtime Control Plane: Embedded HTTP REST API to inspect metrics, adjust fault rates,
//     and reset seeds on running instances without downtime.
//   - Zero Heap Allocation Hot Path: Recycled 32 KB I/O buffers backed by sync.Pool.
//
// # Quick Start
//
//	package main
//
//	import (
//		"context"
//		"time"
//		"github.com/rexagamer/faultwire"
//	)
//
//	func main() {
//		eng := faultwire.NewEngine(faultwire.Config{
//			Seed:        42,
//			DropRate:    0.10,
//			CorruptRate: 0.05,
//			DelayRate:   0.20,
//			MinDelay:    50 * time.Millisecond,
//			MaxDelay:    200 * time.Millisecond,
//		})
//
//		srv, err := faultwire.NewServer(faultwire.ServerConfig{
//			ListenAddr:   ":8000",
//			UpstreamAddr: "127.0.0.1:9000",
//			Inbound:      eng,
//		})
//		if err != nil {
//			panic(err)
//		}
//
//		_ = srv.Start(context.Background())
//	}
package faultwire
