# FaultWire

[![Go Version](https://img.shields.io/badge/Go-1.22+-00ADD8?style=flat&logo=go)](https://go.dev/)
[![Go Reference](https://pkg.go.dev/badge/github.com/rexagamer/faultwire.svg)](https://pkg.go.dev/github.com/rexagamer/faultwire)
[![Dependencies](https://img.shields.io/badge/dependencies-zero-brightgreen.svg)](#architecture)
[![CI Status](https://github.com/rexagamer/faultwire/actions/workflows/ci.yml/badge.svg)](https://github.com/rexagamer/faultwire/actions)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

**FaultWire** is a deterministic, zero-dependency Layer 4 TCP fault-injection proxy written in Go.

Distributed systems rarely fail in clean, reproducible ways. When production systems crash from half-open sockets, bit-flipped wire payloads, unhandled packet fragmentation, or latency spikes, reproducing that exact failure sequence locally is notoriously difficult. FaultWire sits between client applications and backend services, intercepting raw TCP byte streams and injecting **seeded, deterministic network chaos**—allowing engineering teams to replay exact failure sequences on demand.

---

## Why FaultWire?

| Capability | FaultWire | Shopify Toxiproxy | Chaos Mesh |
|---|---|---|---|
| **Zero Dependencies** | Yes (pure Go standard library) | No (external runtime) | No (requires Kubernetes) |
| **Deterministic PRNG Replay** | Yes (`--seed 42`) | No | No |
| **Kernel `TCP RST` Injection** | Yes (`SO_LINGER=0`) | Partial | Yes (iptables/eBPF) |
| **In-Flight Bit-Flipping** | Yes (in-place XOR mutation) | No | No |
| **Bandwidth Throttling** | Yes (Token Bucket) | Yes | Yes |
| **Live Control REST API** | Yes (`:8081`) | Yes | Yes |
| **Testing Harness** | Native Go `testing.TB` integration | External client required | CRD / YAML definitions |
| **Binary Footprint** | ~7 MB static scratch binary | Variable | Complex cluster controller |

---

## Architecture

FaultWire operates transparently at the transport layer (TCP), making it completely protocol-agnostic. It works out of the box with HTTP/1.1, HTTP/2, gRPC, PostgreSQL, MySQL, Redis, Kafka, and custom binary wire protocols.

```text
+-------------------+         +---------------------------------------------------+         +-------------------+
|                   |         |                  FaultWire Proxy                  |         |                   |
|   Client / App    | ======> |  [sync.Pool 32KB] ---> [Seeded Inbound Engine]    | ======> |  Upstream Server  |
|  (Python, Go, JS) |   TCP   |                                                   |   TCP   | (API, DB, Redis)  |
|                   | <====== |  [Token-Bucket Limiter] <--- [Outbound Engine]    | <====== |                   |
+-------------------+         +---------------------------------------------------+         +-------------------+
                                                        |
                                       +----------------+----------------+
                                       |                |                |
                                       v                v                v
                                [TCP RST Drop]   [XOR Bit-Flip]   [Jitter / Slice]
                                (SO_LINGER = 0)  (In-Place XOR)   (Context Timer)
```

### Key Engineering Highlights

- **100% Deterministic Chaos Replay:** Powered by an isolated pseudo-random number generator (`math/rand.Source`) guarded by mutex locking. Supplying `--seed 42` guarantees the exact same sequence of packet drops, bit-flips, and delays across test runs.
- **Bi-Directional Pipeline Control:** Inbound (`Client -> Upstream`) and outbound (`Upstream -> Client`) traffic flows pass through dedicated fault engines, allowing you to test client behavior against corrupted responses or dropped ACKs independently.
- **Kernel-Level `TCP RST` Injection:** Dropped connections set socket `SO_LINGER` to `0` (`SetLinger(0)`), instructing the operating system kernel to purge send buffers and fire an immediate `TCP RST` (`Connection reset by peer`) instead of a polite 4-way `FIN` handshake.
- **In-Flight Bitwise Payload Mutation:** Performs bitwise XOR manipulations directly on byte slices to simulate noisy network hardware and faulty NICs while keeping printable ASCII characters readable in logs.
- **Packet Slicing / Fragmentation:** Splits single writes into multiple delayed sub-chunks to expose broken framing assumptions and incomplete `io.Reader` buffering in parsers.
- **Bandwidth Throttling:** Built-in token-bucket rate limiter to emulate bandwidth-constrained networks (2G/3G, satellite WAN).
- **Zero-Allocation Hot Path:** Recycles 32 KB socket buffers using `sync.Pool` pointers, avoiding GC churn under high throughput.
- **Lock-Free Telemetry:** Real-time counters tracked with `sync/atomic` primitives.

---

## Installation

### Pre-Built Binaries
Download pre-compiled static binaries for Linux, macOS, and Windows from [GitHub Releases](https://github.com/rexagamer/faultwire/releases).

### Docker Container
```bash
docker pull ghcr.io/rexagamer/faultwire:latest
```

### Go Install
```bash
go install github.com/rexagamer/faultwire/cmd/faultwire@latest
```

---

## Quickstart

### 1. Run the CLI Proxy
Start a proxy listening on `:8000` forwarding to PostgreSQL or an API on `:9000`, injecting 20% drops, 25% bit-flips, and 200ms latency:

```bash
faultwire \
  -listen :8000 \
  -upstream 127.0.0.1:9000 \
  -control :8081 \
  -seed 42 \
  -drop 0.20 \
  -corrupt 0.25 \
  -delay 0.20 \
  -min-delay 100ms \
  -max-delay 300ms
```

### 2. Run with Built-in Chaos Presets
Use curated profiles without configuring individual fault percentages:

```bash
faultwire -upstream 127.0.0.1:9000 -profile mobile-flaky -seed 42
```

Available presets:
- `mobile-flaky`: High jitter (150ms–600ms), 10% drops, 3% bit-flips, 15% packet slicing.
- `cross-region-wan`: Constant 120ms–220ms latency jitter, 2% drops.
- `nic-fault`: Hardware-level corruption with 30% payload bit-flips and 15% RST aborts.
- `high-jitter`: 50% chance of random delay spikes between 10ms and 800ms.

---

## First-Class Go Integration Testing

FaultWire provides an in-memory test harness (`pkg/faultwiretest`) designed to bind directly to Go's `testing.TB` lifecycle:

```go
package myapp_test

import (
	"testing"
	"time"

	"github.com/rexagamer/faultwire/pkg/faultwiretest"
)

func TestPaymentClient_CircuitBreakerUnderPartitions(t *testing.T) {
	upstreamDB := "127.0.0.1:5432"

	// Start ephemeral proxy tied to the test lifecycle
	proxy := faultwiretest.NewServer(t, upstreamDB,
		faultwiretest.WithSeed(42),
		faultwiretest.WithDropRate(0.40), // 40% TCP RST drop rate
		faultwiretest.WithDelay(50*time.Millisecond, 150*time.Millisecond, 0.30),
	)

	// Route your application client through proxy.Addr()
	client := NewClient(proxy.Addr())
	err := client.ExecuteTransaction()
	if err != nil {
		t.Logf("Client handled network partition cleanly: %v", err)
	}

	// Rewind the PRNG stream to step 0 for an identical replay
	proxy.Reset()
}
```

---

## Dynamic Runtime Control REST API

FaultWire embeds an HTTP management interface (default `:8081`) allowing test runners and CI/CD jobs to reconfigure or inspect chaos parameters without restarting the proxy:

### Inspect Live Metrics
```bash
curl http://localhost:8081/metrics
```
```json
{
  "inbound": {
    "bytes_forwarded": 512044,
    "active_conns": 3,
    "total_conns": 64,
    "drops": 8,
    "corruptions": 14,
    "delays": 19,
    "fragments": 6
  },
  "outbound": {
    "bytes_forwarded": 1048576,
    "active_conns": 3,
    "total_conns": 64,
    "drops": 0,
    "corruptions": 0,
    "delays": 0,
    "fragments": 0
  }
}
```

### Mutate Fault Rates On the Fly
```bash
curl -X POST http://localhost:8081/config \
  -H "Content-Type: application/json" \
  -d '{
    "inbound": {
      "seed": 9999,
      "drop_rate": 0.50,
      "corrupt_rate": 0.10,
      "delay_rate": 0.20,
      "min_delay": 50000000,
      "max_delay": 200000000
    }
  }'
```

### Reset Seed State for Replay
```bash
curl -X POST http://localhost:8081/reset
```

---

## CLI Configuration Reference

| Flag | Default | Description |
|---|---|---|
| `-listen` | `:8000` | Local TCP address to bind the proxy server. |
| `-upstream` | `127.0.0.1:9000` | Target upstream TCP service address. |
| `-control` | `:8081` | Bind address for the HTTP control plane (empty to disable). |
| `-config` | `""` | Path to optional JSON configuration file. |
| `-profile` | `""` | Pre-configured chaos preset (`mobile-flaky`, `cross-region-wan`, `nic-fault`, `high-jitter`). |
| `-seed` | `1337` | Deterministic PRNG seed for reproducible chaos sequencing. |
| `-drop` | `0.0` | Probability (`0.0` to `1.0`) of severing connection via kernel `TCP RST`. |
| `-corrupt` | `0.0` | Probability (`0.0` to `1.0`) of flipping a bit in the TCP payload. |
| `-delay` | `0.0` | Probability (`0.0` to `1.0`) of injecting latency jitter. |
| `-fragment` | `0.0` | Probability (`0.0` to `1.0`) of slicing TCP packets into delayed chunks. |
| `-min-delay` | `50ms` | Minimum duration for latency jitter. |
| `-max-delay` | `400ms` | Maximum duration for latency jitter. |
| `-bandwidth-kb` | `0` | Bandwidth throttle limit in KB/second (`0` = unlimited). |
| `-verbose` | `true` | Log chaos decisions in real time. |

---

## Automated Chaos Arena Simulator

FaultWire includes a self-contained demonstration simulator (`cmd/arena/main.go`) that starts a mock financial exchange, fires a series of JSON trade orders through aggressive chaos injection, and then resets the seed to prove 100% bit-level reproducibility:

```bash
go run ./cmd/arena/main.go
```

---

## License

Distributed under the MIT License. See [LICENSE](LICENSE) for details.