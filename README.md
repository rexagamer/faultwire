# FaultWire

[![Go Version](https://img.shields.io/badge/Go-1.22+-00ADD8?style=flat&logo=go)](https://go.dev/)
[![Dependencies](https://img.shields.io/badge/dependencies-zero-brightgreen.svg)](#architecture--data-path)
[![CI Status](https://github.com/rexagamer/faultwire/actions/workflows/ci.yml/badge.svg)](https://github.com/rexagamer/faultwire/actions)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

**FaultWire** is a deterministic, zero-dependency Layer 4 TCP fault-injection proxy written in Go. 

Distributed systems rarely fail in clean, reproducible ways. When an application crashes in production due to a half-open socket, bit-flipped packet, or sudden latency spike, reproducing that exact failure locally is nearly impossible. FaultWire sits between your client and backend services, intercepting raw TCP byte streams and injecting **seeded, deterministic network chaos**—allowing engineers to replay exact failure sequences on demand.

---

## Architecture & Data Path

FaultWire operates transparently at the transport layer (TCP), making it completely language- and protocol-agnostic. It works out of the box with HTTP, gRPC, PostgreSQL, Redis, or custom binary wire protocols without modifying a single line of application code.

```text
+-------------------+         +---------------------------------------------------+         +-------------------+
|                   |         |                  FaultWire Proxy                  |         |                   |
|   Client / App    | ======> |  [32KB sync.Pool] ---> [Seeded PRNG Fault Engine] | ======> |  Upstream Server  |
|  (Python, Go, JS) |  TCP    |                                 |                 |  TCP    | (API, DB, Redis)  |
|                   | <====== |  [Lock-Free Atomics] <----------+                 | <====== |                   |
+-------------------+         +---------------------------------------------------+         +-------------------+
                                                                |
                                       +------------------------+------------------------+
                                       |                        |                        |
                                       v                        v                        v
                                [TCP RST Drop]          [XOR Bit-Flip]          [Jitter / Slice]
                                (SO_LINGER = 0)        (In-Place Mutate)        (Context Timer)
```

### Engineering Highlights

* **100% Deterministic Chaos Replay:** Powered by an isolated, mutex-guarded pseudo-random number generator (`math/rand.Source`). Feeding FaultWire `--seed 42` guarantees the exact same sequence of packet drops, bit-flips, and millisecond delays across test runs.
* **Kernel-Level `TCP RST` Injection:** Instead of closing connections politely with a standard 4-way `FIN` handshake, dropped connections configure socket `SO_LINGER` to `0` (`SetLinger(0)`), forcing the OS kernel to purge send buffers and fire an immediate `TCP RST` (`Connection reset by peer`) packet.
* **In-Flight Bitwise Payload Corruption:** Simulates hardware line noise and faulty NICs by performing in-place bitwise `XOR` mutations on raw TCP byte slices before forwarding.
* **TCP Packet Fragmentation:** Splits single TCP writes into multiple delayed sub-slices to expose broken framing assumptions and incomplete `io.Reader` loops in upstream parsers.
* **Zero-Allocation Hot Path:** Reuses fixed 32 KB socket buffers via `sync.Pool` pointers (`*[]byte`), avoiding garbage collection pressure under high throughput.
* **Lock-Free Telemetry:** Tracks active connections, byte throughput, and fault distribution counters in real time using `sync/atomic` primitives.

---

## Project Structure

```text
faultwire/
├── cmd/
│   ├── faultwire/       # Production CLI proxy server with live atomic telemetry
│   └── arena/           # Self-contained High-Frequency Trading chaos & replay simulator
├── pkg/
│   ├── engine/          # Deterministic PRNG fault engine, atomic metrics & unit tests
│   └── proxy/           # Full-duplex TCP interceptor, SO_LINGER RST handler & buffer pool
└── examples/
    └── python/          # Real-world Python Payment API & Client resilience testbed
```

---

## Quickstart

### 1. Clone and Build

```bash
git clone [https://github.com/](https://github.com/)rexagamer/faultwire.git
cd faultwire
go build -o faultwire ./cmd/faultwire
```

### 2. Run the Chaos Arena Simulator

FaultWire includes a built-in **Chaos Arena** (`cmd/arena/main.go`) that spins up a mock financial exchange, places FaultWire in the middle with `Seed 42`, fires 12 sequential trade orders, and then resets the seed to verify **100% deterministic fault replay**:

```bash
go run ./cmd/arena/main.go
```

**Sample Output:**
```text
==========================================================================================
   FAULTWIRE CHAOS ARENA: HIGH-FREQUENCY TRADING RESILIENCE & DETERMINISM SIMULATOR
==========================================================================================
Backend Exchange : 127.0.0.1:61043
FaultWire Proxy  : 127.0.0.1:61044 (Seed: 42 | Drop: 20% | Corrupt: 25% | Delay: 25%)

>>> EXECUTING RUN #1 (SEED 42)
------------------------------------------------------------------------------------------
Order #01 |    1ms | CORRUPTED (BITFLIP)  | ERR_CORRUPT: Silent Data Mutation! Symbol="BTC-USD" Action="BEY"
Order #02 |    1ms | CORRUPTED (BITFLIP)  | ERR_CORRUPT: JSON Parse Failed -> Raw Wire Bytes: {"order_id":2,"symbol":"BTC-USD","action#:"BUY","amount":2000}
Order #03 |    1ms | HEALTHY (PASS)       | ACK: Executed BUY BTC-USD ($3000)
Order #04 |    1ms | CORRUPTED (BITFLIP)  | ERR_CORRUPT: JSON Parse Failed -> Raw Wire Bytes: {"order_id"24,"symbol":"BTC-USD","action":"BUY","amount":4000}
Order #05 |   11ms | HEALTHY (PASS)       | ACK: Executed BUY BTC-USD ($5000)
Order #06 |    1ms | HEALTHY (PASS)       | ACK: Executed BUY BTC-USD ($6000)
Order #07 |  244ms | DELAYED (JITTER)     | ACK: Executed BUY BTC-USD ($7000)
Order #08 |   11ms | HEALTHY (PASS)       | ACK: Executed BUY BTC-USD ($8000)
Order #09 |    1ms | HEALTHY (PASS)       | ACK: Executed BUY BTC-USD ($9000)
Order #10 |    1ms | DROPPED (TCP RST)    | Socket severed mid-flight (TCP RST)
Order #11 |    1ms | CORRUPTED (BITFLIP)  | ERR_CORRUPT: Silent Data Mutation! Symbol="BTC-TSD" Action="BUY"
Order #12 |  237ms | DELAYED (JITTER)     | ACK: Executed BUY BTC-USD ($12000)

>>> REPLAYING RUN #2 WITH IDENTICAL SEED (SEED 42)
------------------------------------------------------------------------------------------
Order #01 |    1ms | CORRUPTED (BITFLIP)  | ERR_CORRUPT: Silent Data Mutation! Symbol="BTC-USD" Action="BEY"
...
Order #12 |  237ms | DELAYED (JITTER)     | ACK: Executed BUY BTC-USD ($12000)

==========================================================================================
 [SUCCESS] DETERMINISM VERIFIED: Run #1 and Run #2 produced the EXACT same fault sequence!
==========================================================================================
```

---

## Real-World Case Study: Breaking a Python Payment Service

Because FaultWire operates on raw TCP sockets, you can place it in front of any backend service to uncover hidden distributed systems bugs.

### Step 1: Start the Target Backend (`:9000`)
```bash
python examples/python/backend.py
```

### Step 2: Start FaultWire Proxy (`:8000 -> :9000`)
Configure a 20% `TCP RST` drop rate, 20% bit-flip corruption rate, and 30% latency jitter (100ms–500ms) using deterministic seed `77`:
```bash
go run ./cmd/faultwire/main.go \
  -listen :8000 \
  -upstream 127.0.0.1:9000 \
  -seed 77 \
  -drop 0.2 \
  -corrupt 0.2 \
  -delay 0.3 \
  -min-delay 100ms \
  -max-delay 500ms
```

### Step 3: Run the Client Against FaultWire (`:8000`)
```bash
python examples/python/client.py
```

### Bugs Exposed by FaultWire in One Run:

1. **The "Ghost Charge" Double-Billing Bug (Payments #03, #04, #10):**
   * **Client Output:** `Payment #03 | 308ms | FAILED | Network Failure: TimeoutError: timed out`
   * **FaultWire Log:** `[CHAOS] CLIENT->UPSTREAM | Injecting latency jitter: 466.612806ms`
   * **Backend Log:** `[PYTHON BACKEND] Processed payment: User=alice, Amount=$300`
   * **Insight:** The client timed out at `300ms` and reported a failure to the user, but the delayed packet still reached the backend at `466ms` and charged the customer—proving the backend requires idempotency keys.

2. **Wire-Level Byte Corruption (Payment #09):**
   * **FaultWire Log:** `[CHAOS] CLIENT->UPSTREAM | Bit-flip at byte[189]: 0x22 -> 0x02`
   * **Backend Log:** `[PYTHON BACKEND] CRASH! Invalid payload received: b'{"user": "alice\x02, "amount": 900}'`
   * **Insight:** FaultWire flipped a single bit inside the closing quote (`0x22`), exposing how the backend handles malformed wire payloads.

3. **Abrupt Mid-Stream Disconnects (Payments #02, #08):**
   * **FaultWire Log:** `[CHAOS] CLIENT->UPSTREAM | Severing connection with TCP RST (32 bytes dropped)`
   * **Client/Backend Output:** `ConnectionResetError: [WinError 10054] An existing connection was forcibly closed by the remote host`
   * **Insight:** FaultWire allowed the HTTP headers through, then injected a `TCP RST` right as the 32-byte JSON body arrived, testing whether the server leaks file descriptors on half-read sockets.

---

## CLI Configuration Flags

| Flag | Default | Description |
|---|---|---|
| `-listen` | `:8000` | Local TCP address and port for FaultWire to bind and listen on. |
| `-upstream` | `127.0.0.1:9000` | Target upstream TCP address to forward traffic to. |
| `-seed` | `1337` | Deterministic PRNG seed for reproducible fault sequences. |
| `-drop` | `0.0` | Probability (`0.0` to `1.0`) of severing a connection via kernel `TCP RST`. |
| `-corrupt` | `0.0` | Probability (`0.0` to `1.0`) of flipping a random bit in the TCP payload. |
| `-delay` | `0.0` | Probability (`0.0` to `1.0`) of injecting latency jitter before forwarding. |
| `-fragment` | `0.0` | Probability (`0.0` to `1.0`) of slicing a TCP chunk into two delayed writes. |
| `-min-delay` | `50ms` | Minimum duration for injected latency jitter. |
| `-max-delay` | `400ms` | Maximum duration for injected latency jitter. |
| `-verbose` | `true` | Log every intercepted chaos decision in real time. |

---

## Running Automated Tests

FaultWire includes unit tests verifying PRNG determinism and concurrency safety under Go's race detector:

```bash
go test -v -race ./...
```

---

## License

Distributed under the MIT License. See `LICENSE` for more information.