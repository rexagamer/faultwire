package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"time"

	"faultwire/pkg/engine"
	"faultwire/pkg/proxy"
)

type TradeOrder struct {
	OrderID int    `json:"order_id"`
	Symbol  string `json:"symbol"`
	Action  string `json:"action"`
	Amount  int    `json:"amount"`
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Start Mock Financial Exchange Backend
	backendAddr, err := startMockExchange(ctx)
	if err != nil {
		panic(err)
	}

	// 2. Configure FaultWire with aggressive chaos & Seed 42
	eng := engine.New(engine.Config{
		Seed:         42,
		DropRate:     0.20, // 20% hard TCP RST drops
		CorruptRate:  0.25, // 25% bit-flip payload corruption
		DelayRate:    0.25, // 25% latency spikes (150ms - 300ms)
		FragmentRate: 0.10, // 10% TCP packet slicing
		MinDelay:     150 * time.Millisecond,
		MaxDelay:     300 * time.Millisecond,
	})

	srv, err := proxy.New(proxy.Config{
		ListenAddr:   "127.0.0.1:0", // OS picks a free port automatically
		UpstreamAddr: backendAddr,
		Verbose:      false,
	}, eng)
	if err != nil {
		panic(err)
	}

	go func() {
		_ = srv.Start(ctx)
	}()
	time.Sleep(50 * time.Millisecond) // Wait for listener bind
	proxyAddr := srv.Addr()

	fmt.Println("==========================================================================================")
	fmt.Println("   FAULTWIRE CHAOS ARENA: HIGH-FREQUENCY TRADING RESILIENCE & DETERMINISM SIMULATOR")
	fmt.Println("==========================================================================================")
	fmt.Printf("Backend Exchange : %s\n", backendAddr)
	fmt.Printf("FaultWire Proxy  : %s (Seed: 42 | Drop: 20%% | Corrupt: 25%% | Delay: 25%%)\n\n", proxyAddr)

	// 3. Execute Run #1
	fmt.Println(">>> EXECUTING RUN #1 (SEED 42)")
	fmt.Println("------------------------------------------------------------------------------------------")
	resultsRun1 := runSimulation(proxyAddr, 12)

	// 4. Reset Seed to 42 and Execute Run #2
	eng.Reset()
	fmt.Println("\n>>> REPLAYING RUN #2 WITH IDENTICAL SEED (SEED 42)")
	fmt.Println("------------------------------------------------------------------------------------------")
	resultsRun2 := runSimulation(proxyAddr, 12)

	// 5. Verify Determinism
	fmt.Println("\n==========================================================================================")
	identical := true
	for i := range resultsRun1 {
		if resultsRun1[i] != resultsRun2[i] {
			identical = false
			break
		}
	}

	if identical {
		fmt.Println(" [SUCCESS] DETERMINISM VERIFIED: Run #1 and Run #2 produced the EXACT same fault sequence!")
	} else {
		fmt.Println(" [FAILED] Determinism mismatch detected.")
	}
	fmt.Println("==========================================================================================")
}

func runSimulation(proxyAddr string, count int) []string {
	outcomes := make([]string, count)

	for i := 1; i <= count; i++ {
		order := TradeOrder{
			OrderID: i,
			Symbol:  "BTC-USD",
			Action:  "BUY",
			Amount:  1000 * i,
		}
		payload, _ := json.Marshal(order)
		payload = append(payload, '\n')

		start := time.Now()
		conn, err := net.DialTimeout("tcp", proxyAddr, time.Second)
		if err != nil {
			outcomes[i-1] = "DIAL_ERR"
			continue
		}

		_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
		_, _ = conn.Write(payload)

		reply, err := bufio.NewReader(conn).ReadString('\n')
		elapsed := time.Since(start).Round(time.Millisecond)
		_ = conn.Close()

		var status, detail string
		if err != nil {
			status = "DROPPED (TCP RST)"
			detail = "Socket severed mid-flight (TCP RST)"
		} else {
			reply = strings.TrimSpace(reply)
			if strings.HasPrefix(reply, "ERR_CORRUPT:") {
				status = "CORRUPTED (BITFLIP)"
				detail = reply
			} else if elapsed >= 140*time.Millisecond {
				status = "DELAYED (JITTER)"
				detail = reply
			} else {
				status = "HEALTHY (PASS)"
				detail = reply
			}
		}

		outcomes[i-1] = status + "|" + detail
		fmt.Printf("Order #%02d | %6v | %-20s | %s\n", i, elapsed, status, detail)
	}
	return outcomes
}

// startMockExchange starts a TCP server that parses JSON trade orders and reports bit corruption.
func startMockExchange(ctx context.Context) (string, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}

	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				rawLine, err := bufio.NewReader(c).ReadBytes('\n')
				if err != nil {
					return
				}
				var order TradeOrder
				if err := json.Unmarshal(rawLine, &order); err != nil {
					// JSON syntax was broken by a bit-flip!
					msg := fmt.Sprintf("ERR_CORRUPT: JSON Parse Failed -> Raw Wire Bytes: %s\n", strings.TrimSpace(string(rawLine)))
					_, _ = c.Write([]byte(msg))
					return
				}
				// Check if fields inside valid JSON were mutated by a bit-flip
				if order.Symbol != "BTC-USD" || order.Action != "BUY" {
					msg := fmt.Sprintf("ERR_CORRUPT: Silent Data Mutation! Symbol=%q Action=%q\n", order.Symbol, order.Action)
					_, _ = c.Write([]byte(msg))
					return
				}
				resp := fmt.Sprintf("ACK: Executed %s %s ($%d)\n", order.Action, order.Symbol, order.Amount)
				_, _ = c.Write([]byte(resp))
			}(conn)
		}
	}()

	return ln.Addr().String(), nil
}
