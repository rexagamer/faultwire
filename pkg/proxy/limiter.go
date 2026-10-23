package proxy

import (
	"context"
	"sync"
	"time"
)

// TokenBucket throttles throughput to a maximum bytes-per-second limit.
type TokenBucket struct {
	mu           sync.Mutex
	rateBytesSec int64
	tokens       float64
	lastCheck    time.Time
}

func NewTokenBucket(rateBytesSec int64) *TokenBucket {
	if rateBytesSec <= 0 {
		return nil
	}
	return &TokenBucket{
		rateBytesSec: rateBytesSec,
		tokens:       float64(rateBytesSec),
		lastCheck:    time.Now(),
	}
}

// Wait blocks until enough byte budget tokens are available or context expires.
func (tb *TokenBucket) Wait(ctx context.Context, bytesCount int) bool {
	if tb == nil || bytesCount <= 0 {
		return true
	}

	for {
		tb.mu.Lock()
		now := time.Now()
		elapsed := now.Sub(tb.lastCheck).Seconds()
		tb.lastCheck = now

		tb.tokens += elapsed * float64(tb.rateBytesSec)
		if tb.tokens > float64(tb.rateBytesSec) {
			tb.tokens = float64(tb.rateBytesSec)
		}

		if tb.tokens >= float64(bytesCount) {
			tb.tokens -= float64(bytesCount)
			tb.mu.Unlock()
			return true
		}

		missing := float64(bytesCount) - tb.tokens
		waitTime := time.Duration((missing / float64(tb.rateBytesSec)) * float64(time.Second))
		tb.mu.Unlock()

		if waitTime < time.Millisecond {
			waitTime = time.Millisecond
		}

		timer := time.NewTimer(waitTime)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return false
		}
	}
}
