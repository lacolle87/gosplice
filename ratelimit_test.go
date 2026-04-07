package gosplice

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Token bucket internals
// ---------------------------------------------------------------------------

func TestTokenBucket_InitialBurst(t *testing.T) {
	tb := newTokenBucket(RateLimitConfig{Rate: 10, Burst: 5})
	for i := 0; i < 5; i++ {
		tb.mu.Lock()
		tb.refill()
		if tb.tokens < 1 {
			tb.mu.Unlock()
			t.Fatalf("expected token available at iteration %d", i)
		}
		tb.tokens--
		tb.mu.Unlock()
	}
	tb.mu.Lock()
	tb.refill()
	if tb.tokens >= 1 {
		tb.mu.Unlock()
		t.Fatal("expected no token after burst exhaustion")
	}
	tb.mu.Unlock()
}

func TestTokenBucket_Refill(t *testing.T) {
	// Rate=100/s, burst=100 so the cap doesn't interfere with refill measurement
	tb := newTokenBucket(RateLimitConfig{Rate: 100, Interval: time.Second, Burst: 100})
	tb.mu.Lock()
	tb.tokens = 0
	tb.lastTime = time.Now()
	tb.mu.Unlock()

	time.Sleep(50 * time.Millisecond)
	tb.mu.Lock()
	tb.refill()
	tokens := tb.tokens
	tb.mu.Unlock()

	// 100/s * 0.05s = ~5 tokens, allow jitter
	if tokens < 3 || tokens > 8 {
		t.Errorf("expected ~5 tokens after 50ms at 100/s, got %.2f", tokens)
	}
}

func TestTokenBucket_MaxCap(t *testing.T) {
	tb := newTokenBucket(RateLimitConfig{Rate: 1000, Burst: 10})
	time.Sleep(100 * time.Millisecond)
	tb.mu.Lock()
	tb.refill()
	tokens := tb.tokens
	tb.mu.Unlock()
	if tokens > 10 {
		t.Errorf("expected max 10 tokens, got %.2f", tokens)
	}
}

// ---------------------------------------------------------------------------
// RateLimit pipeline method
// ---------------------------------------------------------------------------

func TestRateLimit_Basic(t *testing.T) {
	result := FromSlice([]int{1, 2, 3, 4, 5}).
		RateLimit(RateLimitConfig{Rate: 1000, Burst: 100}).
		Collect()
	assertSliceEqual(t, []int{1, 2, 3, 4, 5}, result)
}

func TestRateLimit_PreservesOrder(t *testing.T) {
	result := FromRange(0, 20).
		RateLimit(RateLimitConfig{Rate: 10000, Burst: 100}).
		Collect()
	expected := make([]int, 20)
	for i := range expected {
		expected[i] = i
	}
	assertSliceEqual(t, expected, result)
}

func TestRateLimit_Empty(t *testing.T) {
	result := FromSlice([]int{}).
		RateLimit(RateLimitConfig{Rate: 10}).
		Collect()
	if len(result) != 0 {
		t.Fatalf("expected empty, got %d", len(result))
	}
}

func TestRateLimit_ActuallyLimits(t *testing.T) {
	// 20 elem/sec, burst 5 → first 5 instant, remaining 5 take ~250ms
	start := time.Now()
	result := FromRange(0, 10).
		RateLimit(RateLimitConfig{Rate: 20, Burst: 5}).
		Collect()
	elapsed := time.Since(start)

	if len(result) != 10 {
		t.Fatalf("expected 10 elements, got %d", len(result))
	}
	if elapsed < 100*time.Millisecond {
		t.Errorf("completed too fast (%v) — rate limiting may not be working", elapsed)
	}
	if elapsed > 2*time.Second {
		t.Errorf("completed too slow (%v)", elapsed)
	}
}

func TestRateLimit_WithFilter(t *testing.T) {
	result := FromRange(0, 100).
		RateLimit(RateLimitConfig{Rate: 10000, Burst: 200}).
		Filter(func(n int) bool { return n%2 == 0 }).
		Take(5).
		Collect()
	assertSliceEqual(t, []int{0, 2, 4, 6, 8}, result)
}

func TestRateLimit_WithPipeMap(t *testing.T) {
	result := PipeMap(
		FromSlice([]int{1, 2, 3}).
			RateLimit(RateLimitConfig{Rate: 10000, Burst: 100}),
		func(n int) int { return n * 10 },
	).Collect()
	assertSliceEqual(t, []int{10, 20, 30}, result)
}

func TestRateLimit_WithHooks(t *testing.T) {
	var count atomic.Int64
	result := FromSlice([]int{1, 2, 3, 4, 5}).
		WithElementHook(CountElements[int](&count)).
		RateLimit(RateLimitConfig{Rate: 10000, Burst: 100}).
		Collect()
	assertSliceEqual(t, []int{1, 2, 3, 4, 5}, result)
	if count.Load() != 5 {
		t.Errorf("expected 5 hook calls, got %d", count.Load())
	}
}

func TestRateLimit_SizeHint(t *testing.T) {
	src := &rateLimitSource[int]{
		inner:  &sliceSource[int]{data: []int{1, 2, 3}},
		bucket: newTokenBucket(RateLimitConfig{Rate: 100}),
	}
	if src.SizeHint() != 3 {
		t.Errorf("expected SizeHint=3, got %d", src.SizeHint())
	}
}

func TestRateLimit_DefaultInterval(t *testing.T) {
	cfg := RateLimitConfig{Rate: 100}
	if cfg.interval() != time.Second {
		t.Errorf("expected default interval 1s, got %v", cfg.interval())
	}
}

func TestRateLimit_DefaultBurst(t *testing.T) {
	cfg := RateLimitConfig{Rate: 50}
	if cfg.burst() != 50 {
		t.Errorf("expected default burst=Rate=50, got %d", cfg.burst())
	}
}

func TestRateLimit_CustomBurst(t *testing.T) {
	cfg := RateLimitConfig{Rate: 50, Burst: 10}
	if cfg.burst() != 10 {
		t.Errorf("expected burst=10, got %d", cfg.burst())
	}
}

// ---------------------------------------------------------------------------
// RateLimitCtx — context-aware
// ---------------------------------------------------------------------------

func TestRateLimitCtx_CancelStopsPipeline(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	// Slow rate, small burst — cancel should stop quickly
	pipeline := RateLimitCtx(
		FromRange(0, 1000),
		RateLimitConfig{Rate: 1, Burst: 2},
		ctx,
	)

	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	result := pipeline.Collect()

	if len(result) >= 100 {
		t.Errorf("expected early stop, got %d elements", len(result))
	}
}

func TestRateLimitCtx_PreCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Rate=1, Burst=1 → first token is available immediately from bucket.
	// But waitCtx checks ctx.Done() via select, and with pre-cancelled ctx
	// the select may pick either branch. So we use a slow rate and expect
	// at most the burst amount (1) to pass through before ctx wins.
	result := RateLimitCtx(
		FromRange(0, 100),
		RateLimitConfig{Rate: 1, Burst: 1},
		ctx,
	).Collect()

	// With pre-cancelled ctx: burst token may or may not be consumed
	// (select on cancelled ctx is non-deterministic). Either 0 or 1 is acceptable.
	if len(result) > 1 {
		t.Errorf("expected at most 1 with pre-cancelled ctx, got %d", len(result))
	}
}

func TestRateLimitCtx_NilCtx(t *testing.T) {
	result := RateLimitCtx(
		FromSlice([]int{1, 2, 3}),
		RateLimitConfig{Rate: 10000, Burst: 100},
		nil,
	).Collect()
	assertSliceEqual(t, []int{1, 2, 3}, result)
}

func TestRateLimitCtx_BackgroundCtx(t *testing.T) {
	result := RateLimitCtx(
		FromSlice([]int{1, 2, 3}),
		RateLimitConfig{Rate: 10000, Burst: 100},
		context.Background(),
	).Collect()
	assertSliceEqual(t, []int{1, 2, 3}, result)
}

func TestRateLimitCtx_SizeHint(t *testing.T) {
	src := &rateLimitCtxSource[int]{
		inner:  &sliceSource[int]{data: []int{1, 2, 3, 4}},
		bucket: newTokenBucket(RateLimitConfig{Rate: 100}),
		ctx:    context.Background(),
	}
	if src.SizeHint() != 4 {
		t.Errorf("expected SizeHint=4, got %d", src.SizeHint())
	}
}

// ---------------------------------------------------------------------------
// Integration: RateLimit + WithTimeout
// ---------------------------------------------------------------------------

func TestRateLimit_WithTimeout(t *testing.T) {
	p := FromRange(0, 1000).
		WithTimeout(80 * time.Millisecond).
		RateLimit(RateLimitConfig{Rate: 20, Burst: 5})

	result := p.Collect()

	if len(result) < 3 {
		t.Errorf("expected at least 3 results, got %d", len(result))
	}
	if len(result) >= 1000 {
		t.Errorf("expected partial results, got all %d", len(result))
	}
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

func BenchmarkRateLimit_HighRate_1k(b *testing.B) {
	data := make([]int, 1000)
	for i := range data {
		data[i] = i
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		FromSlice(data).
			RateLimit(RateLimitConfig{Rate: 1_000_000, Burst: 100_000}).
			Collect()
	}
}

func BenchmarkRateLimit_Overhead_vs_Plain(b *testing.B) {
	data := make([]int, 1000)
	for i := range data {
		data[i] = i
	}

	b.Run("plain", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			FromSlice(data).Collect()
		}
	})
	b.Run("rate_limited", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			FromSlice(data).
				RateLimit(RateLimitConfig{Rate: 10_000_000, Burst: 1_000_000}).
				Collect()
		}
	})
}
