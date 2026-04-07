package gosplice

import (
	"context"
	"sync"
	"time"
)

// ---------------------------------------------------------------------------
// RateLimitConfig
// ---------------------------------------------------------------------------

// RateLimitConfig controls the rate limiter behavior.
type RateLimitConfig struct {
	// Rate is the number of elements allowed per Interval.
	Rate int

	// Interval is the time window for the rate.
	// Defaults to 1 second if zero.
	Interval time.Duration

	// Burst is the maximum number of elements that can pass through
	// without waiting (the token bucket capacity). Defaults to Rate if zero.
	// Set Burst > Rate to allow short spikes above the sustained rate.
	Burst int
}

func (c RateLimitConfig) interval() time.Duration {
	if c.Interval <= 0 {
		return time.Second
	}
	return c.Interval
}

func (c RateLimitConfig) burst() int {
	if c.Burst > 0 {
		return c.Burst
	}
	return c.Rate
}

// ---------------------------------------------------------------------------
// Pipeline method
// ---------------------------------------------------------------------------

// RateLimit adds a token bucket rate limiter as a lazy pipeline stage.
// Elements pass through at most cfg.Rate elements per cfg.Interval.
// The limiter is context-aware — if the pipeline has a context set via
// WithContext or WithTimeout, the wait is cancelled when the context expires.
//
// RateLimit is lazy: it returns a new pipeline without consuming any elements.
// The rate limiting happens during iteration (Collect, ForEach, etc.).
//
// This is the primary use case for API call pipelines where you need to
// respect external rate limits (e.g. 100 requests/second).
//
//	// 50 elements per second, burst of 10
//	results := gs.PipeMap(
//	    gs.FromSlice(urls).
//	        RateLimit(gs.RateLimitConfig{Rate: 50, Burst: 10}),
//	    fetchURL,
//	).Collect()
//
//	// With context timeout — stops waiting if deadline expires
//	results := gs.PipeMap(
//	    gs.FromSlice(urls).
//	        WithTimeout(30 * time.Second).
//	        RateLimit(gs.RateLimitConfig{Rate: 100}),
//	    fetchURL,
//	).Collect()
func (p *Pipeline[T]) RateLimit(cfg RateLimitConfig) *Pipeline[T] {
	return &Pipeline[T]{
		source: &rateLimitSource[T]{
			inner:  p.source,
			bucket: newTokenBucket(cfg),
		},
		hooks:   p.hooks,
		ctx:     p.ctx,
		cancel:  p.cancel,
		ctxNoop: p.ctxNoop,
	}
}

// ---------------------------------------------------------------------------
// rateLimitSource — Source[T] wrapper
// ---------------------------------------------------------------------------

type rateLimitSource[T any] struct {
	inner  Source[T]
	bucket *tokenBucket
}

func (s *rateLimitSource[T]) Next() (T, bool) {
	s.bucket.wait()
	return s.inner.Next()
}

func (s *rateLimitSource[T]) SizeHint() int {
	if sizer, ok := s.inner.(Sizer); ok {
		return sizer.SizeHint()
	}
	return -1
}

// ---------------------------------------------------------------------------
// Context-aware variant
// ---------------------------------------------------------------------------

// RateLimitCtx is like RateLimit but uses the provided context for cancellation.
// This is useful when the rate-limited pipeline is consumed by a parallel operation
// that doesn't propagate the pipeline's context to the source.
//
// In most cases, use RateLimit instead — it automatically uses the pipeline's context.
func RateLimitCtx[T any](p *Pipeline[T], cfg RateLimitConfig, ctx context.Context) *Pipeline[T] {
	return &Pipeline[T]{
		source: &rateLimitCtxSource[T]{
			inner:  p.source,
			bucket: newTokenBucket(cfg),
			ctx:    ctx,
		},
		hooks:   p.hooks,
		ctx:     p.ctx,
		cancel:  p.cancel,
		ctxNoop: p.ctxNoop,
	}
}

type rateLimitCtxSource[T any] struct {
	inner  Source[T]
	bucket *tokenBucket
	ctx    context.Context
}

func (s *rateLimitCtxSource[T]) Next() (T, bool) {
	if !s.bucket.waitCtx(s.ctx) {
		var zero T
		return zero, false
	}
	return s.inner.Next()
}

func (s *rateLimitCtxSource[T]) SizeHint() int {
	if sizer, ok := s.inner.(Sizer); ok {
		return sizer.SizeHint()
	}
	return -1
}

// ---------------------------------------------------------------------------
// Token bucket implementation
// ---------------------------------------------------------------------------

// tokenBucket is a classic token bucket rate limiter.
// Tokens are replenished continuously at a fixed rate.
// No external dependencies — just time.Now() and time.Sleep().
type tokenBucket struct {
	mu       sync.Mutex
	tokens   float64
	max      float64
	rate     float64 // tokens per nanosecond
	lastTime time.Time
}

func newTokenBucket(cfg RateLimitConfig) *tokenBucket {
	burst := float64(cfg.burst())
	interval := cfg.interval()
	rate := float64(cfg.Rate) / float64(interval)
	return &tokenBucket{
		tokens:   burst, // start full
		max:      burst,
		rate:     rate,
		lastTime: time.Now(),
	}
}

// refill adds tokens based on elapsed time since last refill.
// Must be called with mu held.
func (tb *tokenBucket) refill() {
	now := time.Now()
	elapsed := now.Sub(tb.lastTime)
	tb.lastTime = now
	tb.tokens += float64(elapsed) * tb.rate
	if tb.tokens > tb.max {
		tb.tokens = tb.max
	}
}

// wait blocks until a token is available. Not context-aware.
func (tb *tokenBucket) wait() {
	for {
		tb.mu.Lock()
		tb.refill()
		if tb.tokens >= 1 {
			tb.tokens--
			tb.mu.Unlock()
			return
		}
		// Calculate wait time for next token
		deficit := 1 - tb.tokens
		waitNs := deficit / tb.rate
		tb.mu.Unlock()
		time.Sleep(time.Duration(waitNs))
	}
}

// waitCtx blocks until a token is available or ctx is cancelled.
// Returns false if ctx was cancelled before a token became available.
func (tb *tokenBucket) waitCtx(ctx context.Context) bool {
	if ctx == nil || ctx.Done() == nil {
		tb.wait()
		return true
	}
	for {
		tb.mu.Lock()
		tb.refill()
		if tb.tokens >= 1 {
			tb.tokens--
			tb.mu.Unlock()
			return true
		}
		deficit := 1 - tb.tokens
		waitNs := deficit / tb.rate
		tb.mu.Unlock()

		timer := time.NewTimer(time.Duration(waitNs))
		select {
		case <-ctx.Done():
			timer.Stop()
			return false
		case <-timer.C:
			// Token should be available now — loop back to claim it
		}
	}
}
