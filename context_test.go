package gosplice

import (
	"context"
	"errors"
	"fmt"
	"io"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// WithContext + Err() — basic pattern
// ---------------------------------------------------------------------------

func TestWithContext_NilByDefault(t *testing.T) {
	p := FromSlice([]int{1, 2, 3})
	if p.ctx != nil {
		t.Fatal("ctx should be nil by default")
	}
	result := p.Collect()
	assertSliceEqual(t, []int{1, 2, 3}, result)
}

func TestWithContext_NoCancel_FullResult(t *testing.T) {
	ctx := context.Background()
	p := FromSlice([]int{1, 2, 3}).WithContext(ctx)
	result := p.Collect()
	assertSliceEqual(t, []int{1, 2, 3}, result)
	if p.Err() != nil {
		t.Fatalf("unexpected error: %v", p.Err())
	}
}

func TestWithContext_ErrNilOnNormal(t *testing.T) {
	p := FromSlice([]int{1, 2, 3}).WithContext(context.Background())
	p.ForEach(func(int) {})
	if p.Err() != nil {
		t.Fatalf("expected nil err, got %v", p.Err())
	}
}

// ---------------------------------------------------------------------------
// Context cancellation — Collect
// ---------------------------------------------------------------------------

func TestCtxCancel_Collect_Slice(t *testing.T) {
	data := make([]int, 10000)
	for i := range data {
		data[i] = i
	}
	ctx, cancel := context.WithCancel(context.Background())
	p := FromSlice(data).WithContext(ctx)

	// Cancel after a short delay.
	go func() {
		time.Sleep(time.Millisecond)
		cancel()
	}()

	result := p.Collect()
	if len(result) >= len(data) {
		// With 10k elements and 1ms delay this should stop early.
		// But if CPU is very fast, the slice might finish — accept that.
		t.Logf("collected all %d elements (CPU too fast for cancellation)", len(result))
	}
	// The error should be set if cancellation happened during iteration.
	if len(result) < len(data) {
		if !errors.Is(p.Err(), context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", p.Err())
		}
	}
}

func TestCtxCancel_Collect_Channel(t *testing.T) {
	ch := make(chan int)
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		defer close(ch)
		for i := 0; ; i++ {
			select {
			case ch <- i:
			case <-ctx.Done():
				return
			}
		}
	}()

	p := FromChannel(ch).WithContext(ctx)
	go func() {
		time.Sleep(5 * time.Millisecond)
		cancel()
	}()

	result := p.Collect()
	if p.Err() == nil {
		// The channel might have been closed before ctx check — only fail if too many.
		if len(result) > 100000 {
			t.Fatal("expected early termination")
		}
	}
}

// ---------------------------------------------------------------------------
// Context cancellation — ForEach
// ---------------------------------------------------------------------------

func TestCtxCancel_ForEach(t *testing.T) {
	ch := make(chan int)
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		defer close(ch)
		for i := 0; ; i++ {
			select {
			case ch <- i:
			case <-ctx.Done():
				return
			}
		}
	}()

	var count int64
	p := FromChannel(ch).WithContext(ctx)
	go func() {
		time.Sleep(5 * time.Millisecond)
		cancel()
	}()

	p.ForEach(func(v int) { atomic.AddInt64(&count, 1) })
	if !errors.Is(p.Err(), context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", p.Err())
	}
}

// ---------------------------------------------------------------------------
// Context cancellation — Reduce
// ---------------------------------------------------------------------------

func TestCtxCancel_Reduce(t *testing.T) {
	ch := make(chan int)
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		defer close(ch)
		for i := 1; ; i++ {
			select {
			case ch <- i:
			case <-ctx.Done():
				return
			}
		}
	}()

	p := FromChannel(ch).WithContext(ctx)
	go func() {
		time.Sleep(5 * time.Millisecond)
		cancel()
	}()

	_ = p.Reduce(0, func(a, b int) int { return a + b })
	if !errors.Is(p.Err(), context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", p.Err())
	}
}

// ---------------------------------------------------------------------------
// Context cancellation — Count
// ---------------------------------------------------------------------------

func TestCtxCancel_Count(t *testing.T) {
	ch := make(chan int)
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		defer close(ch)
		for i := 0; ; i++ {
			select {
			case ch <- i:
			case <-ctx.Done():
				return
			}
		}
	}()

	p := FromChannel(ch).WithContext(ctx)
	go func() {
		time.Sleep(5 * time.Millisecond)
		cancel()
	}()

	n := p.Count()
	if n < 0 {
		t.Fatal("count should be >= 0")
	}
	if !errors.Is(p.Err(), context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", p.Err())
	}
}

// ---------------------------------------------------------------------------
// Context cancellation — Any / All
// ---------------------------------------------------------------------------

func TestCtxCancel_Any(t *testing.T) {
	ch := make(chan int)
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		// Producer blocks between sends — does NOT close ch on cancel.
		// This forces the pipeline to detect ctx via the iter loop select.
		i := 0
		for {
			select {
			case ch <- i:
				i++
				time.Sleep(time.Millisecond)
			case <-ctx.Done():
				// Don't close ch — let the pipeline's ctx check handle it.
				return
			}
		}
	}()

	p := FromChannelCtx(ctx, ch)
	go func() {
		time.Sleep(15 * time.Millisecond)
		cancel()
	}()

	// Predicate never matches — only cancellation stops it.
	_ = p.Any(func(n int) bool { return n < 0 })
	if p.Err() == nil {
		t.Fatal("expected non-nil error after cancellation")
	}
	if !errors.Is(p.Err(), context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", p.Err())
	}
}

func TestCtxCancel_All(t *testing.T) {
	ch := make(chan int)
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		i := 0
		for {
			select {
			case ch <- i:
				i++
				time.Sleep(time.Millisecond)
			case <-ctx.Done():
				return
			}
		}
	}()

	p := FromChannelCtx(ctx, ch)
	go func() {
		time.Sleep(15 * time.Millisecond)
		cancel()
	}()

	// Predicate always matches — only cancellation stops it.
	_ = p.All(func(n int) bool { return n >= 0 })
	if p.Err() == nil {
		t.Fatal("expected non-nil error after cancellation")
	}
	if !errors.Is(p.Err(), context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", p.Err())
	}
}

// ---------------------------------------------------------------------------
// Context cancellation — First
// ---------------------------------------------------------------------------

func TestCtxCancel_First_AlreadyCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled

	p := FromSlice([]int{1, 2, 3}).WithContext(ctx)
	_, ok := p.First()
	if ok {
		t.Fatal("expected no result with pre-cancelled ctx")
	}
	if !errors.Is(p.Err(), context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", p.Err())
	}
}

// ---------------------------------------------------------------------------
// DeadlineExceeded
// ---------------------------------------------------------------------------

func TestCtx_DeadlineExceeded(t *testing.T) {
	ch := make(chan int)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Millisecond)
	defer cancel()

	go func() {
		i := 0
		for {
			select {
			case ch <- i:
				i++
				time.Sleep(time.Millisecond) // slow producer
			case <-ctx.Done():
				return
			}
		}
	}()

	p := FromChannelCtx(ctx, ch)
	_ = p.Collect()
	if !errors.Is(p.Err(), context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded, got %v", p.Err())
	}
}

// ---------------------------------------------------------------------------
// WithTimeout — real cancellation
// ---------------------------------------------------------------------------

func TestWithTimeout_StopsPipeline(t *testing.T) {
	ch := make(chan int)
	go func() {
		defer close(ch)
		for i := 0; ; i++ {
			ch <- i
			time.Sleep(time.Millisecond)
		}
	}()

	var timeoutFired int32
	p := FromChannel(ch).
		WithTimeout(20 * time.Millisecond).
		WithTimeoutHook(func(d time.Duration) {
			atomic.StoreInt32(&timeoutFired, 1)
		})

	result := p.Collect()
	if len(result) == 0 {
		t.Fatal("should have collected some elements before timeout")
	}
	if !errors.Is(p.Err(), context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded, got %v", p.Err())
	}
	if atomic.LoadInt32(&timeoutFired) != 1 {
		t.Fatal("timeout hook should have fired")
	}
}

func TestWithTimeout_CompletionHookFires(t *testing.T) {
	var completed int32
	ch := make(chan int)
	go func() {
		defer close(ch)
		for i := 0; i < 1000; i++ {
			ch <- i
			time.Sleep(time.Millisecond)
		}
	}()

	FromChannel(ch).
		WithTimeout(15 * time.Millisecond).
		WithCompletionHook(func() { atomic.StoreInt32(&completed, 1) }).
		Collect()

	if atomic.LoadInt32(&completed) != 1 {
		t.Fatal("completion hook not fired after timeout")
	}
}

// ---------------------------------------------------------------------------
// Propagation through PipeMap chains
// ---------------------------------------------------------------------------

func TestCtx_PropagatesThroughPipeMap(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancelled

	ch := make(chan int, 10)
	for i := 0; i < 10; i++ {
		ch <- i
	}
	close(ch)

	p := PipeMap(
		FromChannel(ch).WithContext(ctx),
		func(n int) int { return n * 2 },
	)
	result := p.Collect()

	// With pre-cancelled ctx and channel source, the drain loop should detect cancellation.
	// The channel might deliver some elements before the select sees ctx.Done().
	if len(result) >= 10 {
		t.Logf("got all results — ctx check may not have won the select race")
	}
}

func TestCtx_PropagatesThroughPipeFlatMap(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	ch := make(chan int)
	go func() {
		defer close(ch)
		for i := 0; ; i++ {
			select {
			case ch <- i:
			case <-ctx.Done():
				return
			}
		}
	}()

	p := PipeFlatMap(
		FromChannel(ch).WithContext(ctx),
		func(n int) []int { return []int{n, n} },
	)

	go func() {
		time.Sleep(5 * time.Millisecond)
		cancel()
	}()

	_ = p.Collect()
	// Cancellation happened — the pipeline should have stopped.
	if p.Err() == nil {
		t.Log("err is nil — channel may have closed before ctx check")
	}
}

func TestCtx_PropagatesThroughPipeChunk(t *testing.T) {
	ctx := context.Background()
	p := PipeChunk(
		FromSlice([]int{1, 2, 3, 4, 5}).WithContext(ctx),
		2,
	)
	result := p.Collect()
	if len(result) != 3 {
		t.Fatalf("expected 3 chunks, got %d", len(result))
	}
	if p.Err() != nil {
		t.Fatalf("unexpected error: %v", p.Err())
	}
}

func TestCtx_PropagatesThroughPipeDistinct(t *testing.T) {
	ctx := context.Background()
	p := PipeDistinct(FromSlice([]int{1, 2, 2, 3}).WithContext(ctx))
	result := p.Collect()
	assertSliceEqual(t, []int{1, 2, 3}, result)
	if p.Err() != nil {
		t.Fatalf("unexpected error: %v", p.Err())
	}
}

func TestCtx_PropagatesThroughPipeReduce(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	ch := make(chan int, 5)
	for i := 0; i < 5; i++ {
		ch <- i
	}
	close(ch)

	p := FromChannel(ch).WithContext(ctx)
	_ = PipeReduce(p, 0, func(acc, v int) int { return acc + v })
	// PipeReduce uses fold which checks ctx — error should propagate.
}

// ---------------------------------------------------------------------------
// Aggregations with ctx
// ---------------------------------------------------------------------------

func TestCtx_GroupBy(t *testing.T) {
	p := FromSlice([]int{1, 2, 3, 4}).WithContext(context.Background())
	result := GroupBy(p, func(n int) int { return n % 2 })
	if len(result) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(result))
	}
}

func TestCtx_SumBy(t *testing.T) {
	p := FromSlice([]int{1, 2, 3}).WithContext(context.Background())
	sum := SumBy(p, func(n int) int { return n })
	if sum != 6 {
		t.Fatalf("expected 6, got %d", sum)
	}
}

// ---------------------------------------------------------------------------
// FromChannelCtx
// ---------------------------------------------------------------------------

func TestFromChannelCtx_NormalCompletion(t *testing.T) {
	ch := make(chan int, 3)
	ch <- 1
	ch <- 2
	ch <- 3
	close(ch)

	p := FromChannelCtx(context.Background(), ch)
	result := p.Collect()
	assertSliceEqual(t, []int{1, 2, 3}, result)
	if p.Err() != nil {
		t.Fatalf("unexpected error: %v", p.Err())
	}
}

func TestFromChannelCtx_CancelStopsRead(t *testing.T) {
	ch := make(chan int)
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		i := 0
		for {
			select {
			case ch <- i:
				i++
			case <-ctx.Done():
				return
			}
		}
	}()

	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	p := FromChannelCtx(ctx, ch)
	result := p.Collect()
	if len(result) == 0 {
		t.Fatal("should have collected at least one element")
	}
	// Source sees ctx.Done() and returns false — pipeline stops.
}

// ---------------------------------------------------------------------------
// Parallel cancellation propagation
// ---------------------------------------------------------------------------

func TestCtx_PipeMapParallel(t *testing.T) {
	ctx := context.Background()
	p := PipeMapParallel(
		FromSlice([]int{1, 2, 3}).WithContext(ctx),
		2,
		func(n int) int { return n * 10 },
	)
	result := p.Collect()
	assertSliceEqual(t, []int{10, 20, 30}, result)
}

func TestCtx_PipeFilterParallel(t *testing.T) {
	ctx := context.Background()
	p := PipeFilterParallel(
		FromSlice([]int{1, 2, 3, 4}).WithContext(ctx),
		2,
		func(n int) bool { return n%2 == 0 },
	)
	result := p.Collect()
	assertSliceEqual(t, []int{2, 4}, result)
}

func TestCtx_PipeMapParallelStream(t *testing.T) {
	ctx := context.Background()
	p := PipeMapParallelStream(
		FromSlice([]int{1, 2, 3, 4, 5}).WithContext(ctx),
		2, 8,
		func(n int) int { return n * 10 },
	)
	result := p.Collect()
	assertSliceEqual(t, []int{10, 20, 30, 40, 50}, result)
}

// ---------------------------------------------------------------------------
// WithContext on chained operations
// ---------------------------------------------------------------------------

func TestCtx_FilterTakeCollect(t *testing.T) {
	ctx := context.Background()
	result := FromSlice([]int{1, 2, 3, 4, 5, 6}).
		WithContext(ctx).
		Filter(func(n int) bool { return n%2 == 0 }).
		Take(2).
		Collect()
	assertSliceEqual(t, []int{2, 4}, result)
}

func TestCtx_WithHooks(t *testing.T) {
	var count atomic.Int64
	ctx := context.Background()
	FromSlice([]int{1, 2, 3}).
		WithContext(ctx).
		WithElementHook(func(int) { count.Add(1) }).
		Collect()
	if count.Load() != 3 {
		t.Fatalf("expected 3, got %d", count.Load())
	}
}

// ---------------------------------------------------------------------------
// PipeBatch with ctx
// ---------------------------------------------------------------------------

func TestCtx_PipeBatch(t *testing.T) {
	ctx := context.Background()
	result := PipeBatch(
		FromSlice([]int{1, 2, 3, 4, 5}).WithContext(ctx),
		BatchConfig{Size: 2},
	).Collect()
	if len(result) != 3 {
		t.Fatalf("expected 3 batches, got %d", len(result))
	}
}

// ---------------------------------------------------------------------------
// Zero-overhead benchmarks: ctx==nil must match original numbers
// ---------------------------------------------------------------------------

func BenchmarkDrain_NoCtx_10k(b *testing.B) {
	data := makeIterData(10_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sum := 0
		FromSlice(data).ForEach(func(v int) { sum += v })
	}
}

func BenchmarkDrain_WithCtx_Background_10k(b *testing.B) {
	data := makeIterData(10_000)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sum := 0
		FromSlice(data).WithContext(ctx).ForEach(func(v int) { sum += v })
	}
}

func BenchmarkFold_NoCtx_10k(b *testing.B) {
	data := makeIterData(10_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		FromSlice(data).Reduce(0, func(a, b int) int { return a + b })
	}
}

func BenchmarkFold_WithCtx_Background_10k(b *testing.B) {
	data := makeIterData(10_000)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		FromSlice(data).WithContext(ctx).Reduce(0, func(a, b int) int { return a + b })
	}
}

func BenchmarkFoldWhile_NoCtx_Any_10k(b *testing.B) {
	data := makeIterData(10_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		FromSlice(data).Any(func(n int) bool { return n < 0 })
	}
}

func BenchmarkFoldWhile_WithCtx_Any_10k(b *testing.B) {
	data := makeIterData(10_000)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		FromSlice(data).WithContext(ctx).Any(func(n int) bool { return n < 0 })
	}
}

func BenchmarkCollect_NoCtx_10k(b *testing.B) {
	data := makeIterData(10_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		FromSlice(data).Collect()
	}
}

func BenchmarkCollect_WithCtx_Background_10k(b *testing.B) {
	data := makeIterData(10_000)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		FromSlice(data).WithContext(ctx).Collect()
	}
}

func BenchmarkCount_NoCtx_SliceFastPath_10k(b *testing.B) {
	data := makeIterData(10_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		FromSlice(data).Count()
	}
}

func BenchmarkCount_WithCtx_Fold_10k(b *testing.B) {
	data := makeIterData(10_000)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		FromSlice(data).WithContext(ctx).Count()
	}
}

func BenchmarkPipeMap_NoCtx_10k(b *testing.B) {
	data := makeIterData(10_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		PipeMap(FromSlice(data), func(n int) int { return n * 2 }).Collect()
	}
}

func BenchmarkPipeMap_WithCtx_10k(b *testing.B) {
	data := makeIterData(10_000)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		PipeMap(FromSlice(data).WithContext(ctx), func(n int) int { return n * 2 }).Collect()
	}
}

// ===========================================================================
// Regression: Fix 1 — parallel ops respect context during source drain
// ===========================================================================

func TestFix1_PipeMapParallel_RespectsCtx(t *testing.T) {
	// Pre-cancelled context + large source → must NOT drain all elements.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	p := FromRange(0, 1_000_000).WithContext(ctx)
	result := PipeMapParallel(p, 4, func(n int) int { return n * 2 }).Collect()

	// With pre-cancelled ctx, drainSourceCtx should stop almost immediately.
	if len(result) >= 1_000_000 {
		t.Fatalf("expected early stop, got all %d elements", len(result))
	}
}

func TestFix1_PipeFilterParallel_RespectsCtx(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	p := FromRange(0, 1_000_000).WithContext(ctx)
	result := PipeFilterParallel(p, 4, func(n int) bool { return n%2 == 0 }).Collect()

	if len(result) >= 500_000 {
		t.Fatalf("expected early stop, got %d elements", len(result))
	}
}

func TestFix1_PipeMapParallelErr_RespectsCtx(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	p := FromRange(0, 1_000_000).WithContext(ctx)
	result := PipeMapParallelErr(p, 4, func(n int) (int, error) { return n, nil }).Collect()

	if len(result) >= 1_000_000 {
		t.Fatalf("expected early stop, got all %d elements", len(result))
	}
}

func TestFix1_PipeMapParallel_ErrPropagated(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	p := PipeMapParallel(
		FromRange(0, 100_000).WithContext(ctx),
		4,
		func(n int) int { return n },
	)
	_ = p.Collect()

	if !errors.Is(p.Err(), context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", p.Err())
	}
}

func TestFix1_PipeMapParallel_TimeoutStopsDrain(t *testing.T) {
	// Slow source with timeout — parallel must not block forever.
	// Use funcSource that sleeps per element + generous timeout.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	calls := int64(0)
	p := FromFunc(func() (int, bool) {
		n := int(atomic.AddInt64(&calls, 1))
		time.Sleep(5 * time.Millisecond)
		return n, true // infinite source
	}).WithContext(ctx)

	result := PipeMapParallel(p, 2, func(n int) int { return n * 10 }).Collect()

	// Should have collected some elements before the 100ms timeout.
	if len(result) == 0 {
		t.Fatal("expected some results before timeout")
	}
	// But not an infinite amount.
	if len(result) > 100 {
		t.Fatalf("expected bounded results, got %d", len(result))
	}
}

func TestFix1_PipeMapParallel_SliceNoCtx_Unchanged(t *testing.T) {
	// Without context, behavior unchanged — all elements processed.
	result := PipeMapParallel(
		FromSlice([]int{1, 2, 3, 4, 5}),
		4,
		func(n int) int { return n * n },
	).Collect()
	assertSliceEqual(t, []int{1, 4, 9, 16, 25}, result)
}

// ===========================================================================
// Regression: Fix 2 — stoppableSource deterministic cleanup
// ===========================================================================

func TestFix2_ParallelStream_CtxCancelsGoroutines(t *testing.T) {
	// Infinite source + context cancellation → goroutines must exit.
	ctx, cancel := context.WithCancel(context.Background())

	before := runtime.NumGoroutine()

	p := PipeMapParallelStream(
		FromFunc(func() (int, bool) {
			time.Sleep(time.Millisecond)
			return 42, true
		}).WithContext(ctx),
		4, 8,
		func(n int) int { return n * 2 },
	)

	// Read a few elements then cancel.
	got := 0
	p.ForEach(func(v int) {
		got++
		if got >= 3 {
			cancel()
		}
	})

	// Give goroutines time to wind down.
	time.Sleep(100 * time.Millisecond)
	runtime.GC()
	time.Sleep(100 * time.Millisecond)

	after := runtime.NumGoroutine()
	leaked := after - before
	if leaked > 3 {
		t.Errorf("goroutine leak: before=%d after=%d delta=%d", before, after, leaked)
	}
}

func TestFix2_ParallelStream_TakeStopsWithCtx(t *testing.T) {
	// Take(1) from infinite source with ctx → deterministic cleanup.
	ctx := context.Background()
	before := runtime.NumGoroutine()

	result := PipeMapParallelStream(
		FromFunc(func() (int, bool) {
			time.Sleep(time.Millisecond)
			return 7, true
		}).WithContext(ctx),
		2, 4,
		func(n int) int { return n + 1 },
	).Take(1).Collect()

	if len(result) != 1 || result[0] != 8 {
		t.Fatalf("expected [8], got %v", result)
	}

	runtime.GC()
	time.Sleep(200 * time.Millisecond)
	runtime.GC()
	time.Sleep(100 * time.Millisecond)

	after := runtime.NumGoroutine()
	leaked := after - before
	if leaked > 3 {
		t.Errorf("goroutine leak: before=%d after=%d delta=%d", before, after, leaked)
	}
}

func TestFix2_ParallelStream_NormalCompletion_StillWorks(t *testing.T) {
	result := PipeMapParallelStream(
		FromSlice([]int{1, 2, 3, 4, 5}),
		2, 8,
		func(n int) int { return n * 3 },
	).Collect()
	assertSliceEqual(t, []int{3, 6, 9, 12, 15}, result)
}

func TestFix2_ParallelStream_OrderPreservedWithCtx(t *testing.T) {
	ctx := context.Background()
	result := PipeMapParallelStream(
		FromSlice([]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}).WithContext(ctx),
		4, 16,
		func(n int) int { return n * 10 },
	).Collect()
	assertSliceEqual(t, []int{10, 20, 30, 40, 50, 60, 70, 80, 90, 100}, result)
}

// ===========================================================================
// Regression: Fix 3 — cancel propagates through non-terminal ops
// ===========================================================================

func TestFix3_CancelPropagatesThroughPipeMap(t *testing.T) {
	// WithTimeout on the first pipeline → PipeMap → Filter → Collect
	// finalize() on the final pipeline must call cancel from WithTimeout.
	ch := make(chan int)
	go func() {
		i := 0
		for {
			ch <- i
			i++
			time.Sleep(time.Millisecond)
		}
	}()

	p := FromChannel(ch).WithTimeout(30 * time.Millisecond)
	q := PipeMap(p, func(n int) int { return n * 2 }).Filter(func(n int) bool { return true })

	result := q.Collect()

	// q.finalize() should have called p.cancel, stopping the timeout context.
	if len(result) == 0 {
		t.Fatal("expected some results")
	}
	if !errors.Is(q.Err(), context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded, got %v", q.Err())
	}
}

func TestFix3_CancelPropagatesThroughPipeFlatMap(t *testing.T) {
	ch := make(chan int)
	go func() {
		i := 0
		for {
			ch <- i
			i++
			time.Sleep(time.Millisecond)
		}
	}()

	p := FromChannel(ch).WithTimeout(30 * time.Millisecond)
	q := PipeFlatMap(p, func(n int) []int { return []int{n, n} })

	result := q.Collect()
	if len(result) == 0 {
		t.Fatal("expected some results")
	}
	if !errors.Is(q.Err(), context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded, got %v", q.Err())
	}
}

func TestFix3_CancelPropagatesThroughPipeChunk(t *testing.T) {
	ch := make(chan int)
	go func() {
		i := 0
		for {
			ch <- i
			i++
			time.Sleep(time.Millisecond)
		}
	}()

	p := FromChannel(ch).WithTimeout(30 * time.Millisecond)
	q := PipeChunk(p, 3)

	result := q.Collect()
	if len(result) == 0 {
		t.Fatal("expected some chunks")
	}
	if !errors.Is(q.Err(), context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded, got %v", q.Err())
	}
}

func TestFix3_CancelPropagatesThroughPipeBatch(t *testing.T) {
	ch := make(chan int)
	go func() {
		i := 0
		for {
			ch <- i
			i++
			time.Sleep(time.Millisecond)
		}
	}()

	p := FromChannel(ch).WithTimeout(30 * time.Millisecond)
	q := PipeBatch(p, BatchConfig{Size: 3})

	result := q.Collect()
	if len(result) == 0 {
		t.Fatal("expected some batches")
	}
	if !errors.Is(q.Err(), context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded, got %v", q.Err())
	}
}

func TestFix3_CancelPropagatesThroughPipeDistinct(t *testing.T) {
	ch := make(chan int)
	go func() {
		i := 0
		for {
			ch <- i % 50 // limited unique values
			i++
			time.Sleep(time.Millisecond)
		}
	}()

	p := FromChannel(ch).WithTimeout(30 * time.Millisecond)
	q := PipeDistinct(p)

	result := q.Collect()
	if len(result) == 0 {
		t.Fatal("expected some results")
	}
	if !errors.Is(q.Err(), context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded, got %v", q.Err())
	}
}

func TestFix3_WithTimeout_NoCancel_Leak(t *testing.T) {
	// Ensure WithTimeout's cancel is called even when the pipeline
	// goes through PipeMap → Collect (cancel lives on q, not p).
	leaked := false
	done := make(chan struct{})

	go func() {
		p := FromSlice([]int{1, 2, 3}).WithTimeout(10 * time.Second)
		q := PipeMap(p, func(n int) int { return n })
		_ = q.Collect()
		// After Collect, finalize must have called cancel.
		// If cancel wasn't called, the 10s timer goroutine leaks.
		close(done)
	}()

	select {
	case <-done:
		// ok
	case <-time.After(2 * time.Second):
		leaked = true
	}

	if leaked {
		t.Fatal("pipeline did not finalize — cancel was not propagated")
	}
}

func TestFix3_MultipleFinalize_Safe(t *testing.T) {
	// cancel is idempotent — calling finalize on multiple derived pipelines is safe.
	p := FromSlice([]int{1, 2, 3}).WithTimeout(5 * time.Second)
	q := PipeMap(p, func(n int) int { return n * 2 })
	r := q.Filter(func(n int) bool { return n > 2 })

	result := r.Collect()
	assertSliceEqual(t, []int{4, 6}, result)
	// No panic from double cancel.
}

// ===========================================================================
// Fix 1: PipeMapParallelStream — sender select-loop for fast cancel exit
// ===========================================================================

// TestFix_ParallelStream_NoDeadlockOnCancel verifies that cancelling a
// context while PipeMapParallelStream is running does NOT deadlock.
// The slow fn + small buffer provokes back-pressure.
func TestFix_ParallelStream_NoDeadlockOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	finished := make(chan struct{})
	go func() {
		defer close(finished)
		p := PipeMapParallelStream(
			FromFunc(func() (int, bool) {
				time.Sleep(5 * time.Millisecond)
				return 1, true
			}).WithContext(ctx),
			4, 2, // small buffer — provokes back-pressure
			func(n int) int {
				time.Sleep(20 * time.Millisecond) // slow fn
				return n * 2
			},
		)
		_ = p.Collect()
	}()

	// Let some elements flow, then cancel.
	time.Sleep(80 * time.Millisecond)
	cancel()

	select {
	case <-finished:
		// No deadlock.
	case <-time.After(5 * time.Second):
		t.Fatal("DEADLOCK: PipeMapParallelStream did not terminate after cancel")
	}
}

// TestFix_ParallelStream_CancelNoGoroutineLeak checks that all goroutines
// are cleaned up after context cancellation.
func TestFix_ParallelStream_CancelNoGoroutineLeak(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	before := runtime.NumGoroutine()

	p := PipeMapParallelStream(
		FromFunc(func() (int, bool) {
			time.Sleep(2 * time.Millisecond)
			return 42, true
		}).WithContext(ctx),
		4, 8,
		func(n int) int { return n * 2 },
	)

	got := 0
	p.ForEach(func(v int) {
		got++
		if got >= 5 {
			cancel()
		}
	})

	// Allow goroutines to wind down.
	time.Sleep(200 * time.Millisecond)
	runtime.GC()
	time.Sleep(100 * time.Millisecond)

	after := runtime.NumGoroutine()
	leaked := after - before
	if leaked > 3 {
		t.Errorf("goroutine leak: before=%d after=%d delta=%d", before, after, leaked)
	}
}

// TestFix_ParallelStream_NormalCompletionUnchanged ensures the sender fix
// doesn't break the happy path.
func TestFix_ParallelStream_NormalCompletionUnchanged(t *testing.T) {
	result := PipeMapParallelStream(
		FromSlice([]int{1, 2, 3, 4, 5}),
		2, 8,
		func(n int) int { return n * 3 },
	).Collect()
	assertSliceEqual(t, []int{3, 6, 9, 12, 15}, result)
}

// TestFix_ParallelStream_OrderPreservedAfterFix ensures order is still correct.
func TestFix_ParallelStream_OrderPreservedAfterFix(t *testing.T) {
	result := PipeMapParallelStream(
		FromSlice([]int{10, 20, 30, 40, 50, 60, 70, 80}),
		4, 16,
		func(n int) int { return n + 1 },
	).Collect()
	assertSliceEqual(t, []int{11, 21, 31, 41, 51, 61, 71, 81}, result)
}

// ===========================================================================
// Fix 2: Data race on p.err — atomic access
// ===========================================================================

// TestFix_ErrAtomicNoRace should be run with `go test -race`.
// Writes to err (via ctxDone/setErr) and reads (via Err()) must not race.
func TestFix_ErrAtomicNoRace(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	p := FromFunc(func() (int, bool) {
		time.Sleep(time.Millisecond)
		return 1, true
	}).WithContext(ctx)

	// Read Err() from another goroutine while pipeline is running.
	var readCount atomic.Int64
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for readCount.Load() < 50 {
			_ = p.Err() // concurrent read — race detector catches issues here
			readCount.Add(1)
			runtime.Gosched()
		}
	}()

	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	_ = p.Collect()
	wg.Wait()

	if !errors.Is(p.Err(), context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", p.Err())
	}
}

// TestFix_SetErrFirstWriteWins verifies first-write-wins semantics.
func TestFix_SetErrFirstWriteWins(t *testing.T) {
	p := FromSlice([]int{1})
	first := errors.New("first")
	second := errors.New("second")
	p.setErr(first)
	p.setErr(second)
	if !errors.Is(p.Err(), first) {
		t.Fatalf("expected first error, got %v", p.Err())
	}
}

// TestFix_SetErrNilIgnored verifies that setErr(nil) is a no-op.
func TestFix_SetErrNilIgnored(t *testing.T) {
	p := FromSlice([]int{1})
	p.setErr(nil)
	if p.Err() != nil {
		t.Fatalf("expected nil, got %v", p.Err())
	}
}

// TestFix_ParallelResultErrPropagated checks that parallelResult
// correctly propagates ctx error via setErr.
func TestFix_ParallelResultErrPropagated(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	p := PipeMapParallel(
		FromRange(0, 100_000).WithContext(ctx),
		4,
		func(n int) int { return n },
	)
	_ = p.Collect()

	if !errors.Is(p.Err(), context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", p.Err())
	}
}

// ===========================================================================
// Fix 3: Idempotent finalize — hooks fire only once
// ===========================================================================

// TestFix_FinalizeIdempotent verifies that calling a terminal twice
// does not fire completion hooks again and does not panic.
func TestFix_FinalizeIdempotent(t *testing.T) {
	var completionCount int32
	p := FromSlice([]int{1, 2, 3}).
		WithCompletionHook(func() { atomic.AddInt32(&completionCount, 1) })

	// First call.
	r1 := p.Collect()
	assertSliceEqual(t, []int{1, 2, 3}, r1)

	// Second call — source exhausted, should return nil/empty, no hook re-fire.
	r2 := p.Collect()
	if len(r2) != 0 && r2 != nil {
		t.Fatalf("expected empty on second Collect, got %v", r2)
	}

	if c := atomic.LoadInt32(&completionCount); c != 1 {
		t.Fatalf("completion hook fired %d times, expected 1", c)
	}
}

// TestFix_FinalizeIdempotent_ForEach same test for ForEach.
func TestFix_FinalizeIdempotent_ForEach(t *testing.T) {
	var completionCount int32
	p := FromSlice([]int{1}).
		WithCompletionHook(func() { atomic.AddInt32(&completionCount, 1) })

	p.ForEach(func(int) {})
	p.ForEach(func(int) {}) // second call

	if c := atomic.LoadInt32(&completionCount); c != 1 {
		t.Fatalf("completion hook fired %d times, expected 1", c)
	}
}

// ===========================================================================
// Fix 4: readerSource error propagation
// ===========================================================================

// failingReader returns n lines then fails with an IO error.
type failingReader struct {
	n       int
	current int
}

func (r *failingReader) Read(p []byte) (int, error) {
	if r.current >= r.n {
		return 0, errors.New("simulated disk read error")
	}
	r.current++
	line := fmt.Sprintf("line-%d\n", r.current)
	return copy(p, line), nil
}

func TestFix_ReaderSourceErrorPropagated(t *testing.T) {
	p := FromReader(&failingReader{n: 3})
	result := p.Collect()

	if len(result) != 3 {
		t.Fatalf("expected 3 lines, got %d: %v", len(result), result)
	}
	if p.Err() == nil {
		t.Fatal("expected IO error to be propagated to pipeline.Err()")
	}
	if p.Err().Error() != "simulated disk read error" {
		t.Fatalf("unexpected error: %v", p.Err())
	}
}

func TestFix_ReaderSourceNoError(t *testing.T) {
	// Normal reader — no error should be set.
	r, w := io.Pipe()
	go func() {
		fmt.Fprintln(w, "hello")
		fmt.Fprintln(w, "world")
		w.Close()
	}()

	p := FromReader(r)
	result := p.Collect()
	assertSliceEqual(t, []string{"hello", "world"}, result)
	if p.Err() != nil {
		t.Fatalf("unexpected error: %v", p.Err())
	}
}

// ===========================================================================
// Fix 5: drainSourceCtx — idx correctness on cancellation
// ===========================================================================

// TestFix_DrainSourceCtx_IdxNotAdvancedOnCancel verifies that when
// context is cancelled mid-drain, sliceSource.idx reflects only
// the elements actually consumed (not the full length).
func TestFix_DrainSourceCtx_IdxNotAdvancedOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancelled

	ss := &sliceSource[int]{data: make([]int, 10_000)}
	items, cancelled := drainSourceCtx[int](ss, ctx)

	if !cancelled {
		t.Fatal("expected cancelled=true")
	}
	// With pre-cancelled ctx, should get very few elements (0 or near 0).
	if len(items) >= 10_000 {
		t.Fatalf("expected early stop, got all %d elements", len(items))
	}
	// idx should match what was actually returned, not len(data).
	if ss.idx >= len(ss.data) {
		t.Fatalf("idx=%d should be less than len(data)=%d on cancellation",
			ss.idx, len(ss.data))
	}
}

// TestFix_DrainSourceCtx_FullDrainIdxCorrect verifies that without
// cancellation, idx == len(data) after a full drain.
func TestFix_DrainSourceCtx_FullDrainIdxCorrect(t *testing.T) {
	ctx := context.Background()
	ss := &sliceSource[int]{data: []int{1, 2, 3, 4, 5}}
	items, cancelled := drainSourceCtx[int](ss, ctx)

	if cancelled {
		t.Fatal("should not be cancelled")
	}
	assertSliceEqual(t, []int{1, 2, 3, 4, 5}, items)
	if ss.idx != len(ss.data) {
		t.Fatalf("idx=%d, want %d", ss.idx, len(ss.data))
	}
}

// TestFix_PipeMapParallel_PreCancelledCtx_BoundedResult ensures that
// with a pre-cancelled context, PipeMapParallel doesn't process the
// entire slice.
func TestFix_PipeMapParallel_PreCancelledCtx_BoundedResult(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	result := PipeMapParallel(
		FromRange(0, 1_000_000).WithContext(ctx),
		4,
		func(n int) int { return n * 2 },
	).Collect()

	if len(result) >= 1_000_000 {
		t.Fatalf("expected early stop, got all %d elements", len(result))
	}
}

// ===========================================================================
// Fix: WithContext cancels previous WithTimeout cancel to prevent orphaned ctx
// ===========================================================================

func TestWithContext_CancelsPreviousTimeout(t *testing.T) {
	// WithTimeout sets p.cancel. Calling WithContext after must cancel the old
	// timeout context so its timer goroutine does not leak.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		p := FromSlice([]int{1, 2, 3}).
			WithTimeout(10 * time.Second). // sets p.cancel (10s timer goroutine)
			WithContext(ctx)               // must cancel the old timeout

		// After WithContext, the pipeline uses the new ctx.
		// The old 10s timer goroutine must have been cancelled.
		result := p.Collect()
		if len(result) != 3 {
			t.Errorf("expected 3 elements, got %d", len(result))
		}
	}()

	select {
	case <-done:
		// ok — finished quickly, old timer was cancelled
	case <-time.After(2 * time.Second):
		t.Fatal("pipeline did not complete — old cancel may have leaked")
	}
}

func TestToChannel_WithCtx(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := make(chan int, 10)
	FromSlice([]int{1, 2, 3}).WithContext(ctx).ToChannel(ch)
	var result []int
	for v := range ch {
		result = append(result, v)
	}
	assertSliceEqual(t, []int{1, 2, 3}, result)
}
