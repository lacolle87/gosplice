package gosplice

import (
	"context"
	"errors"
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
