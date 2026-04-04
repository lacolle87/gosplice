package gosplice

import (
	"context"
	"testing"
)

// ===========================================================================
// ctxNoop detection
// ===========================================================================

func TestCtxNoop_BackgroundIsNoop(t *testing.T) {
	p := FromSlice([]int{1}).WithContext(context.Background())
	if !p.ctxNoop {
		t.Fatal("Background() should set ctxNoop = true")
	}
}

func TestCtxNoop_TODOIsNoop(t *testing.T) {
	p := FromSlice([]int{1}).WithContext(context.TODO())
	if !p.ctxNoop {
		t.Fatal("TODO() should set ctxNoop = true")
	}
}

func TestCtxNoop_CancelableIsNotNoop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := FromSlice([]int{1}).WithContext(ctx)
	if p.ctxNoop {
		t.Fatal("WithCancel should set ctxNoop = false")
	}
}

func TestCtxNoop_NilCtxIsNotNoop(t *testing.T) {
	p := FromSlice([]int{1})
	if p.ctxNoop {
		t.Fatal("nil ctx should have ctxNoop = false")
	}
}

func TestCtxNoop_WithTimeoutResetsNoop(t *testing.T) {
	p := FromSlice([]int{1}).
		WithContext(context.Background()).
		WithTimeout(1000)
	if p.ctxNoop {
		t.Fatal("WithTimeout should reset ctxNoop to false")
	}
}

func TestCtxNoop_FromChannelCtxBackground(t *testing.T) {
	ch := make(chan int, 1)
	ch <- 1
	close(ch)
	p := FromChannelCtx(context.Background(), ch)
	if !p.ctxNoop {
		t.Fatal("FromChannelCtx(Background) should set ctxNoop = true")
	}
	result := p.Collect()
	assertSliceEqual(t, []int{1}, result)
}

// ===========================================================================
// ctxActive helper
// ===========================================================================

func TestCtxActive_NilCtx(t *testing.T) {
	p := FromSlice([]int{1})
	if p.ctxActive() {
		t.Fatal("nil ctx should not be active")
	}
}

func TestCtxActive_Background(t *testing.T) {
	p := FromSlice([]int{1}).WithContext(context.Background())
	if p.ctxActive() {
		t.Fatal("Background should not be active (ctxNoop)")
	}
}

func TestCtxActive_Cancelable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := FromSlice([]int{1}).WithContext(ctx)
	if !p.ctxActive() {
		t.Fatal("cancelable ctx should be active")
	}
}

// ===========================================================================
// Correctness: ctxNoop uses fast-path and gives correct results
// ===========================================================================

func TestCtxNoop_Collect_UsesDirectCollectable(t *testing.T) {
	data := makeIterData(1000)
	want := FromSlice(data).Collect()
	got := FromSlice(data).WithContext(context.Background()).Collect()
	assertSliceEqual(t, want, got)
}

func TestCtxNoop_Count_UsesSliceFastPath(t *testing.T) {
	data := makeIterData(10_000)
	want := FromSlice(data).Count()
	got := FromSlice(data).WithContext(context.Background()).Count()
	if got != want {
		t.Fatalf("Count: want %d, got %d", want, got)
	}
}

func TestCtxNoop_Reduce_UsesSliceFastPath(t *testing.T) {
	data := makeIterData(1000)
	want := FromSlice(data).Reduce(0, func(a, b int) int { return a + b })
	got := FromSlice(data).WithContext(context.Background()).Reduce(0, func(a, b int) int { return a + b })
	if got != want {
		t.Fatalf("Reduce: want %d, got %d", want, got)
	}
}

func TestCtxNoop_SumBy(t *testing.T) {
	data := makeIterData(1000)
	want := SumBy(FromSlice(data), func(n int) int { return n })
	got := SumBy(FromSlice(data).WithContext(context.Background()), func(n int) int { return n })
	if got != want {
		t.Fatalf("SumBy: want %d, got %d", want, got)
	}
}

func TestCtxNoop_ForEach_Drain(t *testing.T) {
	data := makeIterData(1000)
	var sum1, sum2 int
	FromSlice(data).ForEach(func(v int) { sum1 += v })
	FromSlice(data).WithContext(context.Background()).ForEach(func(v int) { sum2 += v })
	if sum1 != sum2 {
		t.Fatalf("ForEach: want %d, got %d", sum1, sum2)
	}
}

func TestCtxNoop_Any(t *testing.T) {
	data := makeIterData(1000)
	want := FromSlice(data).Any(func(n int) bool { return n == 500 })
	got := FromSlice(data).WithContext(context.Background()).Any(func(n int) bool { return n == 500 })
	if got != want {
		t.Fatalf("Any: want %v, got %v", want, got)
	}
}

func TestCtxNoop_All(t *testing.T) {
	data := makeIterData(1000)
	want := FromSlice(data).All(func(n int) bool { return n >= 0 })
	got := FromSlice(data).WithContext(context.Background()).All(func(n int) bool { return n >= 0 })
	if got != want {
		t.Fatalf("All: want %v, got %v", want, got)
	}
}

func TestCtxNoop_First(t *testing.T) {
	v1, ok1 := FromSlice([]int{10, 20}).First()
	v2, ok2 := FromSlice([]int{10, 20}).WithContext(context.Background()).First()
	if v1 != v2 || ok1 != ok2 {
		t.Fatalf("First: want (%d,%v), got (%d,%v)", v1, ok1, v2, ok2)
	}
}

func TestCtxNoop_PipeReduce(t *testing.T) {
	data := makeIterData(1000)
	want := PipeReduce(FromSlice(data), 0, func(acc, v int) int { return acc + v })
	got := PipeReduce(FromSlice(data).WithContext(context.Background()), 0, func(acc, v int) int { return acc + v })
	if got != want {
		t.Fatalf("PipeReduce: want %d, got %d", want, got)
	}
}

// ===========================================================================
// Propagation: ctxNoop survives pipeline stages
// ===========================================================================

func TestCtxNoop_PropagatesThroughFilter(t *testing.T) {
	result := FromSlice([]int{1, 2, 3, 4, 5}).
		WithContext(context.Background()).
		Filter(func(n int) bool { return n%2 == 0 }).
		Collect()
	assertSliceEqual(t, []int{2, 4}, result)
}

func TestCtxNoop_PropagatesThroughPipeMap(t *testing.T) {
	result := PipeMap(
		FromSlice([]int{1, 2, 3}).WithContext(context.Background()),
		func(n int) int { return n * 10 },
	).Collect()
	assertSliceEqual(t, []int{10, 20, 30}, result)
}

func TestCtxNoop_PropagatesThroughPipeFlatMap(t *testing.T) {
	result := PipeFlatMap(
		FromSlice([]int{1, 2}).WithContext(context.Background()),
		func(n int) []int { return []int{n, n * 10} },
	).Collect()
	assertSliceEqual(t, []int{1, 10, 2, 20}, result)
}

func TestCtxNoop_PropagatesThroughPipeDistinct(t *testing.T) {
	result := PipeDistinct(
		FromSlice([]int{1, 2, 2, 3}).WithContext(context.Background()),
	).Collect()
	assertSliceEqual(t, []int{1, 2, 3}, result)
}

func TestCtxNoop_PropagatesThroughPipeChunk(t *testing.T) {
	result := PipeChunk(
		FromSlice([]int{1, 2, 3, 4, 5}).WithContext(context.Background()),
		2,
	).Collect()
	if len(result) != 3 {
		t.Fatalf("expected 3 chunks, got %d", len(result))
	}
}

func TestCtxNoop_PropagatesThroughPipeReduce(t *testing.T) {
	got := PipeReduce(
		FromSlice([]int{1, 2, 3}).WithContext(context.Background()),
		0, func(acc, v int) int { return acc + v },
	)
	if got != 6 {
		t.Fatalf("PipeReduce: want 6, got %d", got)
	}
}

func TestCtxNoop_PropagatesThroughTakeSkip(t *testing.T) {
	result := FromSlice([]int{1, 2, 3, 4, 5}).
		WithContext(context.Background()).
		Skip(1).
		Take(3).
		Collect()
	assertSliceEqual(t, []int{2, 3, 4}, result)
}

func TestCtxNoop_PropagatesThroughPeek(t *testing.T) {
	var peeked []int
	result := FromSlice([]int{1, 2, 3}).
		WithContext(context.Background()).
		Peek(func(v int) { peeked = append(peeked, v) }).
		Collect()
	assertSliceEqual(t, []int{1, 2, 3}, result)
	assertSliceEqual(t, []int{1, 2, 3}, peeked)
}

func TestCtxNoop_PropagatesThroughPipeBatch(t *testing.T) {
	result := PipeBatch(
		FromSlice([]int{1, 2, 3, 4, 5}).WithContext(context.Background()),
		BatchConfig{Size: 2},
	).Collect()
	if len(result) != 3 {
		t.Fatalf("expected 3 batches, got %d", len(result))
	}
}

// ===========================================================================
// drainSourceCtx optimization for uncancelable contexts
// ===========================================================================

func TestDrainSourceCtx_BackgroundSkipsChecks(t *testing.T) {
	ss := &sliceSource[int]{data: []int{1, 2, 3, 4, 5}}
	items, cancelled := drainSourceCtx[int](ss, context.Background())
	if cancelled {
		t.Fatal("Background should not report cancelled")
	}
	assertSliceEqual(t, []int{1, 2, 3, 4, 5}, items)
	if ss.idx != len(ss.data) {
		t.Fatalf("idx=%d, want %d", ss.idx, len(ss.data))
	}
}

// ===========================================================================
// Benchmarks: Background() should now match ctx==nil
// ===========================================================================

func BenchmarkCollect_CtxNoop_10k(b *testing.B) {
	data := makeIterData(10_000)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		FromSlice(data).WithContext(ctx).Collect()
	}
}

func BenchmarkCount_CtxNoop_SliceFastPath_10k(b *testing.B) {
	data := makeIterData(10_000)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		FromSlice(data).WithContext(ctx).Count()
	}
}

func BenchmarkFold_CtxNoop_10k(b *testing.B) {
	data := makeIterData(10_000)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		FromSlice(data).WithContext(ctx).Reduce(0, func(a, b int) int { return a + b })
	}
}

func BenchmarkDrain_CtxNoop_10k(b *testing.B) {
	data := makeIterData(10_000)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sum := 0
		FromSlice(data).WithContext(ctx).ForEach(func(v int) { sum += v })
	}
}

func BenchmarkPipeMap_CtxNoop_10k(b *testing.B) {
	data := makeIterData(10_000)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		PipeMap(FromSlice(data).WithContext(ctx), func(n int) int { return n * 2 }).Collect()
	}
}

func BenchmarkFoldWhile_CtxNoop_Any_10k(b *testing.B) {
	data := makeIterData(10_000)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		FromSlice(data).WithContext(ctx).Any(func(n int) bool { return n == 9999 })
	}
}
