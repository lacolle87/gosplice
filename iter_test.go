package gosplice

import (
	"sync/atomic"
	"testing"
)

// ---------------------------------------------------------------------------
// drain
// ---------------------------------------------------------------------------

func TestDrain_SliceFastPath(t *testing.T) {
	var sum int
	FromSlice([]int{1, 2, 3, 4}).ForEach(func(v int) { sum += v })
	if sum != 10 {
		t.Fatalf("want 10, got %d", sum)
	}
}

func TestDrain_ChannelSource(t *testing.T) {
	ch := make(chan int, 4)
	for _, v := range []int{10, 20, 30, 40} {
		ch <- v
	}
	close(ch)
	var sum int
	FromChannel(ch).ForEach(func(v int) { sum += v })
	if sum != 100 {
		t.Fatalf("want 100, got %d", sum)
	}
}

func TestDrain_WithHooks(t *testing.T) {
	var hookCount atomic.Int64
	completed := false
	var sum int
	FromSlice([]int{5, 10, 15}).
		WithElementHook(func(int) { hookCount.Add(1) }).
		WithCompletionHook(func() { completed = true }).
		ForEach(func(v int) { sum += v })

	if sum != 30 {
		t.Fatalf("sum: want 30, got %d", sum)
	}
	if hookCount.Load() != 3 {
		t.Fatalf("hookCount: want 3, got %d", hookCount.Load())
	}
	if !completed {
		t.Fatal("completion hook not fired")
	}
}

func TestDrain_Empty(t *testing.T) {
	called := false
	FromSlice([]int{}).ForEach(func(int) { called = true })
	if called {
		t.Fatal("fn called on empty pipeline")
	}
}

func TestDrain_CollectFallback(t *testing.T) {
	i := 0
	data := []int{10, 20, 30}
	result := FromFunc(func() (int, bool) {
		if i >= len(data) {
			return 0, false
		}
		v := data[i]
		i++
		return v, true
	}).Collect()
	assertSliceEqual(t, []int{10, 20, 30}, result)
}

func TestDrain_CollectTo(t *testing.T) {
	buf := make([]int, 0, 100)
	result := FromSlice([]int{1, 2, 3}).CollectTo(buf)
	assertSliceEqual(t, []int{1, 2, 3}, result)
	if cap(result) != 100 {
		t.Fatalf("cap: want 100, got %d", cap(result))
	}
}

func TestDrain_FilterChain(t *testing.T) {
	var result []int
	FromSlice([]int{1, 2, 3, 4, 5}).
		Filter(func(n int) bool { return n%2 == 0 }).
		ForEach(func(v int) { result = append(result, v) })
	assertSliceEqual(t, []int{2, 4}, result)
}

// ---------------------------------------------------------------------------
// fold
// ---------------------------------------------------------------------------

func TestFold_Reduce(t *testing.T) {
	got := FromSlice([]int{1, 2, 3, 4}).Reduce(0, func(a, b int) int { return a + b })
	if got != 10 {
		t.Fatalf("want 10, got %d", got)
	}
}

func TestFold_Reduce_WithHooks(t *testing.T) {
	var hookCount atomic.Int64
	got := FromSlice([]int{1, 2, 3}).
		WithElementHook(func(int) { hookCount.Add(1) }).
		Reduce(100, func(a, b int) int { return a + b })
	if got != 106 {
		t.Fatalf("want 106, got %d", got)
	}
	if hookCount.Load() != 3 {
		t.Fatalf("hookCount: want 3, got %d", hookCount.Load())
	}
}

func TestFold_PipeReduce(t *testing.T) {
	got := PipeReduce(
		FromSlice([]string{"ab", "cde", "f"}),
		0,
		func(acc int, s string) int { return acc + len(s) },
	)
	if got != 6 {
		t.Fatalf("want 6, got %d", got)
	}
}

func TestFold_Count_SliceFastPath(t *testing.T) {
	got := FromSlice([]int{1, 2, 3, 4, 5}).Count()
	if got != 5 {
		t.Fatalf("want 5, got %d", got)
	}
}

func TestFold_Count_Fallback(t *testing.T) {
	ch := make(chan int, 3)
	ch <- 1
	ch <- 2
	ch <- 3
	close(ch)
	got := FromChannel(ch).Count()
	if got != 3 {
		t.Fatalf("want 3, got %d", got)
	}
}

func TestFold_Count_WithHooks(t *testing.T) {
	var hookCount atomic.Int64
	got := FromSlice([]int{1, 2, 3}).
		WithElementHook(func(int) { hookCount.Add(1) }).
		Count()
	if got != 3 {
		t.Fatalf("want 3, got %d", got)
	}
	if hookCount.Load() != 3 {
		t.Fatalf("hookCount: want 3, got %d", hookCount.Load())
	}
}

func TestFold_SumBy(t *testing.T) {
	type item struct{ price float64 }
	items := []item{{10.5}, {20.0}, {4.5}}
	got := SumBy(FromSlice(items), func(i item) float64 { return i.price })
	if got != 35.0 {
		t.Fatalf("want 35.0, got %f", got)
	}
}

func TestFold_GroupBy(t *testing.T) {
	groups := GroupBy(FromSlice([]int{1, 2, 3, 4, 5, 6}), func(n int) string {
		if n%2 == 0 {
			return "even"
		}
		return "odd"
	})
	assertSliceEqual(t, []int{2, 4, 6}, groups["even"])
	assertSliceEqual(t, []int{1, 3, 5}, groups["odd"])
}

func TestFold_GroupBy_WithHooks(t *testing.T) {
	var hookCount atomic.Int64
	groups := GroupBy(
		FromSlice([]int{1, 2, 3}).WithElementHook(func(int) { hookCount.Add(1) }),
		func(n int) int { return n % 2 },
	)
	if len(groups) != 2 {
		t.Fatalf("want 2 groups, got %d", len(groups))
	}
	if hookCount.Load() != 3 {
		t.Fatalf("hookCount: want 3, got %d", hookCount.Load())
	}
}

func TestFold_CountBy(t *testing.T) {
	counts := CountBy(FromSlice([]string{"a", "b", "a", "c", "b", "a"}), func(s string) string { return s })
	if counts["a"] != 3 || counts["b"] != 2 || counts["c"] != 1 {
		t.Fatalf("unexpected: %v", counts)
	}
}

func TestFold_MaxBy(t *testing.T) {
	elem, ok := MaxBy(FromSlice([]int{3, 1, 4, 1, 5, 9, 2, 6}), func(n int) int { return n })
	if !ok || elem != 9 {
		t.Fatalf("want (9, true), got (%d, %v)", elem, ok)
	}
}

func TestFold_MaxBy_Empty(t *testing.T) {
	_, ok := MaxBy(FromSlice([]int{}), func(n int) int { return n })
	if ok {
		t.Fatal("want false for empty")
	}
}

func TestFold_MinBy(t *testing.T) {
	elem, ok := MinBy(FromSlice([]int{3, 1, 4, 1, 5}), func(n int) int { return n })
	if !ok || elem != 1 {
		t.Fatalf("want (1, true), got (%d, %v)", elem, ok)
	}
}

func TestFold_Partition(t *testing.T) {
	m, u := Partition(FromSlice([]int{1, 2, 3, 4, 5}), func(n int) bool { return n > 3 })
	assertSliceEqual(t, []int{4, 5}, m)
	assertSliceEqual(t, []int{1, 2, 3}, u)
}

func TestFold_Partition_WithHooks(t *testing.T) {
	var hookCount atomic.Int64
	m, _ := Partition(
		FromSlice([]int{1, 2, 3, 4}).WithElementHook(func(int) { hookCount.Add(1) }),
		func(n int) bool { return n%2 == 0 },
	)
	assertSliceEqual(t, []int{2, 4}, m)
	if hookCount.Load() != 4 {
		t.Fatalf("hookCount: want 4, got %d", hookCount.Load())
	}
}

func TestFold_Channel_Reduce_WithHooks(t *testing.T) {
	ch := make(chan int, 5)
	for _, v := range []int{10, 20, 30, 40, 50} {
		ch <- v
	}
	close(ch)
	var hookCount atomic.Int64
	got := FromChannel(ch).
		WithElementHook(func(int) { hookCount.Add(1) }).
		Reduce(0, func(a, b int) int { return a + b })
	if got != 150 {
		t.Fatalf("want 150, got %d", got)
	}
	if hookCount.Load() != 5 {
		t.Fatalf("hookCount: want 5, got %d", hookCount.Load())
	}
}

func TestFold_FuncSource(t *testing.T) {
	i := 0
	data := []int{1, 2, 3}
	got := FromFunc(func() (int, bool) {
		if i >= len(data) {
			return 0, false
		}
		v := data[i]
		i++
		return v, true
	}).Reduce(0, func(a, b int) int { return a + b })
	if got != 6 {
		t.Fatalf("want 6, got %d", got)
	}
}

func TestFold_Filter_SumBy(t *testing.T) {
	got := SumBy(
		FromSlice([]int{1, 2, 3, 4, 5}).Filter(func(n int) bool { return n > 2 }),
		func(n int) int { return n },
	)
	if got != 12 {
		t.Fatalf("want 12, got %d", got)
	}
}

// ---------------------------------------------------------------------------
// foldWhile
// ---------------------------------------------------------------------------

func TestFoldWhile_Any_Found(t *testing.T) {
	if !FromSlice([]int{1, 2, 3, 4, 5}).Any(func(n int) bool { return n == 3 }) {
		t.Fatal("want true")
	}
}

func TestFoldWhile_Any_NotFound(t *testing.T) {
	if FromSlice([]int{1, 2, 3}).Any(func(n int) bool { return n > 10 }) {
		t.Fatal("want false")
	}
}

func TestFoldWhile_Any_Empty(t *testing.T) {
	if FromSlice([]int{}).Any(func(int) bool { return true }) {
		t.Fatal("want false for empty")
	}
}

func TestFoldWhile_Any_ShortCircuit(t *testing.T) {
	visited := 0
	FromSlice([]int{1, 2, 3, 4, 5}).Any(func(n int) bool {
		visited++
		return n == 2
	})
	if visited != 2 {
		t.Fatalf("want 2 visits, got %d", visited)
	}
}

func TestFoldWhile_Any_WithHooks(t *testing.T) {
	var hookCount atomic.Int64
	got := FromSlice([]int{1, 2, 3, 4}).
		WithElementHook(func(int) { hookCount.Add(1) }).
		Any(func(n int) bool { return n == 2 })
	if !got {
		t.Fatal("want true")
	}
	if hookCount.Load() != 2 {
		t.Fatalf("hookCount: want 2, got %d", hookCount.Load())
	}
}

func TestFoldWhile_All_True(t *testing.T) {
	if !FromSlice([]int{2, 4, 6}).All(func(n int) bool { return n%2 == 0 }) {
		t.Fatal("want true")
	}
}

func TestFoldWhile_All_False(t *testing.T) {
	if FromSlice([]int{2, 4, 5, 6}).All(func(n int) bool { return n%2 == 0 }) {
		t.Fatal("want false")
	}
}

func TestFoldWhile_All_Empty(t *testing.T) {
	if !FromSlice([]int{}).All(func(int) bool { return false }) {
		t.Fatal("want true for empty (vacuous truth)")
	}
}

func TestFoldWhile_All_ShortCircuit(t *testing.T) {
	visited := 0
	FromSlice([]int{2, 4, 5, 6, 8}).All(func(n int) bool {
		visited++
		return n%2 == 0
	})
	if visited != 3 {
		t.Fatalf("want 3 visits, got %d", visited)
	}
}

func TestFoldWhile_Channel_Any(t *testing.T) {
	ch := make(chan int, 5)
	for _, v := range []int{1, 2, 3, 4, 5} {
		ch <- v
	}
	close(ch)
	if !FromChannel(ch).Any(func(n int) bool { return n == 3 }) {
		t.Fatal("want true")
	}
}

// ---------------------------------------------------------------------------
// completion hooks fire from every terminal
// ---------------------------------------------------------------------------

func TestCompletionHook_Reduce(t *testing.T) {
	fired := false
	FromSlice([]int{1}).WithCompletionHook(func() { fired = true }).Reduce(0, func(a, b int) int { return a + b })
	if !fired {
		t.Fatal("not fired")
	}
}

func TestCompletionHook_Any(t *testing.T) {
	fired := false
	FromSlice([]int{1}).WithCompletionHook(func() { fired = true }).Any(func(int) bool { return true })
	if !fired {
		t.Fatal("not fired")
	}
}

func TestCompletionHook_All(t *testing.T) {
	fired := false
	FromSlice([]int{1}).WithCompletionHook(func() { fired = true }).All(func(int) bool { return true })
	if !fired {
		t.Fatal("not fired")
	}
}

func TestCompletionHook_ForEach(t *testing.T) {
	fired := false
	FromSlice([]int{1}).WithCompletionHook(func() { fired = true }).ForEach(func(int) {})
	if !fired {
		t.Fatal("not fired")
	}
}

func TestCompletionHook_GroupBy(t *testing.T) {
	fired := false
	GroupBy(FromSlice([]int{1}).WithCompletionHook(func() { fired = true }), func(n int) int { return n })
	if !fired {
		t.Fatal("not fired")
	}
}

func TestCompletionHook_SumBy(t *testing.T) {
	fired := false
	SumBy(FromSlice([]int{1}).WithCompletionHook(func() { fired = true }), func(n int) int { return n })
	if !fired {
		t.Fatal("not fired")
	}
}

func TestCompletionHook_MaxBy(t *testing.T) {
	fired := false
	MaxBy(FromSlice([]int{1}).WithCompletionHook(func() { fired = true }), func(n int) int { return n })
	if !fired {
		t.Fatal("not fired")
	}
}

func TestCompletionHook_Partition(t *testing.T) {
	fired := false
	Partition(FromSlice([]int{1}).WithCompletionHook(func() { fired = true }), func(int) bool { return true })
	if !fired {
		t.Fatal("not fired")
	}
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

func makeIterData(n int) []int {
	s := make([]int, n)
	for i := range s {
		s[i] = i
	}
	return s
}

// --- drain ---

func BenchmarkDrain_ForEach_10k(b *testing.B) {
	data := makeIterData(10_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sum := 0
		FromSlice(data).ForEach(func(v int) { sum += v })
	}
}

func BenchmarkDrain_ForEach_WithHooks_10k(b *testing.B) {
	data := makeIterData(10_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sum := 0
		FromSlice(data).WithElementHook(func(int) {}).ForEach(func(v int) { sum += v })
	}
}

// --- fold ---

func BenchmarkFold_Reduce_10k(b *testing.B) {
	data := makeIterData(10_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		FromSlice(data).Reduce(0, func(a, b int) int { return a + b })
	}
}

func BenchmarkFold_SumBy_10k(b *testing.B) {
	data := makeIterData(10_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		SumBy(FromSlice(data), func(n int) int { return n })
	}
}

func BenchmarkFold_GroupBy_10k(b *testing.B) {
	data := makeIterData(10_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		GroupBy(FromSlice(data), func(n int) int { return n % 10 })
	}
}

func BenchmarkFold_MaxBy_10k(b *testing.B) {
	data := makeIterData(10_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MaxBy(FromSlice(data), func(n int) int { return n })
	}
}

func BenchmarkFold_Partition_10k(b *testing.B) {
	data := makeIterData(10_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Partition(FromSlice(data), func(n int) bool { return n%2 == 0 })
	}
}

func BenchmarkFold_PipeReduce_10k(b *testing.B) {
	data := makeIterData(10_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		PipeReduce(FromSlice(data), int64(0), func(acc int64, v int) int64 { return acc + int64(v) })
	}
}

func BenchmarkFold_CountBy_10k(b *testing.B) {
	data := makeIterData(10_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CountBy(FromSlice(data), func(n int) int { return n % 100 })
	}
}

// --- foldWhile ---

func BenchmarkFoldWhile_Any_EarlyHit_10k(b *testing.B) {
	data := makeIterData(10_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		FromSlice(data).Any(func(n int) bool { return n == 50 })
	}
}

func BenchmarkFoldWhile_Any_Miss_10k(b *testing.B) {
	data := makeIterData(10_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		FromSlice(data).Any(func(n int) bool { return n < 0 })
	}
}

func BenchmarkFoldWhile_All_Pass_10k(b *testing.B) {
	data := makeIterData(10_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		FromSlice(data).All(func(n int) bool { return n >= 0 })
	}
}

func BenchmarkFoldWhile_All_EarlyFail_10k(b *testing.B) {
	data := makeIterData(10_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		FromSlice(data).All(func(n int) bool { return n < 50 })
	}
}

// --- fast path vs fallback ---

func BenchmarkCount_SliceFastPath_10k(b *testing.B) {
	data := makeIterData(10_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		FromSlice(data).Count()
	}
}

func BenchmarkCount_FoldFallback_10k(b *testing.B) {
	data := makeIterData(10_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		FromSlice(data).WithElementHook(func(int) {}).Count()
	}
}

func BenchmarkCollect_DirectCollectable_10k(b *testing.B) {
	data := makeIterData(10_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		FromSlice(data).Collect()
	}
}

func BenchmarkCollect_DrainFallback_10k(b *testing.B) {
	data := makeIterData(10_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		FromSlice(data).WithElementHook(func(int) {}).Collect()
	}
}
