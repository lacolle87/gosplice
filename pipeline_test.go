package gosplice

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestPipelineFilter(t *testing.T) {
	assertSliceEqual(t, []int{2, 4, 6},
		FromSlice([]int{1, 2, 3, 4, 5, 6}).Filter(func(n int) bool { return n%2 == 0 }).Collect())
}

func TestPipelineFilterEmpty(t *testing.T) {
	r := FromSlice([]int{}).Filter(func(n int) bool { return n > 0 }).Collect()
	if len(r) != 0 {
		t.Errorf("expected empty, got %v", r)
	}
}

func TestPipelineTake(t *testing.T) {
	assertSliceEqual(t, []int{1, 2, 3}, FromSlice([]int{1, 2, 3, 4, 5}).Take(3).Collect())
}

func TestPipelineTakeMoreThanAvailable(t *testing.T) {
	assertSliceEqual(t, []int{1, 2}, FromSlice([]int{1, 2}).Take(10).Collect())
}

func TestPipelineSkip(t *testing.T) {
	assertSliceEqual(t, []int{3, 4, 5}, FromSlice([]int{1, 2, 3, 4, 5}).Skip(2).Collect())
}

func TestPipelineSkipAll(t *testing.T) {
	r := FromSlice([]int{1, 2, 3}).Skip(10).Collect()
	if len(r) != 0 {
		t.Errorf("expected empty, got %v", r)
	}
}

func TestPipelineReduce(t *testing.T) {
	sum := FromSlice([]int{1, 2, 3, 4, 5}).Reduce(0, func(a, b int) int { return a + b })
	if sum != 15 {
		t.Errorf("expected 15, got %d", sum)
	}
}

func TestPipeReduceCrossType(t *testing.T) {
	type Order struct {
		Item  string
		Price float64
	}
	total := PipeReduce(FromSlice([]Order{{"a", 1.5}, {"b", 2.0}, {"c", 3.5}}), 0.0,
		func(sum float64, o Order) float64 { return sum + o.Price })
	if total != 7.0 {
		t.Errorf("expected 7.0, got %f", total)
	}
}

func TestPipeReduceToString(t *testing.T) {
	r := PipeReduce(FromSlice([]int{1, 2, 3}), "",
		func(acc string, n int) string {
			if acc != "" {
				acc += ","
			}
			return acc + fmt.Sprintf("%d", n)
		})
	if r != "1,2,3" {
		t.Errorf("expected '1,2,3', got %q", r)
	}
}

func TestPipelineCount(t *testing.T) {
	n := FromSlice([]int{1, 2, 3, 4, 5}).Filter(func(n int) bool { return n > 3 }).Count()
	if n != 2 {
		t.Errorf("expected 2, got %d", n)
	}
}

func TestPipelineFirst(t *testing.T) {
	v, ok := FromSlice([]int{10, 20, 30}).Filter(func(n int) bool { return n > 15 }).First()
	if !ok || v != 20 {
		t.Errorf("expected (20, true), got (%d, %v)", v, ok)
	}
}

func TestPipelineFirstEmpty(t *testing.T) {
	_, ok := FromSlice([]int{}).First()
	if ok {
		t.Error("expected false")
	}
}

func TestPipelineAny(t *testing.T) {
	if !FromSlice([]int{1, 2, 3}).Any(func(n int) bool { return n == 2 }) {
		t.Error("expected true")
	}
	if FromSlice([]int{1, 2, 3}).Any(func(n int) bool { return n == 9 }) {
		t.Error("expected false")
	}
}

func TestPipelineAll(t *testing.T) {
	if !FromSlice([]int{2, 4, 6}).All(func(n int) bool { return n%2 == 0 }) {
		t.Error("expected true")
	}
	if FromSlice([]int{2, 3, 6}).All(func(n int) bool { return n%2 == 0 }) {
		t.Error("expected false")
	}
}

func TestPipelineChaining(t *testing.T) {
	assertSliceEqual(t, []int{4, 6},
		FromSlice([]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}).
			Filter(func(n int) bool { return n%2 == 0 }).
			Skip(1).
			Take(2).
			Collect())
}

func TestPipeMap(t *testing.T) {
	assertSliceEqual(t, []string{"num_1", "num_2", "num_3"},
		PipeMap(FromSlice([]int{1, 2, 3}), func(n int) string { return fmt.Sprintf("num_%d", n) }).Collect())
}

func TestPipeMapTypeChange(t *testing.T) {
	assertSliceEqual(t, []int{5, 5, 2},
		PipeMap(FromSlice([]string{"hello", "world", "go"}), func(s string) int { return len(s) }).Collect())
}

func TestPipeMapErr(t *testing.T) {
	errCount := int32(0)
	p := FromSlice([]int{1, 2, 0, 4}).
		WithErrorHook(func(err error, v int) { atomic.AddInt32(&errCount, 1) })

	result := PipeMapErr(p, func(n int) (float64, error) {
		if n == 0 {
			return 0, errors.New("zero")
		}
		return 10.0 / float64(n), nil
	}).Collect()

	if len(result) != 3 {
		t.Errorf("expected 3 results, got %d", len(result))
	}
	if atomic.LoadInt32(&errCount) != 1 {
		t.Errorf("expected 1 error, got %d", errCount)
	}
}

func TestPipeFlatMap(t *testing.T) {
	assertSliceEqual(t, []int{1, 10, 2, 20, 3, 30},
		PipeFlatMap(FromSlice([]int{1, 2, 3}), func(n int) []int { return []int{n, n * 10} }).Collect())
}

func TestPipeDistinct(t *testing.T) {
	assertSliceEqual(t, []int{1, 2, 3, 4},
		PipeDistinct(FromSlice([]int{1, 2, 2, 3, 1, 4, 3})).Collect())
}

func TestPipeDistinctEmpty(t *testing.T) {
	r := PipeDistinct(FromSlice([]int{})).Collect()
	if len(r) != 0 {
		t.Errorf("expected empty, got %v", r)
	}
}

func TestPipeChunk(t *testing.T) {
	chunks := PipeChunk(FromSlice([]int{1, 2, 3, 4, 5}), 2).Collect()
	if len(chunks) != 3 {
		t.Fatalf("expected 3 chunks, got %d", len(chunks))
	}
	assertSliceEqual(t, []int{1, 2}, chunks[0])
	assertSliceEqual(t, []int{3, 4}, chunks[1])
	assertSliceEqual(t, []int{5}, chunks[2])
}

func TestPipeChunkEmpty(t *testing.T) {
	r := PipeChunk(FromSlice([]int{}), 3).Collect()
	if len(r) != 0 {
		t.Errorf("expected empty, got %v", r)
	}
}

func TestPipeWindow(t *testing.T) {
	w := PipeWindow(FromSlice([]int{1, 2, 3, 4, 5}), 3, 1).Collect()
	if len(w) != 3 {
		t.Fatalf("expected 3 windows, got %d", len(w))
	}
	assertSliceEqual(t, []int{1, 2, 3}, w[0])
	assertSliceEqual(t, []int{2, 3, 4}, w[1])
	assertSliceEqual(t, []int{3, 4, 5}, w[2])
}

func TestPipeWindowStep2(t *testing.T) {
	w := PipeWindow(FromSlice([]int{1, 2, 3, 4, 5, 6}), 3, 2).Collect()
	if len(w) != 2 {
		t.Fatalf("expected 2 windows, got %d", len(w))
	}
	assertSliceEqual(t, []int{1, 2, 3}, w[0])
	assertSliceEqual(t, []int{3, 4, 5}, w[1])
}

func TestPipeWindowEmpty(t *testing.T) {
	r := PipeWindow(FromSlice([]int{}), 3, 1).Collect()
	if len(r) != 0 {
		t.Errorf("expected empty, got %v", r)
	}
}

func TestPipeWindowSmallerThanSize(t *testing.T) {
	w := PipeWindow(FromSlice([]int{1, 2}), 5, 1).Collect()
	if len(w) != 1 {
		t.Fatalf("expected 1 partial window, got %d", len(w))
	}
	assertSliceEqual(t, []int{1, 2}, w[0])
}

func TestGroupBy(t *testing.T) {
	g := GroupBy(FromSlice([]int{1, 2, 3, 4, 5, 6}), func(n int) string {
		if n%2 == 0 {
			return "even"
		}
		return "odd"
	})
	assertSliceEqual(t, []int{2, 4, 6}, g["even"])
	assertSliceEqual(t, []int{1, 3, 5}, g["odd"])
}

func TestGroupByEmpty(t *testing.T) {
	g := GroupBy(FromSlice([]int{}), func(n int) int { return n })
	if len(g) != 0 {
		t.Errorf("expected empty, got %v", g)
	}
}

func TestCountBy(t *testing.T) {
	c := CountBy(FromSlice([]string{"apple", "avocado", "banana", "blueberry", "cherry"}),
		func(s string) byte { return s[0] })
	if c['a'] != 2 || c['b'] != 2 || c['c'] != 1 {
		t.Errorf("unexpected: %v", c)
	}
}

func TestSumBy(t *testing.T) {
	type item struct {
		price float64
	}
	s := SumBy(FromSlice([]item{{10.5}, {20.0}, {5.5}}), func(i item) float64 { return i.price })
	if s != 36.0 {
		t.Errorf("expected 36.0, got %f", s)
	}
}

func TestMaxBy(t *testing.T) {
	type item struct {
		name  string
		score int
	}
	m, ok := MaxBy(FromSlice([]item{{"a", 10}, {"b", 50}, {"c", 30}}), func(i item) int { return i.score })
	if !ok || m.name != "b" {
		t.Errorf("expected b, got %v", m)
	}
}

func TestMaxByEmpty(t *testing.T) {
	_, ok := MaxBy(FromSlice([]int{}), func(n int) int { return n })
	if ok {
		t.Error("expected false")
	}
}

func TestMinBy(t *testing.T) {
	m, ok := MinBy(FromSlice([]int{5, 1, 3, 2}), func(n int) int { return n })
	if !ok || m != 1 {
		t.Errorf("expected 1, got %d", m)
	}
}

func TestMinByEmpty(t *testing.T) {
	_, ok := MinBy(FromSlice([]int{}), func(n int) int { return n })
	if ok {
		t.Error("expected false")
	}
}

func TestPartition(t *testing.T) {
	m, u := Partition(FromSlice([]int{1, 2, 3, 4, 5}), func(n int) bool { return n%2 == 0 })
	assertSliceEqual(t, []int{2, 4}, m)
	assertSliceEqual(t, []int{1, 3, 5}, u)
}

func TestPipeMapParallel(t *testing.T) {
	assertSliceEqual(t, []int{1, 4, 9, 16, 25},
		PipeMapParallel(FromSlice([]int{1, 2, 3, 4, 5}), 4, func(n int) int { return n * n }).Collect())
}

func TestPipeMapParallelEmpty(t *testing.T) {
	r := PipeMapParallel(FromSlice([]int{}), 4, func(n int) int { return n }).Collect()
	if len(r) != 0 {
		t.Errorf("expected empty, got %v", r)
	}
}

func TestPipeFilterParallel(t *testing.T) {
	assertSliceEqual(t, []int{3, 6, 9},
		PipeFilterParallel(FromSlice([]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}), 4,
			func(n int) bool { return n%3 == 0 }).Collect())
}

func TestPipeFilterParallelEmpty(t *testing.T) {
	r := PipeFilterParallel(FromSlice([]int{}), 4, func(n int) bool { return true }).Collect()
	if len(r) != 0 {
		t.Errorf("expected empty, got %v", r)
	}
}

func TestPipeMapParallelErr(t *testing.T) {
	errCount := int32(0)
	p := FromSlice([]int{1, 0, 3, 0, 5}).
		WithErrorHook(func(err error, v int) { atomic.AddInt32(&errCount, 1) })

	result := PipeMapParallelErr(p, 2, func(n int) (int, error) {
		if n == 0 {
			return 0, errors.New("zero")
		}
		return n * 10, nil
	}).Collect()

	assertSliceEqual(t, []int{10, 30, 50}, result)
	if atomic.LoadInt32(&errCount) != 2 {
		t.Errorf("expected 2 errors, got %d", errCount)
	}
}

func TestPipeMapParallelStreamOrdered(t *testing.T) {
	assertSliceEqual(t, []int{10, 20, 30, 40, 50, 60, 70, 80, 90, 100},
		PipeMapParallelStream(FromSlice([]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}), 4, 16,
			func(n int) int { return n * 10 }).Collect())
}

func TestPipeBatch(t *testing.T) {
	b := PipeBatch(FromSlice([]int{1, 2, 3, 4, 5}), BatchConfig{Size: 2}).Collect()
	if len(b) != 3 {
		t.Fatalf("expected 3 batches, got %d", len(b))
	}
	assertSliceEqual(t, []int{1, 2}, b[0])
	assertSliceEqual(t, []int{3, 4}, b[1])
	assertSliceEqual(t, []int{5}, b[2])
}

func TestBatchHookFired(t *testing.T) {
	count := 0
	p := FromSlice([]int{1, 2, 3, 4, 5}).
		WithBatchHook(func(batch []int) { count++ })
	_ = PipeBatch(p, BatchConfig{Size: 2}).Collect()
	if count != 3 {
		t.Errorf("expected 3, got %d", count)
	}
}

func TestPipeBatchWithTimeout(t *testing.T) {
	ch := make(chan int, 10)
	for i := 1; i <= 5; i++ {
		ch <- i
	}
	close(ch)
	batches := PipeBatch(FromChannel(ch), BatchConfig{Size: 3, MaxWait: 100 * time.Millisecond}).Collect()
	total := 0
	for _, b := range batches {
		total += len(b)
	}
	if total != 5 {
		t.Errorf("expected 5 total, got %d", total)
	}
}

func TestFromChannel(t *testing.T) {
	ch := make(chan int, 5)
	for i := 1; i <= 5; i++ {
		ch <- i
	}
	close(ch)
	assertSliceEqual(t, []int{3, 4, 5},
		FromChannel(ch).Filter(func(n int) bool { return n > 2 }).Collect())
}

func TestFromReader(t *testing.T) {
	assertSliceEqual(t, []string{"hello", "world", "go"},
		FromReader(strings.NewReader("hello\nworld\ngo")).Collect())
}

func TestFromRange(t *testing.T) {
	assertSliceEqual(t, []int{0, 1, 2, 3, 4}, FromRange(0, 5).Collect())
	assertSliceEqual(t, []int{}, FromRange(5, 5).Collect())
}

func TestFromFunc(t *testing.T) {
	i := 0
	r := FromFunc(func() (int, bool) {
		if i >= 3 {
			return 0, false
		}
		v := i * i
		i++
		return v, true
	}).Collect()
	assertSliceEqual(t, []int{0, 1, 4}, r)
}

func TestPeek(t *testing.T) {
	var peeked []int
	r := FromSlice([]int{1, 2, 3}).Peek(func(n int) { peeked = append(peeked, n) }).Collect()
	assertSliceEqual(t, []int{1, 2, 3}, r)
	assertSliceEqual(t, []int{1, 2, 3}, peeked)
}

func TestCollectToNil(t *testing.T) {
	assertSliceEqual(t, []int{1, 2, 3}, FromSlice([]int{1, 2, 3}).CollectTo(nil))
}

func TestCollectToPreallocated(t *testing.T) {
	buf := make([]int, 0, 100)
	assertSliceEqual(t, []int{1, 2, 3}, FromSlice([]int{1, 2, 3}).CollectTo(buf))
}

func TestElementHookFired(t *testing.T) {
	count := int32(0)
	FromSlice([]int{1, 2, 3}).
		WithElementHook(func(v int) { atomic.AddInt32(&count, 1) }).
		Collect()
	if atomic.LoadInt32(&count) != 3 {
		t.Errorf("expected 3, got %d", count)
	}
}

func TestCompletionHookFired(t *testing.T) {
	completed := false
	FromSlice([]int{1, 2, 3}).WithCompletionHook(func() { completed = true }).Collect()
	if !completed {
		t.Error("not fired")
	}
}

func TestCompletionHookOnReduce(t *testing.T) {
	completed := false
	FromSlice([]int{1, 2}).WithCompletionHook(func() { completed = true }).Reduce(0, func(a, b int) int { return a + b })
	if !completed {
		t.Error("not fired")
	}
}

func TestCompletionHookOnForEach(t *testing.T) {
	completed := false
	FromSlice([]int{1}).WithCompletionHook(func() { completed = true }).ForEach(func(n int) {})
	if !completed {
		t.Error("not fired")
	}
}

func TestCompletionHookOnCount(t *testing.T) {
	completed := false
	FromSlice([]int{1}).WithCompletionHook(func() { completed = true }).Count()
	if !completed {
		t.Error("not fired")
	}
}

func TestMultipleHooksCompose(t *testing.T) {
	var order []string
	FromSlice([]int{1}).
		WithElementHook(func(v int) { order = append(order, "first") }).
		WithElementHook(func(v int) { order = append(order, "second") }).
		Collect()
	assertSliceEqual(t, []string{"first", "second"}, order)
}

func TestLazyEvaluation(t *testing.T) {
	evaluated := int32(0)
	FromSlice([]int{1, 2, 3, 4, 5}).
		Filter(func(n int) bool {
			atomic.AddInt32(&evaluated, 1)
			return n > 3
		}).
		Take(1).
		Collect()
	if atomic.LoadInt32(&evaluated) != 4 {
		t.Errorf("expected 4 lazy evaluations, got %d", evaluated)
	}
}

func TestEndToEndETL(t *testing.T) {
	type Record struct {
		Name   string
		Score  int
		Active bool
	}

	records := []Record{
		{"Alice", 85, true}, {"Bob", 42, false}, {"Charlie", 91, true},
		{"Diana", 67, true}, {"Eve", 23, false}, {"Frank", 78, true},
	}

	names := PipeMap(
		FromSlice(records).
			Filter(func(r Record) bool { return r.Active }).
			Filter(func(r Record) bool { return r.Score >= 70 }),
		func(r Record) string { return strings.ToUpper(r.Name) },
	).Collect()

	assertSliceEqual(t, []string{"ALICE", "CHARLIE", "FRANK"}, names)
}

func TestPipelineForEachCount(t *testing.T) {
	count := 0
	FromSlice([]int{1, 2, 3, 4, 5}).
		Filter(func(n int) bool { return n%2 == 0 }).
		ForEach(func(n int) { count++ })
	if count != 2 {
		t.Errorf("expected 2, got %d", count)
	}
}

func TestCollectTo_Nil(t *testing.T) {
	result := FromSlice([]int{1, 2, 3}).CollectTo(nil)
	assertSliceEqual(t, []int{1, 2, 3}, result)
}

func TestCollectTo_Reuse(t *testing.T) {
	buf := make([]int, 0, 100)
	result := FromSlice([]int{10, 20, 30}).CollectTo(buf)
	assertSliceEqual(t, []int{10, 20, 30}, result)
	if cap(result) != 100 {
		t.Fatalf("expected reused capacity 100, got %d", cap(result))
	}
}

func TestCollectTo_WithFilter(t *testing.T) {
	buf := make([]int, 5, 10) // pre-filled, should be reset to [:0]
	result := FromSlice([]int{1, 2, 3}).Filter(func(n int) bool { return n > 1 }).CollectTo(buf)
	assertSliceEqual(t, []int{2, 3}, result)
}

func TestFirst_WithHook(t *testing.T) {
	var count atomic.Int64
	v, ok := FromSlice([]int{10, 20, 30}).
		WithElementHook(CountElements[int](&count)).
		First()
	if !ok || v != 10 {
		t.Fatalf("expected 10, got %d ok=%v", v, ok)
	}
	if count.Load() != 1 {
		t.Fatalf("expected 1 hook call, got %d", count.Load())
	}
}

func TestFirst_Empty(t *testing.T) {
	_, ok := FromSlice([]int{}).First()
	if ok {
		t.Fatal("expected ok=false")
	}
}

func TestAll_AllMatch(t *testing.T) {
	if !FromSlice([]int{2, 4, 6}).All(func(n int) bool { return n%2 == 0 }) {
		t.Fatal("expected true")
	}
}

func TestAll_NoneMatch(t *testing.T) {
	if FromSlice([]int{1, 3, 5}).All(func(n int) bool { return n%2 == 0 }) {
		t.Fatal("expected false")
	}
}

func TestAny_NoneMatch(t *testing.T) {
	if FromSlice([]int{1, 3, 5}).Any(func(n int) bool { return n%2 == 0 }) {
		t.Fatal("expected false")
	}
}

func TestAny_Empty(t *testing.T) {
	if FromSlice([]int{}).Any(func(n int) bool { return true }) {
		t.Fatal("expected false for empty")
	}
}

func TestAll_Empty(t *testing.T) {
	if !FromSlice([]int{}).All(func(n int) bool { return false }) {
		t.Fatal("expected true for empty (vacuous truth)")
	}
}

func TestCount_NonSliceSource(t *testing.T) {
	ch := make(chan int, 3)
	ch <- 1
	ch <- 2
	ch <- 3
	close(ch)
	n := FromChannel(ch).Count()
	if n != 3 {
		t.Fatalf("expected 3, got %d", n)
	}
}

func TestCount_WithFilter(t *testing.T) {
	n := FromSlice([]int{1, 2, 3, 4, 5}).Filter(func(n int) bool { return n > 3 }).Count()
	if n != 2 {
		t.Fatalf("expected 2, got %d", n)
	}
}

func TestCount_WithCtx(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	n := FromSlice([]int{1, 2, 3}).WithContext(ctx).Count()
	if n != 3 {
		t.Fatalf("expected 3, got %d", n)
	}
}

func TestFilterCollectAll_NonSlice(t *testing.T) {
	// Filter on channel source — collectAll returns nil, falls back to drain
	ch := make(chan int, 3)
	ch <- 1
	ch <- 2
	ch <- 3
	close(ch)
	result := FromChannel(ch).Filter(func(n int) bool { return n > 1 }).Collect()
	assertSliceEqual(t, []int{2, 3}, result)
}

func TestMapCollectAll_NonSlice(t *testing.T) {
	ch := make(chan int, 3)
	ch <- 10
	ch <- 20
	close(ch)
	result := PipeMap(FromChannel(ch), func(n int) int { return n / 10 }).Collect()
	assertSliceEqual(t, []int{1, 2}, result)
}

func TestChunkCollectAll_NonSlice(t *testing.T) {
	ch := make(chan int, 4)
	ch <- 1
	ch <- 2
	ch <- 3
	ch <- 4
	close(ch)
	result := PipeChunk(FromChannel(ch), 2).Collect()
	if len(result) != 2 {
		t.Fatalf("expected 2 chunks, got %d", len(result))
	}
}

func TestWindowCollectAll_NonSlice(t *testing.T) {
	ch := make(chan int, 5)
	for i := 1; i <= 5; i++ {
		ch <- i
	}
	close(ch)
	result := PipeWindow(FromChannel(ch), 3, 1).Collect()
	if len(result) != 3 {
		t.Fatalf("expected 3 windows, got %d", len(result))
	}
}

func TestWindowCollectAll_Slice_SmallData(t *testing.T) {
	// n < size — exercises the fast-path partial branch
	result := PipeWindow(FromSlice([]int{1}), 5, 1).Collect()
	if len(result) != 1 {
		t.Fatalf("expected 1 partial window, got %d", len(result))
	}
	assertSliceEqual(t, []int{1}, result[0])
}

func TestChunkCollectAll_Slice_Empty(t *testing.T) {
	result := PipeChunk(FromSlice([]int{}), 3).Collect()
	if len(result) != 0 {
		t.Fatal("expected empty")
	}
}

func TestSetErr_Nil(t *testing.T) {
	p := FromSlice([]int{1})
	p.setErr(nil)
	if p.Err() != nil {
		t.Fatal("setErr(nil) should be no-op")
	}
}

func TestSetErr_OnlyFirst(t *testing.T) {
	p := FromSlice([]int{1})
	p.setErr(errors.New("first"))
	p.setErr(errors.New("second"))
	if p.Err().Error() != "first" {
		t.Fatalf("expected 'first', got %v", p.Err())
	}
}

func TestReduce_CtxActive(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	total := FromSlice([]int{1, 2, 3}).WithContext(ctx).Reduce(0, func(a, b int) int { return a + b })
	if total != 6 {
		t.Fatalf("expected 6, got %d", total)
	}
}

func TestReduce_WithHooks(t *testing.T) {
	var count atomic.Int64
	total := FromSlice([]int{1, 2, 3}).
		WithElementHook(CountElements[int](&count)).
		Reduce(0, func(a, b int) int { return a + b })
	if total != 6 {
		t.Fatalf("expected 6, got %d", total)
	}
	if count.Load() != 3 {
		t.Fatalf("expected 3, got %d", count.Load())
	}
}

func TestReduce_NonSlice(t *testing.T) {
	ch := make(chan int, 3)
	ch <- 1
	ch <- 2
	ch <- 3
	close(ch)
	total := FromChannel(ch).Reduce(0, func(a, b int) int { return a + b })
	if total != 6 {
		t.Fatalf("expected 6, got %d", total)
	}
}

func TestMapCollectAll_WithHooks(t *testing.T) {
	var count atomic.Int64
	result := PipeMap(
		FromSlice([]int{1, 2, 3}).WithElementHook(CountElements[int](&count)),
		func(n int) int { return n * 2 },
	).Collect()
	assertSliceEqual(t, []int{2, 4, 6}, result)
	if count.Load() != 3 {
		t.Fatalf("expected 3, got %d", count.Load())
	}
}
