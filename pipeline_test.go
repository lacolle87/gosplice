package gosplice

import (
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestPipelineFilter(t *testing.T) {
	result := FromSlice([]int{1, 2, 3, 4, 5, 6}).
		Filter(func(n int) bool { return n%2 == 0 }).
		Collect()

	assertSliceEqual(t, []int{2, 4, 6}, result)
}

func TestPipelineFilterEmpty(t *testing.T) {
	result := FromSlice([]int{}).
		Filter(func(n int) bool { return n > 0 }).
		Collect()

	if len(result) != 0 {
		t.Errorf("expected empty slice, got %v", result)
	}
}

func TestPipelineTake(t *testing.T) {
	result := FromSlice([]int{1, 2, 3, 4, 5}).
		Take(3).
		Collect()

	assertSliceEqual(t, []int{1, 2, 3}, result)
}

func TestPipelineTakeMoreThanAvailable(t *testing.T) {
	result := FromSlice([]int{1, 2}).
		Take(10).
		Collect()

	assertSliceEqual(t, []int{1, 2}, result)
}

func TestPipelineSkip(t *testing.T) {
	result := FromSlice([]int{1, 2, 3, 4, 5}).
		Skip(2).
		Collect()

	assertSliceEqual(t, []int{3, 4, 5}, result)
}

func TestPipelineSkipAll(t *testing.T) {
	result := FromSlice([]int{1, 2, 3}).
		Skip(10).
		Collect()

	if len(result) != 0 {
		t.Errorf("expected empty slice, got %v", result)
	}
}

func TestPipelineReduce(t *testing.T) {
	sum := FromSlice([]int{1, 2, 3, 4, 5}).
		Reduce(0, func(a, b int) int { return a + b })

	if sum != 15 {
		t.Errorf("expected 15, got %d", sum)
	}
}

func TestReduceCrossType(t *testing.T) {
	words := []string{"hello", "world", "go"}
	totalLen := Reduce(words, 0, func(acc int, s string) int {
		return acc + len(s)
	})
	if totalLen != 12 {
		t.Errorf("expected 12, got %d", totalLen)
	}
}

func TestReduceToMap(t *testing.T) {
	type kv struct {
		key string
		val int
	}
	items := []kv{{"a", 1}, {"b", 2}, {"c", 3}}
	m := Reduce(items, make(map[string]int), func(acc map[string]int, item kv) map[string]int {
		acc[item.key] = item.val
		return acc
	})
	if m["a"] != 1 || m["b"] != 2 || m["c"] != 3 {
		t.Errorf("unexpected map: %v", m)
	}
}

func TestPipeReduceCrossType(t *testing.T) {
	type Order struct {
		Item  string
		Price float64
	}
	orders := []Order{{"apple", 1.5}, {"bread", 2.0}, {"cheese", 3.5}}
	total := PipeReduce(FromSlice(orders), 0.0, func(sum float64, o Order) float64 {
		return sum + o.Price
	})
	if total != 7.0 {
		t.Errorf("expected 7.0, got %f", total)
	}
}

func TestPipeReduceToString(t *testing.T) {
	result := PipeReduce(
		FromSlice([]int{1, 2, 3}),
		"",
		func(acc string, n int) string {
			if acc != "" {
				acc += ","
			}
			return acc + fmt.Sprintf("%d", n)
		},
	)
	if result != "1,2,3" {
		t.Errorf("expected '1,2,3', got '%s'", result)
	}
}

func TestPipelineCount(t *testing.T) {
	count := FromSlice([]int{1, 2, 3, 4, 5}).
		Filter(func(n int) bool { return n > 3 }).
		Count()

	if count != 2 {
		t.Errorf("expected 2, got %d", count)
	}
}

func TestPipelineFirst(t *testing.T) {
	v, ok := FromSlice([]int{10, 20, 30}).
		Filter(func(n int) bool { return n > 15 }).
		First()

	if !ok || v != 20 {
		t.Errorf("expected (20, true), got (%d, %v)", v, ok)
	}
}

func TestPipelineFirstEmpty(t *testing.T) {
	_, ok := FromSlice([]int{}).First()
	if ok {
		t.Error("expected false for empty pipeline")
	}
}

func TestPipelineAny(t *testing.T) {
	has := FromSlice([]int{1, 2, 3}).
		Any(func(n int) bool { return n == 2 })

	if !has {
		t.Error("expected true")
	}
}

func TestPipelineAll(t *testing.T) {
	all := FromSlice([]int{2, 4, 6}).
		All(func(n int) bool { return n%2 == 0 })

	if !all {
		t.Error("expected true")
	}

	notAll := FromSlice([]int{2, 3, 6}).
		All(func(n int) bool { return n%2 == 0 })

	if notAll {
		t.Error("expected false")
	}
}

func TestPipelineChaining(t *testing.T) {
	result := FromSlice([]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}).
		Filter(func(n int) bool { return n%2 == 0 }).
		Skip(1).
		Take(2).
		Collect()

	assertSliceEqual(t, []int{4, 6}, result)
}

func TestPipeMap(t *testing.T) {
	p := FromSlice([]int{1, 2, 3})
	result := PipeMap(p, func(n int) string {
		return fmt.Sprintf("num_%d", n)
	}).Collect()

	assertSliceEqual(t, []string{"num_1", "num_2", "num_3"}, result)
}

func TestPipeMapTypeChange(t *testing.T) {
	p := FromSlice([]string{"hello", "world", "go"})
	result := PipeMap(p, func(s string) int {
		return len(s)
	}).Collect()

	assertSliceEqual(t, []int{5, 5, 2}, result)
}

func TestPipeMapErr(t *testing.T) {
	errCount := int32(0)
	p := FromSlice([]int{1, 2, 0, 4}).
		WithErrorHook(func(err error, v int) {
			atomic.AddInt32(&errCount, 1)
		})

	result := PipeMapErr(p, func(n int) (float64, error) {
		if n == 0 {
			return 0, errors.New("division by zero")
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
	p := FromSlice([]int{1, 2, 3})
	result := PipeFlatMap(p, func(n int) []int {
		return []int{n, n * 10}
	}).Collect()

	assertSliceEqual(t, []int{1, 10, 2, 20, 3, 30}, result)
}

func TestPipeDistinct(t *testing.T) {
	p := FromSlice([]int{1, 2, 2, 3, 1, 4, 3})
	result := PipeDistinct(p).Collect()

	assertSliceEqual(t, []int{1, 2, 3, 4}, result)
}

func TestPipeChunk(t *testing.T) {
	p := FromSlice([]int{1, 2, 3, 4, 5})
	chunks := PipeChunk(p, 2).Collect()

	if len(chunks) != 3 {
		t.Fatalf("expected 3 chunks, got %d", len(chunks))
	}
	assertSliceEqual(t, []int{1, 2}, chunks[0])
	assertSliceEqual(t, []int{3, 4}, chunks[1])
	assertSliceEqual(t, []int{5}, chunks[2])
}

func TestPipeWindow(t *testing.T) {
	p := FromSlice([]int{1, 2, 3, 4, 5})
	windows := PipeWindow(p, 3, 1).Collect()

	if len(windows) != 3 {
		t.Fatalf("expected 3 windows, got %d", len(windows))
	}
	assertSliceEqual(t, []int{1, 2, 3}, windows[0])
	assertSliceEqual(t, []int{2, 3, 4}, windows[1])
	assertSliceEqual(t, []int{3, 4, 5}, windows[2])
}

func TestGroupBy(t *testing.T) {
	p := FromSlice([]int{1, 2, 3, 4, 5, 6})
	groups := GroupBy(p, func(n int) string {
		if n%2 == 0 {
			return "even"
		}
		return "odd"
	})

	if len(groups) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(groups))
	}
	assertSliceEqual(t, []int{2, 4, 6}, groups["even"])
	assertSliceEqual(t, []int{1, 3, 5}, groups["odd"])
}

func TestCountBy(t *testing.T) {
	p := FromSlice([]string{"apple", "avocado", "banana", "blueberry", "cherry"})
	counts := CountBy(p, func(s string) byte {
		return s[0]
	})

	if counts['a'] != 2 {
		t.Errorf("expected 2 for 'a', got %d", counts['a'])
	}
	if counts['b'] != 2 {
		t.Errorf("expected 2 for 'b', got %d", counts['b'])
	}
	if counts['c'] != 1 {
		t.Errorf("expected 1 for 'c', got %d", counts['c'])
	}
}

func TestSumBy(t *testing.T) {
	type item struct {
		name  string
		price float64
	}
	items := []item{
		{"a", 10.5},
		{"b", 20.0},
		{"c", 5.5},
	}
	sum := SumBy(FromSlice(items), func(i item) float64 {
		return i.price
	})

	if sum != 36.0 {
		t.Errorf("expected 36.0, got %f", sum)
	}
}

func TestMaxBy(t *testing.T) {
	type item struct {
		name  string
		score int
	}
	items := []item{{"a", 10}, {"b", 50}, {"c", 30}}
	maximum, ok := MaxBy(FromSlice(items), func(i item) int { return i.score })

	if !ok || maximum.name != "b" {
		t.Errorf("expected b with score 50, got %v", maximum)
	}
}

func TestMinBy(t *testing.T) {
	minimum, ok := MinBy(FromSlice([]int{5, 1, 3, 2}), func(n int) int { return n })
	if !ok || minimum != 1 {
		t.Errorf("expected 1, got %d", minimum)
	}
}

func TestPartition(t *testing.T) {
	matched, unmatched := Partition(
		FromSlice([]int{1, 2, 3, 4, 5}),
		func(n int) bool { return n%2 == 0 },
	)

	assertSliceEqual(t, []int{2, 4}, matched)
	assertSliceEqual(t, []int{1, 3, 5}, unmatched)
}

func TestPipeMapParallel(t *testing.T) {
	p := FromSlice([]int{1, 2, 3, 4, 5})
	result := PipeMapParallel(p, 4, func(n int) int {
		return n * n
	}).Collect()

	assertSliceEqual(t, []int{1, 4, 9, 16, 25}, result)
}

func TestPipeFilterParallel(t *testing.T) {
	p := FromSlice([]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
	result := PipeFilterParallel(p, 4, func(n int) bool {
		return n%3 == 0
	}).Collect()

	assertSliceEqual(t, []int{3, 6, 9}, result)
}

func TestPipeMapParallelErr(t *testing.T) {
	errCount := int32(0)
	p := FromSlice([]int{1, 0, 3, 0, 5}).
		WithErrorHook(func(err error, v int) {
			atomic.AddInt32(&errCount, 1)
		})

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

func TestPipeBatch(t *testing.T) {
	p := FromSlice([]int{1, 2, 3, 4, 5})
	batches := PipeBatch(p, BatchConfig{Size: 2}).Collect()

	if len(batches) != 3 {
		t.Fatalf("expected 3 batches, got %d", len(batches))
	}
	assertSliceEqual(t, []int{1, 2}, batches[0])
	assertSliceEqual(t, []int{3, 4}, batches[1])
	assertSliceEqual(t, []int{5}, batches[2])
}

func TestBatchHookFired(t *testing.T) {
	batchCount := 0
	p := FromSlice([]int{1, 2, 3, 4, 5}).
		WithBatchHook(func(batch []int) {
			batchCount++
		})

	_ = PipeBatch(p, BatchConfig{Size: 2}).Collect()

	if batchCount != 3 {
		t.Errorf("expected 3 batch hooks, got %d", batchCount)
	}
}

func TestFromChannel(t *testing.T) {
	ch := make(chan int, 5)
	for i := 1; i <= 5; i++ {
		ch <- i
	}
	close(ch)

	result := FromChannel(ch).
		Filter(func(n int) bool { return n > 2 }).
		Collect()

	assertSliceEqual(t, []int{3, 4, 5}, result)
}

func TestFromReader(t *testing.T) {
	r := strings.NewReader("hello\nworld\ngo")
	result := FromReader(r).Collect()

	assertSliceEqual(t, []string{"hello", "world", "go"}, result)
}

func TestFromRange(t *testing.T) {
	result := FromRange(0, 5).Collect()
	assertSliceEqual(t, []int{0, 1, 2, 3, 4}, result)
}

func TestFromFunc(t *testing.T) {
	i := 0
	result := FromFunc(func() (int, bool) {
		if i >= 3 {
			return 0, false
		}
		v := i * i
		i++
		return v, true
	}).Collect()

	assertSliceEqual(t, []int{0, 1, 4}, result)
}

func TestElementHookFired(t *testing.T) {
	count := int32(0)
	FromSlice([]int{1, 2, 3}).
		WithElementHook(func(v int) {
			atomic.AddInt32(&count, 1)
		}).
		Collect()

	if atomic.LoadInt32(&count) != 3 {
		t.Errorf("expected 3 element hooks, got %d", count)
	}
}

func TestCompletionHookFired(t *testing.T) {
	completed := false
	FromSlice([]int{1, 2, 3}).
		WithCompletionHook(func() {
			completed = true
		}).
		Collect()

	if !completed {
		t.Error("completion hook not fired")
	}
}

func TestPeek(t *testing.T) {
	var peeked []int
	result := FromSlice([]int{1, 2, 3}).
		Peek(func(n int) { peeked = append(peeked, n) }).
		Collect()

	assertSliceEqual(t, []int{1, 2, 3}, result)
	assertSliceEqual(t, []int{1, 2, 3}, peeked)
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
		t.Errorf("expected 4 evaluations (lazy), got %d", evaluated)
	}
}

func TestWriteTo(t *testing.T) {
	var written []int
	FromSlice([]int{1, 2, 3}).
		WriteTo(func(n int) {
			written = append(written, n)
		})

	assertSliceEqual(t, []int{1, 2, 3}, written)
}

func TestEndToEndETLPipeline(t *testing.T) {
	type Record struct {
		ID     int
		Name   string
		Score  int
		Active bool
	}

	records := []Record{
		{1, "Alice", 85, true},
		{2, "Bob", 42, false},
		{3, "Charlie", 91, true},
		{4, "Diana", 67, true},
		{5, "Eve", 23, false},
		{6, "Frank", 78, true},
	}

	errCount := int32(0)
	elementCount := int32(0)

	p := FromSlice(records).
		WithElementHook(func(r Record) {
			atomic.AddInt32(&elementCount, 1)
		}).
		WithErrorHook(func(err error, r Record) {
			atomic.AddInt32(&errCount, 1)
		}).
		Filter(func(r Record) bool { return r.Active }).
		Filter(func(r Record) bool { return r.Score >= 70 })

	names := PipeMap(p, func(r Record) string {
		return strings.ToUpper(r.Name)
	}).Collect()

	assertSliceEqual(t, []string{"ALICE", "CHARLIE", "FRANK"}, names)
}

func assertSliceEqual[T comparable](t *testing.T, expected, actual []T) {
	t.Helper()
	if len(expected) != len(actual) {
		t.Errorf("length mismatch: expected %d, got %d\nexpected: %v\nactual:   %v", len(expected), len(actual), expected, actual)
		return
	}
	for i := range expected {
		if expected[i] != actual[i] {
			t.Errorf("mismatch at index %d: expected %v, got %v", i, expected[i], actual[i])
		}
	}
}

func TestCollectToNil(t *testing.T) {
	result := FromSlice([]int{1, 2, 3}).CollectTo(nil)
	assertSliceEqual(t, []int{1, 2, 3}, result)
}

func TestCollectToPreallocated(t *testing.T) {
	buf := make([]int, 0, 100)
	result := FromSlice([]int{1, 2, 3}).CollectTo(buf)
	assertSliceEqual(t, []int{1, 2, 3}, result)
}

func TestPipeWindowStep2(t *testing.T) {
	p := FromSlice([]int{1, 2, 3, 4, 5, 6})
	windows := PipeWindow(p, 3, 2).Collect()

	if len(windows) != 2 {
		t.Fatalf("expected 2 windows, got %d", len(windows))
	}
	assertSliceEqual(t, []int{1, 2, 3}, windows[0])
	assertSliceEqual(t, []int{3, 4, 5}, windows[1])
}

func TestPipeWindowSmallerThanSize(t *testing.T) {
	p := FromSlice([]int{1, 2})
	windows := PipeWindow(p, 5, 1).Collect()

	if len(windows) != 1 {
		t.Fatalf("expected 1 partial window, got %d", len(windows))
	}
	assertSliceEqual(t, []int{1, 2}, windows[0])
}

func TestPipeWindowEmpty(t *testing.T) {
	p := FromSlice([]int{})
	windows := PipeWindow(p, 3, 1).Collect()

	if len(windows) != 0 {
		t.Fatalf("expected 0 windows, got %d", len(windows))
	}
}

func TestPipeMapParallelStreamOrdered(t *testing.T) {
	p := FromSlice([]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
	result := PipeMapParallelStream(p, 4, 16, func(n int) int {
		return n * 10
	}).Collect()

	assertSliceEqual(t, []int{10, 20, 30, 40, 50, 60, 70, 80, 90, 100}, result)
}

func TestPipeMapParallelEmpty(t *testing.T) {
	p := FromSlice([]int{})
	result := PipeMapParallel(p, 4, func(n int) int { return n }).Collect()

	if len(result) != 0 {
		t.Errorf("expected empty, got %v", result)
	}
}

func TestPipeFilterParallelEmpty(t *testing.T) {
	p := FromSlice([]int{})
	result := PipeFilterParallel(p, 4, func(n int) bool { return true }).Collect()

	if len(result) != 0 {
		t.Errorf("expected empty, got %v", result)
	}
}

func TestPipeDistinctEmpty(t *testing.T) {
	p := FromSlice([]int{})
	result := PipeDistinct(p).Collect()

	if len(result) != 0 {
		t.Errorf("expected empty, got %v", result)
	}
}

func TestPipeChunkEmpty(t *testing.T) {
	p := FromSlice([]int{})
	result := PipeChunk(p, 3).Collect()

	if len(result) != 0 {
		t.Errorf("expected empty, got %v", result)
	}
}

func TestGroupByEmpty(t *testing.T) {
	groups := GroupBy(FromSlice([]int{}), func(n int) int { return n % 2 })
	if len(groups) != 0 {
		t.Errorf("expected empty groups, got %v", groups)
	}
}

func TestMaxByEmpty(t *testing.T) {
	_, ok := MaxBy(FromSlice([]int{}), func(n int) int { return n })
	if ok {
		t.Error("expected false for empty slice")
	}
}

func TestMinByEmpty(t *testing.T) {
	_, ok := MinBy(FromSlice([]int{}), func(n int) int { return n })
	if ok {
		t.Error("expected false for empty slice")
	}
}

func TestMultipleHooksCompose(t *testing.T) {
	calls := make([]string, 0)
	FromSlice([]int{1}).
		WithElementHook(func(v int) { calls = append(calls, "hook1") }).
		WithElementHook(func(v int) { calls = append(calls, "hook2") }).
		Collect()

	assertSliceEqual(t, []string{"hook1", "hook2"}, calls)
}

func TestPipeBatchWithTimeout(t *testing.T) {
	ch := make(chan int, 10)
	for i := 1; i <= 5; i++ {
		ch <- i
	}
	close(ch)

	batches := PipeBatch(FromChannel(ch), BatchConfig{
		Size:    3,
		MaxWait: 100 * time.Millisecond,
	}).Collect()

	total := 0
	for _, b := range batches {
		total += len(b)
	}
	if total != 5 {
		t.Errorf("expected 5 total elements across batches, got %d", total)
	}
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
