package gosplice_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	gs "github.com/lacolle87/gosplice"
)

// ===========================================================================
// Level 1 — Slice functions
// ===========================================================================

func ExampleMap() {
	result := gs.Map([]int{1, 2, 3, 4}, func(n int) int { return n * n })
	fmt.Println(result)
	// Output: [1 4 9 16]
}

func ExampleMapInPlace() {
	prices := []float64{10.0, 20.0, 30.0}
	gs.MapInPlace(prices, func(p float64) float64 { return p * 1.1 })
	fmt.Printf("%.1f %.1f %.1f\n", prices[0], prices[1], prices[2])
	// Output: 11.0 22.0 33.0
}

func ExampleFilter() {
	evens := gs.Filter([]int{1, 2, 3, 4, 5, 6}, func(n int) bool { return n%2 == 0 })
	fmt.Println(evens)
	// Output: [2 4 6]
}

func ExampleFilterInPlace() {
	data := []int{1, 2, 3, 4, 5, 6}
	result := gs.FilterInPlace(data, func(n int) bool { return n > 3 })
	fmt.Println(result)
	// Output: [4 5 6]
}

func ExampleReduce() {
	sum := gs.Reduce([]int{1, 2, 3, 4, 5}, 0, func(acc, n int) int { return acc + n })
	fmt.Println(sum)
	// Output: 15
}

func ExampleReduce_buildMap() {
	type Pair struct {
		Key   string
		Value int
	}
	pairs := []Pair{{"a", 1}, {"b", 2}, {"c", 3}}
	m := gs.Reduce(pairs, make(map[string]int), func(acc map[string]int, p Pair) map[string]int {
		acc[p.Key] = p.Value
		return acc
	})
	fmt.Println(m["a"], m["b"], m["c"])
	// Output: 1 2 3
}

func ExampleFind() {
	v, ok := gs.Find([]int{10, 20, 30, 40}, func(n int) bool { return n > 25 })
	fmt.Println(v, ok)
	// Output: 30 true
}

func ExampleSome() {
	fmt.Println(gs.Some([]int{1, 2, 3}, func(n int) bool { return n > 2 }))
	fmt.Println(gs.Some([]int{1, 2, 3}, func(n int) bool { return n > 5 }))
	// Output:
	// true
	// false
}

func ExampleEvery() {
	fmt.Println(gs.Every([]int{2, 4, 6}, func(n int) bool { return n%2 == 0 }))
	fmt.Println(gs.Every([]int{2, 3, 6}, func(n int) bool { return n%2 == 0 }))
	// Output:
	// true
	// false
}

func ExampleUnique() {
	result := gs.Unique([]int{1, 2, 2, 3, 1, 4, 3})
	fmt.Println(result)
	// Output: [1 2 3 4]
}

func ExampleChunk() {
	result := gs.Chunk([]int{1, 2, 3, 4, 5}, 2)
	fmt.Println(result)
	// Output: [[1 2] [3 4] [5]]
}

func ExampleFlatMap() {
	result := gs.FlatMap([]int{1, 2, 3}, func(n int) []int { return []int{n, n * 10} })
	fmt.Println(result)
	// Output: [1 10 2 20 3 30]
}

func ExampleZip() {
	names := []string{"Alice", "Bob", "Charlie"}
	scores := []int{95, 87, 92}
	pairs := gs.Zip(names, scores)
	for _, p := range pairs {
		fmt.Printf("%s:%d ", p.First, p.Second)
	}
	fmt.Println()
	// Output: Alice:95 Bob:87 Charlie:92
}

func ExampleReverse() {
	result := gs.Reverse([]int{1, 2, 3, 4})
	fmt.Println(result)
	// Output: [4 3 2 1]
}

func ExampleIncludes() {
	fmt.Println(gs.Includes([]string{"go", "rust", "python"}, "rust"))
	fmt.Println(gs.Includes([]string{"go", "rust", "python"}, "java"))
	// Output:
	// true
	// false
}

func ExampleCount() {
	n := gs.Count([]int{1, 2, 3, 4, 5}, func(n int) bool { return n > 3 })
	fmt.Println(n)
	// Output: 2
}

// ===========================================================================
// Level 2 — Pipelines (lazy, composable)
// ===========================================================================

func ExampleFromSlice() {
	result := gs.FromSlice([]int{1, 2, 3, 4, 5}).
		Filter(func(n int) bool { return n%2 == 0 }).
		Collect()
	fmt.Println(result)
	// Output: [2 4]
}

func ExamplePipeline_Take() {
	result := gs.FromSlice([]int{10, 20, 30, 40, 50}).Take(3).Collect()
	fmt.Println(result)
	// Output: [10 20 30]
}

func ExamplePipeline_Skip() {
	result := gs.FromSlice([]int{10, 20, 30, 40, 50}).Skip(2).Collect()
	fmt.Println(result)
	// Output: [30 40 50]
}

func ExamplePipeline_Reduce() {
	total := gs.FromSlice([]int{1, 2, 3, 4}).Reduce(0, func(a, b int) int { return a + b })
	fmt.Println(total)
	// Output: 10
}

func ExamplePipeline_Count() {
	n := gs.FromSlice([]int{1, 2, 3, 4, 5}).
		Filter(func(n int) bool { return n > 3 }).
		Count()
	fmt.Println(n)
	// Output: 2
}

func ExamplePipeline_First() {
	v, ok := gs.FromSlice([]int{5, 10, 15, 20}).
		Filter(func(n int) bool { return n > 12 }).
		First()
	fmt.Println(v, ok)
	// Output: 15 true
}

func ExamplePipeline_Any() {
	has := gs.FromSlice([]int{1, 2, 3, 4}).Any(func(n int) bool { return n == 3 })
	fmt.Println(has)
	// Output: true
}

func ExamplePipeline_All() {
	all := gs.FromSlice([]int{2, 4, 6}).All(func(n int) bool { return n%2 == 0 })
	fmt.Println(all)
	// Output: true
}

func ExamplePipeline_Peek() {
	var seen []int
	result := gs.FromSlice([]int{1, 2, 3}).
		Peek(func(n int) { seen = append(seen, n) }).
		Collect()
	fmt.Println(result, seen)
	// Output: [1 2 3] [1 2 3]
}

func ExampleFromRange() {
	result := gs.FromRange(0, 5).Collect()
	fmt.Println(result)
	// Output: [0 1 2 3 4]
}

func ExampleFromFunc() {
	i := 0
	result := gs.FromFunc(func() (int, bool) {
		if i >= 3 {
			return 0, false
		}
		i++
		return i * 10, true
	}).Collect()
	fmt.Println(result)
	// Output: [10 20 30]
}

func ExampleFromReader() {
	r := strings.NewReader("hello\nworld\ngosplice")
	lines := gs.FromReader(r).Collect()
	fmt.Println(lines)
	// Output: [hello world gosplice]
}

// ===========================================================================
// Type-changing transforms (free functions)
// ===========================================================================

func ExamplePipeMap() {
	result := gs.PipeMap(
		gs.FromSlice([]int{1, 2, 3}),
		func(n int) string { return fmt.Sprintf("item-%d", n) },
	).Collect()
	fmt.Println(result)
	// Output: [item-1 item-2 item-3]
}

func ExamplePipeMapErr() {
	result := gs.PipeMapErr(
		gs.FromSlice([]int{2, 0, 4}),
		func(n int) (float64, error) {
			if n == 0 {
				return 0, errors.New("division by zero")
			}
			return 10.0 / float64(n), nil
		},
	).Collect()
	fmt.Println(result)
	// Output: [5 2.5]
}

func ExamplePipeFlatMap() {
	result := gs.PipeFlatMap(
		gs.FromSlice([]string{"hello", "go"}),
		func(s string) []byte { return []byte(s) },
	).Collect()
	fmt.Println(string(result))
	// Output: hellogo
}

func ExamplePipeDistinct() {
	result := gs.PipeDistinct(gs.FromSlice([]int{3, 1, 2, 1, 3, 2})).Collect()
	fmt.Println(result)
	// Output: [3 1 2]
}

func ExamplePipeChunk() {
	chunks := gs.PipeChunk(gs.FromSlice([]int{1, 2, 3, 4, 5}), 2).Collect()
	for _, c := range chunks {
		fmt.Println(c)
	}
	// Output:
	// [1 2]
	// [3 4]
	// [5]
}

func ExamplePipeWindow() {
	windows := gs.PipeWindow(gs.FromSlice([]int{1, 2, 3, 4, 5}), 3, 1).Collect()
	for _, w := range windows {
		fmt.Println(w)
	}
	// Output:
	// [1 2 3]
	// [2 3 4]
	// [3 4 5]
}

func ExamplePipeReduce() {
	csv := gs.PipeReduce(gs.FromSlice([]int{1, 2, 3}), "",
		func(acc string, n int) string {
			if acc != "" {
				acc += ","
			}
			return acc + fmt.Sprintf("%d", n)
		})
	fmt.Println(csv)
	// Output: 1,2,3
}

// ===========================================================================
// Aggregations
// ===========================================================================

func ExampleGroupBy() {
	type Item struct {
		Cat   string
		Value int
	}
	items := []Item{{"a", 1}, {"b", 2}, {"a", 3}}
	groups := gs.GroupBy(gs.FromSlice(items), func(i Item) string { return i.Cat })
	fmt.Println(len(groups["a"]), len(groups["b"]))
	// Output: 2 1
}

func ExampleCountBy() {
	words := []string{"go", "rust", "go", "python", "go", "rust"}
	counts := gs.CountBy(gs.FromSlice(words), func(w string) string { return w })
	fmt.Println(counts["go"], counts["rust"], counts["python"])
	// Output: 3 2 1
}

func ExampleSumBy() {
	type Order struct {
		Amount float64
	}
	total := gs.SumBy(gs.FromSlice([]Order{{10.5}, {20.0}, {3.5}}),
		func(o Order) float64 { return o.Amount })
	fmt.Println(total)
	// Output: 34
}

func ExampleMaxBy() {
	type Score struct {
		Name  string
		Value int
	}
	top, _ := gs.MaxBy(gs.FromSlice([]Score{{"Alice", 90}, {"Bob", 95}, {"Charlie", 88}}),
		func(s Score) int { return s.Value })
	fmt.Println(top.Name)
	// Output: Bob
}

func ExampleMinBy() {
	low, _ := gs.MinBy(gs.FromSlice([]int{30, 10, 50, 20}), func(n int) int { return n })
	fmt.Println(low)
	// Output: 10
}

func ExamplePartition() {
	evens, odds := gs.Partition(
		gs.FromSlice([]int{1, 2, 3, 4, 5}),
		func(n int) bool { return n%2 == 0 },
	)
	fmt.Println(evens, odds)
	// Output: [2 4] [1 3 5]
}

// ===========================================================================
// Batching
// ===========================================================================

func ExamplePipeBatch() {
	batches := gs.PipeBatch(
		gs.FromSlice([]int{1, 2, 3, 4, 5}),
		gs.BatchConfig{Size: 2},
	).Collect()
	for _, b := range batches {
		fmt.Println(b)
	}
	// Output:
	// [1 2]
	// [3 4]
	// [5]
}

// ===========================================================================
// Level 3 — Hooks, error handling, context
// ===========================================================================

func ExamplePipeline_WithElementHook() {
	var count atomic.Int64
	result := gs.FromSlice([]int{1, 2, 3, 4, 5}).
		WithElementHook(gs.CountElements[int](&count)).
		Filter(func(n int) bool { return n > 2 }).
		Collect()
	fmt.Println(result, count.Load())
	// Output: [3 4 5] 3
}

func ExamplePipeline_WithCompletionHook() {
	completed := false
	gs.FromSlice([]int{1, 2, 3}).
		WithCompletionHook(func() { completed = true }).
		Collect()
	fmt.Println(completed)
	// Output: true
}

func ExampleRetryHandler() {
	var attempts atomic.Int32
	result := gs.PipeMapErr(
		gs.FromSlice([]int{42}).
			WithErrorHandler(gs.RetryHandler[int](3, 0)).
			WithMaxRetries(5),
		func(n int) (string, error) {
			if attempts.Add(1) < 3 {
				return "", errors.New("transient")
			}
			return fmt.Sprintf("ok-%d", n), nil
		},
	).Collect()
	fmt.Println(result)
	// Output: [ok-42]
}

func ExampleAbortOnError() {
	result := gs.PipeMapErr(
		gs.FromSlice([]int{1, 2, 3, 4, 5}).
			WithErrorHandler(gs.AbortOnError[int]()),
		func(n int) (int, error) {
			if n == 3 {
				return 0, errors.New("stop")
			}
			return n * 10, nil
		},
	).Collect()
	fmt.Println(result)
	// Output: [10 20]
}

func ExamplePipeline_WithContext() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	result := gs.FromSlice([]int{1, 2, 3, 4, 5}).
		WithContext(ctx).
		Collect()
	fmt.Println(result)
	// Output: [1 2 3 4 5]
}

func ExamplePipeline_WithTimeout() {
	ch := make(chan int)
	go func() {
		defer close(ch)
		for i := 0; ; i++ {
			ch <- i
			time.Sleep(10 * time.Millisecond)
		}
	}()

	p := gs.FromChannel(ch).WithTimeout(35 * time.Millisecond)
	result := p.Collect()

	// Exact count depends on timing, but should be partial.
	fmt.Println(len(result) > 0 && len(result) < 100)
	// Output: true
}

// ===========================================================================
// Parallel processing
// ===========================================================================

func ExamplePipeMapParallel() {
	result := gs.PipeMapParallel(
		gs.FromSlice([]int{1, 2, 3, 4}), 2,
		func(n int) int { return n * n },
	).Collect()
	fmt.Println(result)
	// Output: [1 4 9 16]
}

func ExamplePipeFilterParallel() {
	result := gs.PipeFilterParallel(
		gs.FromSlice([]int{1, 2, 3, 4, 5, 6}), 2,
		func(n int) bool { return n%2 == 0 },
	).Collect()
	fmt.Println(result)
	// Output: [2 4 6]
}

// ===========================================================================
// Sinks
// ===========================================================================

func ExampleToWriterString() {
	var buf strings.Builder
	_ = gs.ToWriterString(
		gs.FromSlice([]string{"hello", "world"}),
		&buf,
		func(s string) string { return s + "\n" },
	)
	fmt.Print(buf.String())
	// Output:
	// hello
	// world
}

func ExamplePipeline_ToChannel() {
	ch := make(chan int, 10)
	go gs.FromSlice([]int{1, 2, 3}).ToChannel(ch)

	var result []int
	for v := range ch {
		result = append(result, v)
	}
	fmt.Println(result)
	// Output: [1 2 3]
}

// ===========================================================================
// Combined pipeline example
// ===========================================================================

func ExampleFromSlice_pipeline() {
	type Order struct {
		Customer string
		Amount   float64
		Status   string
	}

	orders := []Order{
		{"Alice", 150.0, "completed"},
		{"Bob", 0, "failed"},
		{"Charlie", 320.0, "completed"},
		{"Diana", 89.0, "completed"},
	}

	results := gs.PipeMap(
		gs.FromSlice(orders).
			Filter(func(o Order) bool { return o.Status == "completed" }).
			Filter(func(o Order) bool { return o.Amount > 100 }),
		func(o Order) string { return fmt.Sprintf("%s:%.0f", o.Customer, o.Amount) },
	).Collect()

	fmt.Println(results)
	// Output: [Alice:150 Charlie:320]
}
