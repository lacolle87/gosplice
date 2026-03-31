# GoSplice

A high-performance, lazy, composable ETL pipeline framework for Go with generics.

GoSplice provides streaming pipelines that handle millions of elements with minimal allocations, configurable parallelism, batching, hooks, and aggregation — all with type-safe generics.

## Installation

Requires Go 1.23+.

```bash
go get github.com/lacolle87/gosplice
```

## Quick Start

```go
package main

import (
    "fmt"
    "strings"
    gs "github.com/lacolle87/gosplice"
)

type User struct {
    Name   string
    Score  int
    Active bool
}

func main() {
    users := []User{
        {"Alice", 85, true},
        {"Bob", 42, false},
        {"Charlie", 91, true},
        {"Diana", 67, true},
        {"Eve", 78, true},
    }

    // Lazy pipeline: nothing executes until Collect()
    names := gs.PipeMap(
        gs.FromSlice(users).
            Filter(func(u User) bool { return u.Active }).
            Filter(func(u User) bool { return u.Score >= 70 }),
        func(u User) string { return strings.ToUpper(u.Name) },
    ).Collect()

    fmt.Println(names) // [ALICE CHARLIE EVE]
}
```

## Architecture

### Sources

Pipelines begin with a source adapter. All sources implement a pull-based iterator internally, enabling lazy one-pass evaluation.

| Factory | Description |
|---|---|
| `FromSlice[T]([]T)` | In-memory slice |
| `FromChannel[T](<-chan T)` | Go channel (streaming) |
| `FromReader(io.Reader)` | Line-by-line text reader |
| `FromFunc[T](func() (T, bool))` | Custom generator |
| `FromRange(start, end int)` | Integer range |

### Pipeline Operations

**Same-type methods** (chainable on `*Pipeline[T]`):

- `Filter(func(T) bool)` — lazy predicate filter
- `Take(n int)` — limit to first n elements
- `Skip(n int)` — skip first n elements
- `Peek(func(T))` — side-effect without consuming

**Type-changing free functions** (Go generics require these as top-level functions):

- `PipeMap[T, U](p, func(T) U)` — transform elements
- `PipeMapErr[T, U](p, func(T) (U, error))` — transform with error handling
- `PipeFlatMap[T, U](p, func(T) []U)` — map + flatten
- `PipeDistinct[T comparable](p)` — deduplicate
- `PipeChunk[T](p, size)` — group into fixed-size batches
- `PipeWindow[T](p, size, step)` — sliding window

### Terminal Operations

Terminals trigger lazy evaluation:

- `Collect() []T` — gather all results
- `CollectTo(dst []T) []T` — reuse existing buffer
- `Reduce(func(T,T) T, initial T) T` — fold
- `ForEach(func(T))` — side-effect iteration
- `WriteTo(func(T))` — output to external system
- `Count() int` — element count
- `First() (T, bool)` — first matching element
- `Any(func(T) bool) bool` — short-circuit existence check
- `All(func(T) bool) bool` — short-circuit universal check

### Aggregation

Free functions for keyed aggregation:

```go
groups := gs.GroupBy(pipeline, func(u User) string { return u.Department })
counts := gs.CountBy(pipeline, func(u User) string { return u.Role })
total  := gs.SumBy(pipeline, func(u User) float64 { return u.Salary })
top, _ := gs.MaxBy(pipeline, func(u User) int { return u.Score })
low, _ := gs.MinBy(pipeline, func(u User) int { return u.Score })
matched, unmatched := gs.Partition(pipeline, func(u User) bool { return u.Active })
```

### Parallel Operations

Order-preserving parallel execution with configurable worker pools:

```go
// Parallel map — results maintain input order
result := gs.PipeMapParallel(pipeline, 8, expensiveTransform).Collect()

// Parallel filter
result := gs.PipeFilterParallel(pipeline, 4, expensivePredicate).Collect()

// Parallel map with error handling
result := gs.PipeMapParallelErr(pipeline, 4, func(r Record) (Output, error) {
    return process(r)
}).Collect()

// Unordered parallel (higher throughput, no ordering guarantee)
result := gs.PipeMapParallelUnordered(pipeline, 8, transform).Collect()

// Streaming parallel for channels/readers (order-preserving, bounded memory)
result := gs.PipeMapParallelStream(pipeline, 4, 256, transform).Collect()
```

### Batching

Configurable batching with size and optional timeout for streaming sources:

```go
// Fixed-size batches
batches := gs.PipeBatch(pipeline, gs.BatchConfig{Size: 100}).Collect()

// With timeout for streaming — emit partial batch if timeout expires
batches := gs.PipeBatch(channelPipeline, gs.BatchConfig{
    Size:    100,
    MaxWait: 500 * time.Millisecond,
})
```

### Hooks

Non-blocking, composable hooks for observability without modifying core logic:

```go
pipeline := gs.FromSlice(data).
    WithElementHook(func(r Record) {
        metrics.Inc("records.processed")
    }).
    WithErrorHook(func(err error, r Record) {
        log.Printf("error processing %v: %v", r.ID, err)
    }).
    WithBatchHook(func(batch []Record) {
        db.BulkInsert(batch)
    }).
    WithCompletionHook(func() {
        log.Println("pipeline complete")
    }).
    WithTimeout(30 * time.Second)
```

Multiple hooks of the same type compose — they fire in registration order.

## Design Decisions

**Why free functions for Map/FlatMap?** Go generics don't support method-level type parameters different from the receiver's. `Pipeline[T].Map(func(T) U) Pipeline[U]` is impossible as a method. Free functions like `PipeMap[T, U]` are the idiomatic solution and provide full type safety without `interface{}`.

**Why pull-based iteration?** Push-based (channel per stage) creates goroutine overhead. Pull-based `Next() (T, bool)` keeps everything on one goroutine until you explicitly opt into parallelism.

**Why keep standalone functions?** The `Map`, `Filter`, `Reduce` etc. standalone functions in `slice.go` remain for simple slice operations where a full pipeline is overkill.

## Standalone Slice Utilities

For cases where a pipeline is unnecessary:

```go
doubled := gs.Map(nums, func(n int) int { return n * 2 })
evens := gs.Filter(nums, func(n int) bool { return n%2 == 0 })
sum := gs.Reduce(nums, func(a, b int) int { return a + b }, 0)
unique := gs.Unique(nums)
chunks := gs.Chunk(nums, 10)
reversed := gs.Reverse(nums)
// ... and more
```

## Testing

```bash
go test -v                        # Unit tests
go test -bench=. -benchmem        # Benchmarks
go test -race ./...               # Race detection
```

## License

MIT License — see [LICENSE](LICENSE).
