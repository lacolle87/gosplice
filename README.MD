# GoSplice

Go library for working with slices, streams, and data pipelines. Type-safe generics, lazy evaluation, parallel processing, configurable error handling.

Use the whole pipeline or pick the parts you need — standalone slice functions, streaming I/O, hooks, parallel workers. Each piece works independently.

## Installation

Requires Go 1.23+. Tested with Go 1.26.1

```bash
go get github.com/lacolle87/gosplice
```

```go
import gs "github.com/lacolle87/gosplice"
```

## Use what you need

GoSplice is not an all-or-nothing framework. You can use it at three levels:

**Level 1 — Slice functions.** Drop-in utilities for everyday slice work. No pipeline, no setup.

```go
doubled := gs.Map(nums, func(n int) int { return n * 2 })
evens := gs.Filter(users, func(u User) bool { return u.Active })
total := gs.Reduce(orders, 0.0, func(sum float64, o Order) float64 {
    return sum + o.Price
})
```

**Level 2 — Pipelines.** Chain operations lazily. Nothing executes until a terminal (Collect, Reduce, ForEach) is called.

```go
names := gs.PipeMap(
    gs.FromSlice(users).
        Filter(func(u User) bool { return u.Active }).
        Filter(func(u User) bool { return u.Score >= 70 }),
    func(u User) string { return u.Name },
).Collect()
```

**Level 3 — Pipelines with hooks, error handling, parallelism, and I/O.** For ETL workflows, data processing jobs, streaming.

```go
gs.FromChannel(recordsCh).
    WithErrorHandler(gs.RetryHandler[Record](3, 100*time.Millisecond)).
    WithCompletionHook(func() { log.Println("done") })
// ...process and write to output
```

---

## Slice functions

Standalone functions that work on regular Go slices. No pipeline required.

### Map / MapInPlace

```go
squared := gs.Map([]int{1, 2, 3}, func(n int) int { return n * n })
// [1, 4, 9]

// Zero allocations — modifies the slice in place
gs.MapInPlace(prices, func(p float64) float64 { return p * 1.1 })
```

### Reduce

Two type parameters — accumulator type can differ from element type.

```go
// Sum
total := gs.Reduce(nums, 0, func(acc, n int) int { return acc + n })

// Build a map from a slice
index := gs.Reduce(users, make(map[int]User), func(m map[int]User, u User) map[int]User {
    m[u.ID] = u
    return m
})

// Concatenate with separator
csv := gs.Reduce(names, "", func(acc, name string) string {
    if acc != "" { acc += "," }
    return acc + name
})
```

### Filter / FilterInPlace

```go
adults := gs.Filter(people, func(p Person) bool { return p.Age >= 18 })

// Zero allocations — compacts the slice in place (modifies the original)
active := gs.FilterInPlace(users, func(u User) bool { return u.Active })
```

### Search functions

```go
gs.Some(nums, func(n int) bool { return n > 100 })   // any match?
gs.Every(nums, func(n int) bool { return n > 0 })     // all match?
gs.Includes(tags, "urgent")                             // value exists?

v, ok := gs.Find(users, func(u User) bool { return u.Email == email })
v, ok := gs.FindLast(logs, func(l Log) bool { return l.Level == "ERROR" })

idx := gs.FindIndex(items, func(i Item) bool { return i.ID == targetID })
idx := gs.IndexOf(names, "Alice")
idx := gs.LastIndexOf(tags, "retry")
```

### Transform functions

```go
gs.Reverse([]int{1, 2, 3})          // [3, 2, 1] — new slice
gs.ReverseInPlace(slice)              // modifies in place, zero alloc

gs.Unique([]int{1, 2, 2, 3, 1})     // [1, 2, 3] — preserves order
gs.Flat([][]int{{1, 2}, {3, 4}})     // [1, 2, 3, 4]
gs.FlatMap(nums, func(n int) []int { return []int{n, -n} })

gs.Chunk([]int{1,2,3,4,5}, 2)       // [[1,2], [3,4], [5]]
gs.Remove(slice, []int{2, 4})        // removes specific values
gs.Count(nums, func(n int) bool { return n > 0 })
gs.Zip(names, scores)                // pairs of (name, score)
```

---

## Pipelines

Lazy, composable chains of operations. Nothing runs until you call a terminal.

### Sources

```go
gs.FromSlice(data)                    // in-memory slice
gs.FromChannel(ch)                    // Go channel (streaming)
gs.FromReader(file)                   // io.Reader, line by line
gs.FromFunc(func() (T, bool) {...})   // custom generator
gs.FromRange(0, 1000)                 // integer range
```

### Operations

Chainable methods (same type):

```go
pipeline.Filter(func(r Record) bool { return r.Active })
pipeline.Take(100)      // first N elements
pipeline.Skip(10)       // skip first N
pipeline.Peek(func(r Record) { log.Println(r) })  // observe without consuming
```

Free functions (can change type):

```go
gs.PipeMap(p, func(r Record) string { return r.Name })
gs.PipeMapErr(p, func(r Record) (Output, error) { return transform(r) })
gs.PipeFlatMap(p, func(r Record) []Tag { return r.Tags })
gs.PipeDistinct(p)           // remove duplicates (comparable types)
gs.PipeChunk(p, 100)         // group into fixed-size batches
gs.PipeWindow(p, 5, 1)       // sliding window (size=5, step=1)
```

Type-changing operations are free functions because Go generics don't allow methods to introduce new type parameters.

### Terminals

Terminals trigger execution:

```go
results := pipeline.Collect()                // []T
results := pipeline.CollectTo(existingBuf)   // reuse a buffer
pipeline.ForEach(func(r Record) { process(r) })
pipeline.WriteTo(func(r Record) { db.Insert(r) })

count := pipeline.Count()
first, ok := pipeline.First()
hasAny := pipeline.Any(func(r Record) bool { return r.Priority == "high" })
allOk := pipeline.All(func(r Record) bool { return r.Valid })

sum := pipeline.Reduce(0, func(acc int, r Record) int { return acc + r.Value })

// Cross-type reduce (different accumulator and element types)
total := gs.PipeReduce(pipeline, 0.0, func(sum float64, o Order) float64 {
    return sum + o.Price
})
```

### Aggregations

```go
groups := gs.GroupBy(pipeline, func(u User) string { return u.Department })
// map[string][]User

counts := gs.CountBy(pipeline, func(u User) string { return u.Role })
// map[string]int

total := gs.SumBy(pipeline, func(o Order) float64 { return o.Total })
top, ok := gs.MaxBy(pipeline, func(u User) int { return u.Score })
low, ok := gs.MinBy(pipeline, func(u User) int { return u.Score })
matched, rest := gs.Partition(pipeline, func(u User) bool { return u.Active })
```

---

## Output / sinks

Write pipeline results to channels, writers, or callbacks.

### ToChannel

Streams results into a Go channel. Closes the channel when the pipeline is done.

```go
ch := make(chan string, 100)
p := gs.FromSlice(users).Filter(func(u User) bool { return u.Active })

go gs.PipeMap(p, func(u User) string { return u.Name }).ToChannel(ch)

for name := range ch {
    fmt.Println(name)
}
```

### ToWriter / ToWriterString

Streams results to any `io.Writer` — files, buffers, HTTP response bodies.

```go
file, _ := os.Create("output.csv")
defer file.Close()

err := gs.ToWriterString(
    gs.FromSlice(records).Filter(func(r Record) bool { return r.Valid }),
    file,
    func(r Record) string {
        return fmt.Sprintf("%s,%d,%f\n", r.Name, r.ID, r.Score)
    },
)
```

---

## Error handling

Two approaches: fire-and-forget hooks for observability, or error handlers for control flow.

### Observability hooks (fire-and-forget)

Old-style hooks run on every error but don't affect the pipeline flow. The element is always skipped.

```go
pipeline.WithErrorHook(func(err error, elem Record) {
    log.Printf("failed: %v on %v", err, elem.ID)
})
```

### Error handlers (control flow)

Error handlers decide what happens: skip the element, retry the operation, or abort the pipeline.

```go
type ErrorAction int
const (
    Skip  ErrorAction // drop element, continue
    Retry             // re-run the operation
    Abort             // stop the pipeline
)
```

```go
pipeline.WithErrorHandler(func(err error, elem Record, attempt int) gs.ErrorAction {
    if attempt >= 3 {
        return gs.Skip
    }
    time.Sleep(time.Duration(attempt) * 100 * time.Millisecond)
    return gs.Retry
})
```

The `attempt` counter starts at 1 and increments on each retry. Use `WithMaxRetries(n)` as a safety limit.

### Ready-made handlers

```go
// Retry up to 3 times with 100ms backoff, then skip
pipeline.WithErrorHandler(gs.RetryHandler[Record](3, 100*time.Millisecond))

// Retry up to 3 times, then abort the entire pipeline
pipeline.WithErrorHandler(gs.RetryThenAbort[Record](3, 50*time.Millisecond))

// Skip all errors silently
pipeline.WithErrorHandler(gs.SkipOnError[Record]())

// Abort on first error
pipeline.WithErrorHandler(gs.AbortOnError[Record]())
```

### Ready-made observability hooks

```go
var elemCount atomic.Int64
var errCount atomic.Int64
var errs []error

pipeline.
    WithElementHook(gs.CountElements[Record](&elemCount)).
    WithErrorHook(gs.CountErrors[Record](&errCount)).
    WithErrorHook(gs.CollectErrors[Record](&errs)).
    WithErrorHook(gs.LogErrorsTo[Record](os.Stderr)).
    WithBatchHook(gs.CountBatches[Record](&batchCount))
```

When both an error handler and error hooks are set, the handler takes precedence. Hooks are not called.

---

## Hooks

Hooks provide observability and lifecycle callbacks without modifying pipeline logic.

```go
pipeline.
    WithElementHook(func(r Record) {
        metrics.Inc("records.processed")
    }).
    WithBatchHook(func(batch []Record) {
        db.BulkInsert(batch)
    }).
    WithCompletionHook(func() {
        log.Println("pipeline finished")
    })
```

Multiple hooks of the same type compose — they fire in registration order. Hooks work with all terminals: Collect, Reduce, ForEach, Count, First, Any, All, ToChannel, WriteTo.

---

## Parallel processing

Parallel operations split work across goroutines. Order is preserved.

```go
// Parallel map — each worker processes a contiguous batch
results := gs.PipeMapParallel(pipeline, 8, expensiveTransform).Collect()

// Parallel filter
results := gs.PipeFilterParallel(pipeline, 4, expensivePredicate).Collect()

// Parallel map with error handling
results := gs.PipeMapParallelErr(pipeline, 4, func(r Record) (Output, error) {
    return callExternalAPI(r)
}).Collect()

// Streaming parallel — for channels/readers, bounded memory, order-preserving
results := gs.PipeMapParallelStream(pipeline, 4, 256, transform).Collect()
```

Parallel operations make sense when the per-element work is expensive (API calls, heavy computation). For cheap operations like arithmetic, the sequential pipeline is faster.

## Batching

Group elements into fixed-size batches for bulk processing.

```go
// Fixed-size batches from a slice
batches := gs.PipeBatch(pipeline, gs.BatchConfig{Size: 100}).Collect()

// With timeout — for streaming sources, emit partial batch if timeout expires
batches := gs.PipeBatch(channelPipeline, gs.BatchConfig{
    Size:    100,
    MaxWait: 500 * time.Millisecond,
})
```

---

## Quick example

```go
orders := []Order{
    {1, "Alice", 150.0, "completed"},
    {2, "Bob", 0, "failed"},
    {3, "Charlie", 320.0, "completed"},
    {4, "Diana", 89.0, "completed"},
    {5, "Eve", 450.0, "pending"},
}

var processed atomic.Int64

results := gs.PipeMap(
    gs.FromSlice(orders).
        WithElementHook(gs.CountElements[Order](&processed)).
        Filter(func(o Order) bool { return o.Status == "completed" }).
        Filter(func(o Order) bool { return o.Amount > 100 }),
    func(o Order) string {
        return fmt.Sprintf("%s: $%.2f", o.Customer, o.Amount)
    },
).Collect()
// ["Alice: $150.00", "Charlie: $320.00"]

byStatus := gs.CountBy(gs.FromSlice(orders), func(o Order) string { return o.Status })
// map[completed:3 failed:1 pending:1]
```

---

## Real-world example: streaming ETL from an API

Full working example that streams Star Wars characters from [swapi.dev](https://swapi.dev), collects them through a pipeline, then runs multiple analyses. See [`examples/swapi/main.go`](examples/swapi/main.go).

```go
// Phase 1: stream from paginated API into a channel, collect through pipeline
people := gs.FromChannel(characterChannel()).
    WithElementHook(gs.CountElements[Person](&fetched)).
    WithElementHook(func(p Person) {
        if fetched.Load()%10 == 0 {
            log.Printf("streamed: %d characters", fetched.Load())
        }
    }).
    WithCompletionHook(func() {
        log.Printf("stream complete: %d characters in %v", fetched.Load(), time.Since(start))
    }).
    Collect()

// Phase 2: single-pass stats using cross-type Reduce
stats := gs.Reduce(people, Stats{}, func(s Stats, p Person) Stats {
    s.Total++
    if h := p.GetHeight(); h > 0 {
        s.HeightSum += h
        s.HeightCount++
    }
    return s
})

// Phase 3: distributions via CountBy
heightDist := gs.CountBy(gs.FromSlice(people), heightBucket)
genderDist := gs.CountBy(gs.FromSlice(people), normalizeGender)
hairDist   := gs.CountBy(gs.FromSlice(people), normalizeHair)
eyeDist    := gs.CountBy(gs.FromSlice(people), func(p Person) string { return p.EyeColor })

// Phase 4: find extremes
tallest, _ := gs.MaxBy(
    gs.FromSlice(people).Filter(func(p Person) bool { return p.GetHeight() > 0 }),
    func(p Person) int { return p.GetHeight() },
)

// Phase 5: group names by height category
byHeight := gs.GroupBy(gs.FromSlice(people), heightBucket)
for bucket, group := range byHeight {
    names := gs.Map(group, func(p Person) string { return p.Name })
    fmt.Printf("  %s: %s\n", bucket, strings.Join(names, ", "))
}

// Phase 6: write filtered results to stdout
_ = gs.ToWriterString(
    gs.FromSlice(people).Filter(func(p Person) bool { return p.GetHeight() > 180 }),
    os.Stdout,
    func(p Person) string { return fmt.Sprintf("  %s — %d cm\n", p.Name, p.GetHeight()) },
)
```

What this uses from GoSplice:

| Feature | API |
|---|---|
| Streaming source | `FromChannel` |
| Ready-made hook | `CountElements` |
| Custom hooks | `WithElementHook`, `WithCompletionHook` |
| Terminal | `Collect` |
| Standalone reduce | `Reduce` (cross-type: `[]Person → Stats`) |
| Distributions | `CountBy` ×4 |
| Grouping | `GroupBy` + `Map` for names |
| Extremes | `MaxBy` with `Filter` |
| Type transform | `PipeMap` (`Person → NameFilms`) |
| Output sink | `ToWriterString` |

## Testing

```bash
go test -v ./...                  # all tests
go test -bench=. -benchmem        # benchmarks with allocations
go test -race ./...               # race condition detection
```

## Project structure

```
gosplice/
├── source.go       Source interface, adapters (FromSlice, FromChannel, FromReader, FromFunc, FromRange)
├── stage.go        Pipeline stage types (filter, map, flatMap, chunk, window, distinct...)
├── pipeline.go     Pipeline[T] struct, chainable operations, terminal operations
├── transform.go    Type-changing functions (PipeMap, PipeFlatMap, PipeReduce...)
├── aggregate.go    Aggregations (GroupBy, CountBy, SumBy, MaxBy, MinBy, Partition)
├── parallel.go     Parallel operations (PipeMapParallel, PipeFilterParallel...)
├── batch.go        Batching with size and timeout
├── sink.go         Output adapters (ToChannel, ToWriter, ToWriterString)
├── hooks.go        Hook types, ErrorAction, error handling dispatch
├── hookfn.go       Ready-made hooks (RetryHandler, CountElements, LogErrorsTo...)
├── slice.go        Standalone slice functions (Map, Filter, Reduce, Unique...)
└── examples/
    └── swapi/      Streaming ETL from Star Wars API
```

## License

MIT License — see [LICENSE](LICENSE).