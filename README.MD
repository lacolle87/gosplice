# GoSplice

[![Go Report Card](https://goreportcard.com/badge/github.com/lacolle87/gosplice)](https://goreportcard.com/report/github.com/lacolle87/gosplice)
[![pkg.go.dev](https://pkg.go.dev/badge/github.com/lacolle87/gosplice)](https://pkg.go.dev/github.com/lacolle87/gosplice)
[![Go Version](https://img.shields.io/badge/Go-1.23+-00ADD8.svg)](https://golang.org/doc/go1.23)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Last Commit](https://img.shields.io/github/last-commit/lacolle87/gosplice/master)](https://github.com/lacolle87/gosplice/commits/master)
[![CI](https://github.com/lacolle87/gosplice/actions/workflows/main.yaml/badge.svg)](https://github.com/lacolle87/gosplice/actions/workflows/main.yaml)
[![Go Coverage](https://img.shields.io/codecov/c/github/lacolle87/gosplice)](https://codecov.io/gh/lacolle87/gosplice)

Go library for working with slices, streams, and data pipelines. Type-safe generics, lazy evaluation, parallel processing, configurable error handling, context-aware cancellation.

Use the whole pipeline or pick the parts you need — standalone slice functions, streaming I/O, hooks, parallel workers. Each piece works independently.

## Why GoSplice

GoSplice started as a small utility library — `Map`, `Filter`, `Reduce` for Go slices with generics. The kind of thing you write once in every project and then copy-paste between repos.

Then it grew. Not because there was a grand plan, but because real projects kept needing the next piece: "I need to chain these operations lazily" → pipelines. "I need to process a channel with timeout" → context-aware sources. "Some records fail and I want to skip them" → error hooks. "API calls need rate limiting" → RateLimit stage. Each addition solved a concrete problem that came up in production code.

The philosophy stayed the same throughout: **don't fight Go, extend it**. No new DSL, no reflection where generics work, no framework that takes over your `main()`. Every function should feel like it could have been in the standard library — if the standard library shipped with generics from day one.

Performance matters. GoSplice takes the same approach as hand-written `for` loops: direct iteration over slices without intermediate allocations when possible, fast paths for common patterns (`sliceSource` → `Collect` skips the iterator protocol entirely), amortized context checks to avoid per-element channel operations. The benchmarks are in the repo — run `go test -bench=. -benchmem` and compare with your hand-rolled loops. The gap is small enough that the readability win is worth it.

Where GoSplice helps:

- **Backend services** — process API responses, transform data between layers, validate and enrich records with error tracking
- **ETL and data pipelines** — read CSV, filter, transform, batch-insert into a database, all with timeout and rate limiting
- **CLI tools** — quick scripts that process files, aggregate data, produce reports without pulling in heavyweight frameworks
- **Anywhere you'd write a `for` loop with an accumulator** — GoSplice gives you the same loop with composability, hooks, and parallel execution bolted on

It is not a replacement for Apache Beam or Spark. It is a replacement for the 200-line `processRecords()` function that everyone writes differently in every project.

## Installation

Requires Go 1.23+.

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

**Level 3 — Pipelines with hooks, error handling, parallelism, context, CSV, rate limiting, and I/O.** For ETL workflows, data processing jobs, streaming.

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

gs.FromChannelCtx(ctx, recordsCh).
RateLimit(gs.RateLimitConfig{Rate: 100, Burst: 20}).
WithErrorHandler(gs.RetryHandler[Record](3, 100*time.Millisecond)).
WithCompletionHook(func() { log.Println("done") })
// ...process and write to output
```

---

## What's new in v1.1

- **CSV** — streaming read/write with three access levels: `Row` (pandas-style `row.Get("name")`, `row.GetFloat("price")`), functional mapper (`FromCSVFunc`), and struct tags (`FromCSV[T]`). Two matching sinks: `ToCSV` and `ToCSVStruct`. Custom delimiters, header handling, variable-length rows.
- **Rate limiting** — `RateLimit(cfg)` as a lazy pipeline stage. Token bucket, context-aware, composable with Filter/PipeMap/parallel. Use for API rate limits, DB throttling, backpressure.
- **Statistics** — `MeanBy`, `VarianceBy`, `StdDevBy`, `MedianBy`, `PercentileBy`, `DescribeBy`, `CorrelationBy` for pipelines. Standalone slice versions: `Mean`, `Variance`, `StdDev`, `Median`, `Percentile`, `Describe`, `Correlation`, `Histogram`. Single-pass Welford's algorithm where possible.
- **Numeric constraint** — `Numeric` interface for stats functions (int/uint/float types, no string).

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
total := gs.Reduce(nums, 0, func(acc, n int) int { return acc + n })

index := gs.Reduce(users, make(map[int]User), func(m map[int]User, u User) map[int]User {
m[u.ID] = u
return m
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
gs.Some(nums, func(n int) bool { return n > 100 })
gs.Every(nums, func(n int) bool { return n > 0 })
gs.Includes(tags, "urgent")

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

### Lazy evaluation

```go
// Nothing happens here — just building a description
p := gs.FromSlice(data).
Filter(func(n int) bool { return n > 0 }).
Take(10)

// NOW it runs
result := p.Collect()
```

### Sources

| Function | Description | Use when |
|---|---|---|
| `FromSlice(data)` | In-memory slice | You have all data upfront |
| `FromChannel(ch)` | Go channel | Streaming from a goroutine producer |
| `FromChannelCtx(ctx, ch)` | Context-aware channel | Streaming with cancellation support |
| `FromReader(r)` | `io.Reader`, line by line | Reading files, HTTP bodies |
| `FromFunc(fn)` | Custom generator `func() (T, bool)` | Computed/infinite sequences |
| `FromRange(start, end)` | Integer range `[start, end)` | Numeric sequences |
| `FromCSVRows(r, cfg)` | CSV → `Row` (pandas-style) | CSV exploration, untyped access |
| `FromCSVFunc(r, cfg, fn)` | CSV → `T` via mapper function | CSV with explicit parsing |
| `FromCSV[T](r, cfg)` | CSV → `T` via struct tags | CSV with automatic field mapping |

### Operations (lazy, chainable)

| Operation | Description |
|---|---|
| `Filter(fn)` | Keep elements where `fn` returns true |
| `Take(n)` | Stop after first `n` elements |
| `Skip(n)` | Discard first `n` elements |
| `Peek(fn)` | Call `fn` on each element, pass through unchanged |
| `RateLimit(cfg)` | Token bucket rate limiter — throttle element throughput |

### Type-changing operations (free functions)

| Function | Description |
|---|---|
| `PipeMap(p, fn)` | Transform `T → U` |
| `PipeMapErr(p, fn)` | Transform `T → (U, error)` with error handling |
| `PipeFlatMap(p, fn)` | Transform `T → []U`, flatten results |
| `PipeDistinct(p)` | Remove duplicates (comparable types) |
| `PipeChunk(p, size)` | Group into fixed-size `[]T` batches |
| `PipeWindow(p, size, step)` | Sliding window (step=1 → full overlap, step=size → same as chunk) |
| `PipeReduce(p, init, fn)` | Cross-type reduce `T → U` |

### Terminals (trigger execution)

| Terminal | Returns |
|---|---|
| `Collect()` | `[]T` |
| `CollectTo(buf)` | `[]T` (reuse allocation) |
| `ForEach(fn)` | — |
| `Count()` | `int` |
| `First()` | `(T, bool)` |
| `Reduce(init, fn)` | `T` |
| `Any(pred)` | `bool` (short-circuits) |
| `All(pred)` | `bool` (short-circuits) |

### Aggregations

| Function | Returns |
|---|---|
| `GroupBy(p, keyFn)` | `map[K][]T` |
| `CountBy(p, keyFn)` | `map[K]int` |
| `SumBy(p, fn)` | `N` |
| `MaxBy(p, fn)` | `(T, bool)` |
| `MinBy(p, fn)` | `(T, bool)` |
| `Partition(p, pred)` | `(matched, unmatched []T)` |

### Statistics

| Function | Description |
|---|---|
| `MeanBy(p, fn)` | Arithmetic mean (single-pass) |
| `VarianceBy(p, fn)` | Population variance (Welford's algorithm) |
| `StdDevBy(p, fn)` | Population standard deviation |
| `MedianBy(p, fn)` | Median (collects + sorts) |
| `PercentileBy(p, pct, fn)` | Arbitrary percentile with linear interpolation |
| `DescribeBy(p, fn)` | Full summary: count, mean, stddev, min, Q1, median, Q3, max |
| `CorrelationBy(p, fnX, fnY)` | Pearson correlation (single-pass) |

Standalone slice versions: `Mean`, `Variance`, `StdDev`, `Median`, `Percentile`, `Describe`, `Correlation`, `Histogram`.

### Sinks

| Function | Description |
|---|---|
| `ToChannel(ch)` | Write elements to a channel |
| `ToWriter(p, w, encode)` | Write to `io.Writer` as bytes |
| `ToWriterString(p, w, format)` | Write to `io.Writer` as strings |
| `ToCSV(p, w, cfg, header, fn)` | Write CSV with explicit formatter |
| `ToCSVStruct[T](p, w, cfg)` | Write CSV via struct tags |

---

## Context and timeout

```go
p := gs.FromChannel(tickStream).
WithTimeout(5 * time.Second).
WithTimeoutHook(func(d time.Duration) {
log.Printf("pipeline timed out after %v", d)
})

results := p.Collect()
if errors.Is(p.Err(), context.DeadlineExceeded) {
// partial results are still available
}
```

If you need both an external context and a timeout, call `WithContext` first, then `WithTimeout`. Context propagates automatically through all pipeline stages.

When `ctx` is nil (default), the pipeline takes the exact same code paths as before context support — zero overhead.

---

## Error handling

### Error handlers (control flow)

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

### Ready-made handlers

```go
pipeline.WithErrorHandler(gs.RetryHandler[Record](3, 100*time.Millisecond))
pipeline.WithErrorHandler(gs.RetryThenAbort[Record](3, 50*time.Millisecond))
pipeline.WithErrorHandler(gs.SkipOnError[Record]())
pipeline.WithErrorHandler(gs.AbortOnError[Record]())
```

### Observability hooks

```go
var elemCount atomic.Int64
var errCount atomic.Int64

pipeline.
WithElementHook(gs.CountElements[Record](&elemCount)).
WithErrorHook(gs.CountErrors[Record](&errCount)).
WithErrorHook(gs.CollectErrors[Record](&errs)).
WithBatchHook(gs.CountBatches[Record](&batchCount)).
WithCompletionHook(func() { log.Println("done") })
```

When both an error handler and error hooks are set, the handler takes precedence.

---

## Parallel processing

Order is preserved in all parallel operations.

| Function | Memory model |
|---|---|
| `PipeMapParallel(p, workers, fn)` | Drains source into a slice, splits across workers |
| `PipeFilterParallel(p, workers, fn)` | Same — drain first, parallel predicate |
| `PipeMapParallelErr(p, workers, fn)` | Same — drain first, parallel with errors |
| `PipeMapParallelStream(p, workers, buf, fn)` | Streaming — bounded memory, reads on the fly |

For unbounded or very large sources, use `PipeMapParallelStream`.

---

## Batching

```go
batches := gs.PipeBatch(pipeline, gs.BatchConfig{Size: 100}).Collect()

// With timeout — emit partial batch if no new element arrives
batches := gs.PipeBatch(channelPipeline, gs.BatchConfig{
Size:    100,
MaxWait: 500 * time.Millisecond,
})
```

---

## CSV

Three levels of access, same "use what you need" philosophy.

### Row (pandas-style)

No struct definitions needed. Great for exploration, scripts, and files with unknown schemas.

```go
rows := gs.FromCSVRows(file, gs.CSVConfig{Header: true}).
    Filter(func(r gs.Row) bool { return r.Get("status") == "active" }).
    Filter(func(r gs.Row) bool {
        salary, _ := r.GetFloat("salary")
        return salary > 50000
    }).
    Collect()

name := rows[0].Get("name")         // by column name
first := rows[0].Index(0)            // by position
age, err := rows[0].GetInt("age")    // typed parsing
m := rows[0].AsMap()                 // map[string]string
```

`Row` methods: `Get`, `Index`, `GetInt`, `GetFloat`, `GetBool`, `Has`, `Len`, `Fields`, `AsMap`, `Columns`. Header column index is built once and shared across all rows.

### Functional mapper (zero-reflect)

Full control over parsing. No reflection, no struct tags.

```go
orders := gs.FromCSVFunc(file, gs.CSVConfig{Header: true}, func(row []string) (Order, error) {
    amount, err := strconv.ParseFloat(row[1], 64)
    if err != nil {
        return Order{}, err
    }
    return Order{ID: row[0], Amount: amount}, nil
}).Collect()
```

### Struct tags (reflect-once)

Automatic mapping via `csv:"name"` tags. Reflection runs once at startup to build a field table — per-row decoding is a fast-path switch, no further reflection.

```go
type Order struct {
    ID     string  `csv:"order_id"`
    Amount float64 `csv:"amount"`
    Status string  `csv:"status"`
}

orders := gs.FromCSV[Order](file, gs.CSVConfig{Header: true}).
    Filter(func(o Order) bool { return o.Status == "completed" }).
    Collect()
```

### Writing

```go
// Functional — explicit formatter
gs.ToCSV(pipeline, writer, gs.CSVConfig{Header: true},
    []string{"id", "amount", "status"},
    func(o Order) []string {
        return []string{o.ID, fmt.Sprintf("%.2f", o.Amount), o.Status}
    },
)

// Struct tags
gs.ToCSVStruct[Order](pipeline, writer, gs.CSVConfig{Header: true})
```

### CSVConfig

`Comma` — field delimiter (default `,`). `Comment` — skip lines starting with this rune. `Header` — first row is column names. `LazyQuotes`, `TrimLeadingSpace` — passed through to `encoding/csv`. All three source levels and both sinks share the same config.

---

## Rate limiting

Token bucket rate limiter as a lazy pipeline stage. Sits in the chain like `Filter` or `Take` — nothing happens until a terminal is called.

```go
// 100 requests/second, burst of 20
results := gs.PipeMapErr(
    gs.FromSlice(urls).
        RateLimit(gs.RateLimitConfig{Rate: 100, Burst: 20}),
    fetchURL,
).Collect()
```

### Configuration

`Rate` — elements per `Interval`. `Interval` — time window (default 1s). `Burst` — bucket capacity, how many elements pass immediately before the sustained rate kicks in (defaults to `Rate`).

### With context

Rate limiter respects pipeline context. When used with `WithTimeout`, the wait is cancelled on deadline:

```go
results := gs.FromSlice(urls).
    WithTimeout(5 * time.Minute).
    RateLimit(gs.RateLimitConfig{Rate: 50}).
    Collect() // stops on timeout, partial results available
```

For explicit context control outside the pipeline (e.g. inside parallel operations):

```go
gs.RateLimitCtx(pipeline, gs.RateLimitConfig{Rate: 200}, ctx)
```

### When to use

- External API rate limits — avoid 429 responses
- Database/queue write throttling — don't saturate connection pools
- Parallel workers with a shared global limit — `RateLimit` before `PipeMapParallelStream`
- Backpressure on fast producers (Kafka, WebSocket) — cap processing rate, buffer in channel

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

## Examples

Five working examples in [`examples/`](examples/).

### Streaming ETL: Star Wars API → [`examples/swapi/`](examples/swapi/main.go)

Streams characters from a paginated HTTP API with context timeout, collects through a pipeline with progress hooks, then runs analytics.

Uses: `FromChannelCtx`, `WithContext`, `Err`, `CountElements`, hooks, `Collect`, `Reduce`, `CountBy`, `MaxBy`, `GroupBy`, `Map`, `Filter`, `PipeMap`, `ToWriterString`.

### Market data: tick stream → candles → indicators → [`examples/quotes/`](examples/quotes/main.go)

Generates price ticks with context timeout, batches into 1-minute candles, computes technical indicators (SMA, EMA, RSI, Bollinger Bands, VWAP).

Uses: `FromChannelCtx`, `PipeBatch`, `PipeMap`, `PipeWindow`, `PipeReduce`, `CountElements`, `SumBy`, `MaxBy`, `MinBy`, `Filter`, `ToWriterString`.

### Data analytics: real estate reports → [`examples/realestate/`](examples/realestate/main.go)

Generates 500 property listings with bad data, runs validation + labeling pipeline with error tracking, produces market report.

Uses: `FromSlice`, `PipeMapErr`, `CollectErrors`, `CountElements`, `Filter`, `Map`, `FlatMap`, `Some`, `Unique`, `Reduce`, `SumBy`, `MaxBy`, `MinBy`, `CountBy`, `GroupBy`, `Partition`, `ToWriterString`.

### Hooks showcase → [`examples/hooks/`](examples/hooks/main.go)

10 scenarios: observability hooks, error handlers, retry with backoff, batch hooks, hook composition, handler vs hook precedence, context timeout with hook.

Uses: `CountElements`, `CountErrors`, `CollectErrors`, `LogErrorsTo`, `CountBatches`, `WithElementHook`, `WithErrorHook`, `WithErrorHandler`, `WithCompletionHook`, `WithMaxRetries`, `WithTimeout`, `WithTimeoutHook`, `Err`, `SkipOnError`, `AbortOnError`, `RetryHandler`, `RetryThenAbort`, `PipeMapErr`, `PipeBatch`.

### CSV pipeline → [`examples/csv/`](examples/csv/main.go)

Same employee data processed two ways: functional (`FromCSVFunc` → `ToCSV`) and struct tags (`FromCSV` → `ToCSVStruct`). Both approaches produce identical output. Includes department analytics with `GroupBy`, `SumBy`, `MaxBy`, `CountBy`.

Uses: `FromCSVFunc`, `FromCSV`, `ToCSV`, `ToCSVStruct`, `CSVConfig`, `Filter`, `PipeMap`, `WithElementHook`, `CountElements`, `GroupBy`, `SumBy`, `MaxBy`, `CountBy`, `ToWriterString`.

---

## Testing

```bash
go test -v ./...                  # all tests
go test -bench=. -benchmem        # benchmarks with allocations
go test -race ./...               # race condition detection
```

## Project structure

```
gosplice/
├── source.go       Source interface, adapters (FromSlice, FromChannel, FromChannelCtx, FromReader, FromFunc, FromRange)
├── stage.go        Pipeline stage types (filter, map, flatMap, chunk, window, distinct...)
├── pipeline.go     Pipeline[T], WithContext, WithTimeout, Err, chainable operations, terminals
├── iter.go         Core iteration primitives (drain, fold, foldWhile) with ctx-aware branches
├── transform.go    Type-changing functions (PipeMap, PipeFlatMap, PipeReduce...)
├── aggregate.go    Aggregations (GroupBy, CountBy, SumBy, MaxBy, MinBy, Partition)
├── stats.go        Statistics (MeanBy, VarianceBy, MedianBy, PercentileBy, DescribeBy, CorrelationBy, Histogram)
├── parallel.go     Parallel operations (PipeMapParallel, PipeFilterParallel, PipeMapParallelStream...)
├── batch.go        Batching with size and timeout, context-aware cancellation
├── ratelimit.go    Token bucket rate limiter (RateLimit, RateLimitCtx)
├── csv.go          CSV sources and sinks (FromCSV, FromCSVFunc, ToCSV, ToCSVStruct, CSVConfig)
├── row.go          Row type with pandas-style access (FromCSVRows, Row.Get, Row.GetFloat...)
├── sink.go         Output adapters (ToChannel, ToWriter, ToWriterString)
├── hooks.go        Hook types, ErrorAction, error handling dispatch
├── hookfn.go       Ready-made hooks (RetryHandler, CountElements, LogErrorsTo...)
├── slice.go        Standalone slice functions (Map, Filter, Reduce, Unique...)
└── examples/
    ├── swapi/        Streaming ETL from Star Wars API
    ├── quotes/       Market tick stream → candles → technical indicators
    ├── realestate/   Property labeling and market analytics
    ├── hooks/        Hooks system showcase (all hook types + timeout)
    └── csv/          CSV pipeline — functional vs struct tags
```

## License

MIT License — see [LICENSE](LICENSE).