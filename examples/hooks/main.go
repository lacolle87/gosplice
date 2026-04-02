package main

import (
	"errors"
	"fmt"
	gs "github.com/lacolle87/gosplice"
	"math/rand"
	"os"
	"sync/atomic"
	"time"
)

type RawRecord struct {
	ID       int
	Email    string
	Amount   float64
	Currency string
	Source   string
}

type ValidRecord struct {
	ID       int
	Email    string
	AmountEU float64
	Source   string
}

var rates = map[string]float64{
	"EUR": 1.0,
	"USD": 0.92,
	"GBP": 1.17,
	"JPY": 0.0061,
}

func generateRecords(n int) []RawRecord {
	sources := []string{"web", "api", "import", "manual"}
	currencies := []string{"EUR", "USD", "GBP", "JPY", "???"}
	emails := []string{
		"alice@example.com", "bob@test.com", "", "charlie@corp.net",
		"invalid", "diana@example.com", "eve@test.com", "",
	}

	records := make([]RawRecord, n)
	for i := range records {
		records[i] = RawRecord{
			ID:       i + 1,
			Email:    emails[rand.Intn(len(emails))],
			Amount:   float64(rand.Intn(10000)) + rand.Float64()*100,
			Currency: currencies[rand.Intn(len(currencies))],
			Source:   sources[rand.Intn(len(sources))],
		}
		// ~5% negative amounts (refunds that shouldn't be here)
		if rand.Float64() < 0.05 {
			records[i].Amount = -records[i].Amount
		}
	}
	return records
}

func validate(r RawRecord) (ValidRecord, error) {
	if r.Email == "" {
		return ValidRecord{}, errors.New("empty email")
	}
	if len(r.Email) < 5 || r.Email[0] == '@' {
		return ValidRecord{}, fmt.Errorf("bad email: %s", r.Email)
	}
	if r.Amount <= 0 {
		return ValidRecord{}, fmt.Errorf("negative amount: %.2f", r.Amount)
	}

	rate, ok := rates[r.Currency]
	if !ok {
		return ValidRecord{}, fmt.Errorf("unknown currency: %s", r.Currency)
	}

	return ValidRecord{
		ID:       r.ID,
		Email:    r.Email,
		AmountEU: r.Amount * rate,
		Source:   r.Source,
	}, nil
}

// flaky simulates an unreliable external service
// fails ~40% of the time, then succeeds
func flaky(r ValidRecord) (ValidRecord, error) {
	if rand.Float64() < 0.4 {
		return ValidRecord{}, errors.New("service unavailable")
	}
	r.AmountEU = float64(int(r.AmountEU*100)) / 100
	return r, nil
}

func main() {
	fmt.Println("Hooks showcase")
	fmt.Println("==============\n")

	records := generateRecords(200)

	// -------------------------------------------------------
	// Example 1: observability hooks — count, log, collect
	// -------------------------------------------------------
	fmt.Println("--- 1. Observability hooks ---")

	var elemCount atomic.Int64
	var errCount atomic.Int64
	var collectedErrors []error

	result1 := gs.PipeMapErr(
		gs.FromSlice(records).
			WithElementHook(gs.CountElements[RawRecord](&elemCount)).
			WithErrorHook(gs.CountErrors[RawRecord](&errCount)).
			WithErrorHook(gs.CollectErrors[RawRecord](&collectedErrors)).
			WithErrorHook(gs.LogErrorsTo[RawRecord](os.Stderr)).
			WithCompletionHook(func() {
				fmt.Printf("  Pipeline done: %d processed, %d errors\n",
					elemCount.Load(), errCount.Load())
			}),
		validate,
	).Collect()

	fmt.Printf("  Valid records: %d\n", len(result1))
	fmt.Printf("  Collected errors: %d\n", len(collectedErrors))

	// Error breakdown by type
	errTypes := gs.CountBy(gs.FromSlice(collectedErrors), func(e error) string {
		msg := e.Error()
		switch {
		case len(msg) > 10 && msg[:10] == "bad email:":
			return "bad email"
		case msg == "empty email":
			return "empty email"
		case len(msg) > 8 && msg[:8] == "negative":
			return "negative amount"
		case len(msg) > 7 && msg[:7] == "unknown":
			return "unknown currency"
		default:
			return "other"
		}
	})

	fmt.Println("  Error breakdown:")
	for typ, count := range errTypes {
		fmt.Printf("    %-20s %d\n", typ, count)
	}

	// -------------------------------------------------------
	// Example 2: ErrorHandler — skip vs abort
	// -------------------------------------------------------
	fmt.Println("\n--- 2. Error handler: SkipOnError ---")

	skipResult := gs.PipeMapErr(
		gs.FromSlice(records).WithErrorHandler(gs.SkipOnError[RawRecord]()),
		validate,
	).Collect()

	fmt.Printf("  SkipOnError: %d valid out of %d\n", len(skipResult), len(records))

	fmt.Println("\n--- 3. Error handler: AbortOnError ---")

	abortResult := gs.PipeMapErr(
		gs.FromSlice(records).WithErrorHandler(gs.AbortOnError[RawRecord]()),
		validate,
	).Collect()

	fmt.Printf("  AbortOnError: stopped after %d valid (first error = abort)\n", len(abortResult))

	// -------------------------------------------------------
	// Example 3: retry with backoff
	// -------------------------------------------------------
	fmt.Println("\n--- 4. Retry handler on flaky service ---")

	var retryAttempts atomic.Int64

	retryResult := gs.PipeMapErr(
		gs.FromSlice(result1[:min(50, len(result1))]).
			WithErrorHandler(func(err error, elem ValidRecord, attempt int) gs.ErrorAction {
				retryAttempts.Add(1)
				if attempt >= 3 {
					return gs.Skip
				}
				return gs.Retry
			}).
			WithMaxRetries(5),
		flaky,
	).Collect()

	fmt.Printf("  Input: %d | Output: %d | Retry attempts: %d\n",
		min(50, len(result1)), len(retryResult), retryAttempts.Load())

	// -------------------------------------------------------
	// Example 4: ready-made RetryHandler with backoff
	// -------------------------------------------------------
	fmt.Println("\n--- 5. RetryHandler with backoff ---")

	retryAttempts.Store(0)

	retryResult2 := gs.PipeMapErr(
		gs.FromSlice(result1[:min(50, len(result1))]).
			WithErrorHandler(gs.RetryHandler[ValidRecord](3, 10*time.Millisecond)).
			WithMaxRetries(5),
		flaky,
	).Collect()

	fmt.Printf("  With 10ms backoff: %d out of %d succeeded\n",
		len(retryResult2), min(50, len(result1)))

	// -------------------------------------------------------
	// Example 5: RetryThenAbort — fail-fast after retries
	// -------------------------------------------------------
	fmt.Println("\n--- 6. RetryThenAbort ---")

	alwaysFail := func(r ValidRecord) (ValidRecord, error) {
		return ValidRecord{}, errors.New("always fails")
	}

	aborted := gs.PipeMapErr(
		gs.FromSlice(result1[:min(20, len(result1))]).
			WithErrorHandler(gs.RetryThenAbort[ValidRecord](2, 0)).
			WithMaxRetries(5),
		alwaysFail,
	).Collect()

	fmt.Printf("  RetryThenAbort on always-fail: %d results (pipeline aborted after retries)\n", len(aborted))

	// -------------------------------------------------------
	// Example 6: batch hooks
	// -------------------------------------------------------
	fmt.Println("\n--- 7. Batch hooks ---")

	var batchCount atomic.Int64
	var totalBatchSize atomic.Int64

	batches := gs.PipeBatch(
		gs.FromSlice(result1).
			WithBatchHook(func(batch []ValidRecord) {
				batchCount.Add(1)
				totalBatchSize.Add(int64(len(batch)))
				if batchCount.Load() <= 3 {
					fmt.Printf("  Batch #%d: %d items, first=%s\n",
						batchCount.Load(), len(batch), batch[0].Email)
				}
			}),
		gs.BatchConfig{Size: 25},
	).Collect()

	fmt.Printf("  Total: %d batches, %d items, avg size: %.1f\n",
		batchCount.Load(), totalBatchSize.Load(),
		float64(totalBatchSize.Load())/float64(batchCount.Load()))
	_ = batches

	// -------------------------------------------------------
	// Example 7: composing multiple hooks
	// -------------------------------------------------------
	fmt.Println("\n--- 8. Composing multiple hooks ---")

	var phase1Count atomic.Int64
	var phase2Count atomic.Int64
	start := time.Now()

	// Multiple element hooks fire in registration order
	// Completion hook reports timing
	total := gs.PipeMapErr(
		gs.FromSlice(records).
			WithElementHook(func(r RawRecord) { phase1Count.Add(1) }).
			WithElementHook(func(r RawRecord) {
				if phase1Count.Load()%50 == 0 {
					fmt.Printf("  Progress: %d/%d\n", phase1Count.Load(), len(records))
				}
			}).
			WithErrorHook(func(err error, r RawRecord) { phase2Count.Add(1) }).
			WithCompletionHook(func() {
				fmt.Printf("  Completed in %v\n", time.Since(start).Round(time.Microsecond))
			}),
		validate,
	).Collect()

	fmt.Printf("  Hooks fired: %d element, %d error → %d valid\n",
		phase1Count.Load(), phase2Count.Load(), len(total))

	// -------------------------------------------------------
	// Example 8: ErrorHandler takes precedence over ErrorHook
	// -------------------------------------------------------
	fmt.Println("\n--- 9. Handler vs hook precedence ---")

	hookFired := false
	handlerFired := false

	gs.PipeMapErr(
		gs.FromSlice([]RawRecord{{ID: 1, Email: "", Amount: 100, Currency: "EUR"}}).
			WithErrorHook(func(err error, r RawRecord) { hookFired = true }).
			WithErrorHandler(func(err error, r RawRecord, attempt int) gs.ErrorAction {
				handlerFired = true
				return gs.Skip
			}),
		validate,
	).Collect()

	fmt.Printf("  Handler fired: %v, Hook fired: %v\n", handlerFired, hookFired)
	fmt.Println("  (handler takes precedence — hooks are not called)")

	// -------------------------------------------------------
	// Summary
	// -------------------------------------------------------
	fmt.Println("\n--- Summary ---")
	fmt.Println("Hooks used in this example:")
	fmt.Println("  CountElements     — atomic counter per element")
	fmt.Println("  CountErrors       — atomic counter per error")
	fmt.Println("  CollectErrors     — gather all errors into a slice")
	fmt.Println("  LogErrorsTo       — log errors to io.Writer")
	fmt.Println("  CountBatches      — atomic counter per batch")
	fmt.Println("  WithElementHook   — custom per-element logic (progress)")
	fmt.Println("  WithErrorHook     — custom error observation")
	fmt.Println("  WithCompletionHook— cleanup / final metrics")
	fmt.Println("  WithErrorHandler  — control flow: Skip / Retry / Abort")
	fmt.Println("  SkipOnError       — drop and continue")
	fmt.Println("  AbortOnError      — stop pipeline on first error")
	fmt.Println("  RetryHandler      — retry N times with backoff")
	fmt.Println("  RetryThenAbort    — retry N times, then abort")
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
