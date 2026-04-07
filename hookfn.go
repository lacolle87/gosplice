package gosplice

import (
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

// RetryHandler returns an ErrorHandler that retries failed elements up to
// maxAttempts times with linear backoff between attempts.
//
// The backoff sleep blocks the pipeline's iteration goroutine. For sequential
// pipelines (PipeMapErr) this is the correct behavior — elements are processed
// one at a time and the sleep provides natural back-pressure. For pipelines with
// large backoff values or high throughput requirements, consider a custom
// ErrorHandler that returns Retry without sleeping, paired with external rate
// limiting or circuit breaker logic.
//
// Backoff formula: backoff × (attempt + 1), where attempt starts at 1.
// With maxAttempts=3, backoff=100ms: attempt 1 sleeps 200ms, attempt 2 sleeps
// 300ms, attempt 3 returns Skip.
//
// Note: PipeMapParallelErr does not support retries. Use sequential PipeMapErr
// with WithErrorHandler and WithMaxRetries for retry semantics.
func RetryHandler[T any](maxAttempts int, backoff time.Duration) ErrorHandler[T] {
	return func(err error, elem T, attempt int) ErrorAction {
		if attempt >= maxAttempts {
			return Skip
		}
		if backoff > 0 {
			time.Sleep(backoff * time.Duration(attempt+1))
		}
		return Retry
	}
}

// AbortOnError returns an ErrorHandler that stops the pipeline on the first error.
func AbortOnError[T any]() ErrorHandler[T] {
	return func(err error, elem T, attempt int) ErrorAction {
		return Abort
	}
}

// SkipOnError returns an ErrorHandler that silently drops every failed element.
func SkipOnError[T any]() ErrorHandler[T] {
	return func(err error, elem T, attempt int) ErrorAction {
		return Skip
	}
}

// RetryThenAbort returns an ErrorHandler that retries failed elements up to
// maxAttempts times, then aborts the entire pipeline if retries are exhausted.
//
// Same backoff behavior as RetryHandler — sleep blocks the iteration goroutine.
// Use this when partial results are unacceptable: either the element succeeds
// within the retry budget, or the pipeline stops.
//
// See RetryHandler for backoff formula and parallel pipeline limitations.
func RetryThenAbort[T any](maxAttempts int, backoff time.Duration) ErrorHandler[T] {
	return func(err error, elem T, attempt int) ErrorAction {
		if attempt >= maxAttempts {
			return Abort
		}
		if backoff > 0 {
			time.Sleep(backoff * time.Duration(attempt+1))
		}
		return Retry
	}
}

// LogErrorsTo returns an ErrorHook that writes each error and its element to w.
func LogErrorsTo[T any](w io.Writer) ErrorHook[T] {
	return func(err error, elem T) {
		fmt.Fprintf(w, "error processing %v: %v\n", elem, err)
	}
}

// CountElements returns an ElementHook that atomically increments counter
// for every element that reaches the terminal's iteration loop.
func CountElements[T any](counter *atomic.Int64) ElementHook[T] {
	return func(v T) {
		counter.Add(1)
	}
}

// CountErrors returns an ErrorHook that atomically increments counter
// for every error encountered during PipeMapErr processing.
func CountErrors[T any](counter *atomic.Int64) ErrorHook[T] {
	return func(err error, elem T) {
		counter.Add(1)
	}
}

// CountBatches returns a BatchHook that atomically increments counter
// for every batch emitted by PipeBatch.
func CountBatches[T any](counter *atomic.Int64) BatchHook[T] {
	return func(batch []T) {
		counter.Add(1)
	}
}

// CollectErrors returns an ErrorHook that appends every error to the provided
// slice. The hook is safe for concurrent use (mutex-protected internally).
func CollectErrors[T any](errs *[]error) ErrorHook[T] {
	var mu sync.Mutex
	return func(err error, elem T) {
		mu.Lock()
		*errs = append(*errs, err)
		mu.Unlock()
	}
}
