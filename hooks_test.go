package gosplice

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ===========================================================================
// Hooks internals
// ===========================================================================

func TestHooksClone(t *testing.T) {
	h := newHooks[int]()
	h.OnElement = append(h.OnElement, func(int) {})
	h.OnError = append(h.OnError, func(error, int) {})
	h.MaxRetries = 7
	c := h.clone()
	if len(c.OnElement) != 1 || c.MaxRetries != 7 {
		t.Error("clone should copy fields")
	}
	c.OnElement = append(c.OnElement, func(int) {})
	if len(h.OnElement) != 1 {
		t.Error("clone mutation affected original")
	}
}

func TestHooksCloneNil(t *testing.T) {
	var h *Hooks[int]
	c := h.clone()
	if c == nil || c.MaxRetries != 3 {
		t.Errorf("clone of nil: nil=%v MaxRetries=%d", c == nil, c.MaxRetries)
	}
}

func TestHooksFireTimeout(t *testing.T) {
	var fired time.Duration
	h := newHooks[int]()
	h.OnTimeout = append(h.OnTimeout, func(d time.Duration) { fired = d })
	h.fireTimeout(5 * time.Second)
	if fired != 5*time.Second {
		t.Errorf("expected 5s, got %v", fired)
	}
}

func TestHooksFireBatch(t *testing.T) {
	var batches [][]int
	h := newHooks[int]()
	h.OnBatch = append(h.OnBatch, func(b []int) { batches = append(batches, b) })
	h.fireBatch([]int{1, 2, 3})
	if len(batches) != 1 {
		t.Fatalf("expected 1, got %d", len(batches))
	}
	assertSliceEqual(t, []int{1, 2, 3}, batches[0])
}

func TestHooksMultipleCompletionOrder(t *testing.T) {
	var order []int
	h := newHooks[int]()
	h.OnCompletion = append(h.OnCompletion, func() { order = append(order, 1) })
	h.OnCompletion = append(h.OnCompletion, func() { order = append(order, 2) })
	h.fireCompletion()
	assertSliceEqual(t, []int{1, 2}, order)
}

func TestHooksSequentialConfig(t *testing.T) {
	var c1, c2 atomic.Int64
	p := FromSlice([]int{1, 2, 3})
	p.WithElementHook(CountElements[int](&c1))
	p.WithElementHook(CountElements[int](&c2))
	p.Collect()
	if c1.Load() != 3 || c2.Load() != 3 {
		t.Errorf("hook1=%d hook2=%d, expected 3 each", c1.Load(), c2.Load())
	}
}

// ===========================================================================
// CollectErrors race safety (from dr_test.go Fix 1)
// ===========================================================================

func TestCollectErrorsConcurrentSafe(t *testing.T) {
	var errs []error
	hook := CollectErrors[int](&errs)
	var wg sync.WaitGroup
	const goroutines, errorsPerG = 32, 100
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < errorsPerG; i++ {
				hook(errors.New("err"), id)
			}
		}(g)
	}
	wg.Wait()
	if len(errs) != goroutines*errorsPerG {
		t.Errorf("expected %d errors, got %d", goroutines*errorsPerG, len(errs))
	}
}

func TestCollectErrorsSequentialRegression(t *testing.T) {
	var errs []error
	result := PipeMapErr(
		FromSlice([]int{1, 0, 3, 0, 5}).WithErrorHook(CollectErrors[int](&errs)),
		func(n int) (int, error) {
			if n == 0 {
				return 0, errors.New("zero")
			}
			return n * 10, nil
		}).Collect()
	assertSliceEqual(t, []int{10, 30, 50}, result)
	if len(errs) != 2 {
		t.Errorf("expected 2 errors, got %d", len(errs))
	}
}

// ===========================================================================
// Retry semantics (review #4)
// ===========================================================================

// After fix: maxRetries = total attempts. WithMaxRetries(0) → element not
// processed at all. Current code calls fn once before the retry loop,
// so WithMaxRetries(0) and WithMaxRetries(1) behave identically — a bug.

func TestMaxRetries0NoAttempts(t *testing.T) {
	var attempts atomic.Int32
	PipeMapErr(
		FromSlice([]int{1, 2, 3}).
			WithErrorHandler(func(err error, v int, attempt int) ErrorAction { return Retry }).
			WithMaxRetries(0),
		func(n int) (int, error) { attempts.Add(1); return 0, errors.New("fail") },
	).Collect()
	// After fix: 0 means "don't attempt at all" → fn never called.
	if attempts.Load() != 0 {
		t.Errorf("WithMaxRetries(0): fn called %d time(s), expected 0", attempts.Load())
	}
}

func TestMaxRetries0vs1MustDiffer(t *testing.T) {
	countFor := func(maxR int) int32 {
		var attempts atomic.Int32
		PipeMapErr(
			FromSlice([]int{1}).
				WithErrorHandler(func(err error, v int, attempt int) ErrorAction { return Retry }).
				WithMaxRetries(maxR),
			func(n int) (int, error) { attempts.Add(1); return 0, errors.New("fail") },
		).Collect()
		return attempts.Load()
	}
	c0 := countFor(0)
	c1 := countFor(1)
	if c0 == c1 {
		t.Errorf("WithMaxRetries(0) and WithMaxRetries(1) are indistinguishable: both = %d calls", c0)
	}
}

func TestMaxRetries3ExactAttempts(t *testing.T) {
	var attempts atomic.Int32
	PipeMapErr(
		FromSlice([]int{1}).
			WithErrorHandler(func(err error, v int, attempt int) ErrorAction { return Retry }).
			WithMaxRetries(3),
		func(n int) (int, error) { attempts.Add(1); return 0, errors.New("fail") },
	).Collect()
	if attempts.Load() != 3 {
		t.Errorf("WithMaxRetries(3): fn called %d time(s), expected exactly 3", attempts.Load())
	}
}

func TestRetrySucceedsOnSecondAttempt(t *testing.T) {
	var attempts atomic.Int32
	result := PipeMapErr(
		FromSlice([]int{1}).
			WithErrorHandler(func(err error, v int, attempt int) ErrorAction { return Retry }).
			WithMaxRetries(5),
		func(n int) (int, error) {
			if attempts.Add(1) < 3 {
				return 0, errors.New("transient")
			}
			return n * 100, nil
		}).Collect()
	if len(result) != 1 || result[0] != 100 {
		t.Errorf("expected [100], got %v", result)
	}
}

func TestAbortStopsPipeline(t *testing.T) {
	var processed []int
	result := PipeMapErr(
		FromSlice([]int{1, 2, 3, 4, 5}).WithErrorHandler(AbortOnError[int]()),
		func(n int) (int, error) {
			processed = append(processed, n)
			if n == 3 {
				return 0, errors.New("stop")
			}
			return n * 10, nil
		}).Collect()
	if len(result) != 2 {
		t.Errorf("expected [10 20], got %v", result)
	}
	if len(processed) != 3 {
		t.Errorf("expected 3 processed, got %d", len(processed))
	}
}

func TestRetryHandlerExhaustsThenSkips(t *testing.T) {
	var attempts atomic.Int32
	result := PipeMapErr(
		FromSlice([]int{1}).
			WithErrorHandler(RetryHandler[int](3, 0)).WithMaxRetries(5),
		func(n int) (int, error) { attempts.Add(1); return 0, errors.New("fail") },
	).Collect()
	if len(result) != 0 {
		t.Errorf("expected empty, got %v", result)
	}
}

func TestRetryThenAbortExhaustsThenAborts(t *testing.T) {
	result := PipeMapErr(
		FromSlice([]int{1, 2, 3}).
			WithErrorHandler(RetryThenAbort[int](2, 0)).WithMaxRetries(5),
		func(n int) (int, error) { return 0, errors.New("fail") },
	).Collect()
	if len(result) != 0 {
		t.Errorf("expected empty, got %v", result)
	}
}

func TestMapErrNoHandlerNoHookSkips(t *testing.T) {
	assertSliceEqual(t, []int{10, 30}, PipeMapErr(FromSlice([]int{1, 0, 3}),
		func(n int) (int, error) {
			if n == 0 {
				return 0, errors.New("zero")
			}
			return n * 10, nil
		}).Collect())
}

func TestMapErrHookOnlySkipsAndNotifies(t *testing.T) {
	var hookErrs []string
	result := PipeMapErr(
		FromSlice([]int{1, 0, 3}).
			WithErrorHook(func(err error, v int) { hookErrs = append(hookErrs, err.Error()) }),
		func(n int) (int, error) {
			if n == 0 {
				return 0, errors.New("zero")
			}
			return n, nil
		}).Collect()
	assertSliceEqual(t, []int{1, 3}, result)
	if len(hookErrs) != 1 || hookErrs[0] != "zero" {
		t.Errorf("unexpected: %v", hookErrs)
	}
}

// ===========================================================================
// Finalize safety (review #5)
// ===========================================================================

// finalize() uses a plain bool p.done without synchronization.
// Run with -race to detect the data race. The slow completion hook
// widens the race window so multiple goroutines enter the body.
func TestFinalizeConcurrentSafe(t *testing.T) {
	var completions atomic.Int32
	p := FromSlice([]int{1, 2, 3, 4, 5}).
		WithCompletionHook(func() {
			time.Sleep(time.Millisecond)
			completions.Add(1)
		})
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); p.finalize() }()
	}
	wg.Wait()
	if got := completions.Load(); got != 1 {
		t.Errorf("completion fired %d times, expected 1 (race on p.done)", got)
	}
}

func TestFinalizeDoubleCollect(t *testing.T) {
	var completions atomic.Int32
	p := FromSlice([]int{1, 2, 3}).
		WithCompletionHook(func() { completions.Add(1) })
	r1 := p.Collect()
	r2 := p.Collect()
	assertSliceEqual(t, []int{1, 2, 3}, r1)
	_ = r2 // second collect sees exhausted source
	if completions.Load() > 1 {
		t.Errorf("completion fired %d times", completions.Load())
	}
}

// ===========================================================================
// Timeout + hooks
// ===========================================================================

func TestTimeoutFiresTimeoutHook(t *testing.T) {
	var fired atomic.Int32
	ch := make(chan int)
	go func() {
		for i := 0; ; i++ {
			ch <- i
			time.Sleep(5 * time.Millisecond)
		}
	}()
	p := FromChannel(ch).
		WithTimeout(30 * time.Millisecond).
		WithTimeoutHook(func(d time.Duration) { fired.Add(1) })
	_ = p.Collect()
	if fired.Load() != 1 {
		t.Errorf("timeout hook fired %d times", fired.Load())
	}
	if !errors.Is(p.Err(), context.DeadlineExceeded) {
		t.Errorf("expected DeadlineExceeded, got %v", p.Err())
	}
}

func TestTimeoutCompletionStillFires(t *testing.T) {
	var completed atomic.Int32
	p := FromSlice([]int{1, 2, 3}).
		WithTimeout(5 * time.Second).
		WithCompletionHook(func() { completed.Add(1) })
	assertSliceEqual(t, []int{1, 2, 3}, p.Collect())
	if completed.Load() != 1 {
		t.Error("completion not fired")
	}
}

// ===========================================================================
// Pipeline lifecycle
// ===========================================================================

func TestErrNilBeforeTerminal(t *testing.T) {
	if FromSlice([]int{1}).Err() != nil {
		t.Error("should be nil")
	}
}

func TestWithContextNil(t *testing.T) {
	assertSliceEqual(t, []int{1, 2, 3},
		FromSlice([]int{1, 2, 3}).WithContext(nil).Collect())
}
