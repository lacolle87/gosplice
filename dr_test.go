package gosplice

import (
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Fix 1: CollectErrors — data race on *[]error append
// ---------------------------------------------------------------------------

func TestCollectErrorsConcurrentSafe(t *testing.T) {
	// Simulate concurrent error hook invocations.
	// Under the old code this fails with -race.
	var errs []error
	hook := CollectErrors[int](&errs)

	var wg sync.WaitGroup
	const goroutines = 32
	const errorsPerG = 100

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

// ---------------------------------------------------------------------------
// Fix 2: PipeMapParallelStream — goroutine leak on early exit
// ---------------------------------------------------------------------------

func TestParallelStreamNoLeakOnTake(t *testing.T) {
	// Create a slow infinite source — each Next blocks briefly.
	calls := int64(0)
	p := FromFunc(func() (int, bool) {
		n := atomic.AddInt64(&calls, 1)
		time.Sleep(time.Millisecond)
		return int(n), true
	})

	before := runtime.NumGoroutine()

	// Consumer takes only 3 elements from an infinite stream.
	result := PipeMapParallelStream(p, 4, 8, func(n int) int {
		return n * 10
	}).Take(3).Collect()

	if len(result) != 3 {
		t.Fatalf("expected 3 results, got %d", len(result))
	}

	// Force GC so the finalizer on stoppableSource fires,
	// closing done and letting goroutines exit.
	runtime.GC()
	runtime.Gosched()
	time.Sleep(200 * time.Millisecond)
	runtime.GC()
	time.Sleep(100 * time.Millisecond)

	after := runtime.NumGoroutine()

	// Allow a margin — other goroutines may exist (test infra, GC, etc.).
	// The key invariant: we should not have leaked the worker pool.
	leaked := after - before
	if leaked > 3 {
		t.Errorf("probable goroutine leak: before=%d after=%d delta=%d", before, after, leaked)
	}
}

func TestParallelStreamNoLeakOnFirst(t *testing.T) {
	p := FromFunc(func() (int, bool) {
		time.Sleep(time.Millisecond)
		return 42, true
	})

	before := runtime.NumGoroutine()

	v, ok := PipeMapParallelStream(p, 2, 4, func(n int) int {
		return n + 1
	}).First()

	if !ok || v != 43 {
		t.Fatalf("expected (43, true), got (%d, %v)", v, ok)
	}

	runtime.GC()
	runtime.Gosched()
	time.Sleep(200 * time.Millisecond)
	runtime.GC()
	time.Sleep(100 * time.Millisecond)

	after := runtime.NumGoroutine()
	leaked := after - before
	if leaked > 3 {
		t.Errorf("probable goroutine leak: before=%d after=%d delta=%d", before, after, leaked)
	}
}

// ---------------------------------------------------------------------------
// Fix 2 cont.: done channel stops workers blocked on full outCh
// ---------------------------------------------------------------------------

func TestParallelStreamDoneUnblocksWorkers(t *testing.T) {
	// bufSize=1 means outCh fills immediately.
	// Without done-select, workers block forever on outCh <- r.
	ch := make(chan int, 100)
	for i := 1; i <= 50; i++ {
		ch <- i
	}
	close(ch)

	before := runtime.NumGoroutine()

	// Take only 2 from 50 elements with tiny buffer.
	result := PipeMapParallelStream(FromChannel(ch), 4, 1, func(n int) int {
		return n * 2
	}).Take(2).Collect()

	if len(result) != 2 {
		t.Fatalf("expected 2 results, got %d", len(result))
	}

	runtime.GC()
	runtime.Gosched()
	time.Sleep(200 * time.Millisecond)
	runtime.GC()
	time.Sleep(100 * time.Millisecond)

	after := runtime.NumGoroutine()
	leaked := after - before
	if leaked > 3 {
		t.Errorf("probable goroutine leak: before=%d after=%d delta=%d", before, after, leaked)
	}
}

// ---------------------------------------------------------------------------
// Regression: order preservation still works after the fix
// ---------------------------------------------------------------------------

func TestParallelStreamOrderPreservedAfterFix(t *testing.T) {
	result := PipeMapParallelStream(
		FromSlice([]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
		4, 16,
		func(n int) int { return n * 10 },
	).Collect()

	assertSliceEqual(t, []int{10, 20, 30, 40, 50, 60, 70, 80, 90, 100}, result)
}

// ---------------------------------------------------------------------------
// Regression: normal completion closes done, no goroutine lingering
// ---------------------------------------------------------------------------

func TestParallelStreamNormalCompletion(t *testing.T) {
	before := runtime.NumGoroutine()

	result := PipeMapParallelStream(
		FromSlice([]int{1, 2, 3, 4, 5}),
		2, 8,
		func(n int) int { return n * 3 },
	).Collect()

	assertSliceEqual(t, []int{3, 6, 9, 12, 15}, result)

	// No GC needed — normal completion drains the channel,
	// stoppableSource.Next gets ok=false, calls stop().
	time.Sleep(50 * time.Millisecond)

	after := runtime.NumGoroutine()
	leaked := after - before
	if leaked > 2 {
		t.Errorf("goroutines not cleaned up: before=%d after=%d delta=%d", before, after, leaked)
	}
}

// ---------------------------------------------------------------------------
// Fix 2 cont.: stoppableSource.stop is idempotent
// ---------------------------------------------------------------------------

func TestStoppableSourceStopIdempotent(t *testing.T) {
	ch := make(chan int)
	close(ch)

	s := &stoppableSource[int]{ch: ch, done: make(chan struct{})}

	// Multiple stops must not panic on double-close.
	s.stop()
	s.stop()
	s.stop()

	select {
	case <-s.done:
	default:
		t.Error("done channel should be closed")
	}
}

// ---------------------------------------------------------------------------
// CollectErrors still works correctly in sequential pipeline (regression)
// ---------------------------------------------------------------------------

func TestCollectErrorsSequentialRegression(t *testing.T) {
	var errs []error
	p := FromSlice([]int{1, 0, 3, 0, 5}).WithErrorHook(CollectErrors[int](&errs))
	result := PipeMapErr(p, func(n int) (int, error) {
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
