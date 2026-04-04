package gosplice

import (
	"context"
	"errors"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

// ===========================================================================
// Goroutine leak — no context (review #1 + dr_test.go Fix 2)
// ===========================================================================

// These tests do NOT call runtime.GC(). Before the fix, cleanup depends on
// the GC finalizer (SetFinalizer on stoppableSource). After the fix
// (r.cancel = mergedCancel), finalize() triggers mergedCancel directly and
// goroutines exit without GC. Without GC the finalizer never fires, so
// leaked goroutines remain — making the test RED on unfixed code.

func TestParallelStreamNoLeakOnTake(t *testing.T) {
	before := runtime.NumGoroutine()
	result := PipeMapParallelStream(
		FromFunc(func() (int, bool) {
			time.Sleep(time.Millisecond)
			return 1, true
		}), 4, 2, func(n int) int { return n * 10 },
	).Take(3).Collect()

	if len(result) != 3 {
		t.Fatalf("expected 3, got %d", len(result))
	}
	// NO runtime.GC() — cleanup must happen via cancel, not finalizer.
	time.Sleep(200 * time.Millisecond)
	if leaked := runtime.NumGoroutine() - before; leaked > 3 {
		t.Errorf("goroutine leak (Take, no GC): before=%d delta=%d", before, leaked)
	}
}

func TestParallelStreamNoLeakOnFirst(t *testing.T) {
	before := runtime.NumGoroutine()
	v, ok := PipeMapParallelStream(
		FromFunc(func() (int, bool) {
			time.Sleep(time.Millisecond)
			return 42, true
		}), 2, 4, func(n int) int { return n + 1 },
	).First()

	if !ok || v != 43 {
		t.Fatalf("expected (43,true), got (%d,%v)", v, ok)
	}
	time.Sleep(200 * time.Millisecond)
	if leaked := runtime.NumGoroutine() - before; leaked > 3 {
		t.Errorf("goroutine leak (First, no GC): before=%d delta=%d", before, leaked)
	}
}

func TestParallelStreamNoLeakPartialForEach(t *testing.T) {
	before := runtime.NumGoroutine()
	got := 0
	PipeMapParallelStream(
		FromFunc(func() (int, bool) {
			time.Sleep(time.Millisecond)
			return 42, true
		}), 2, 4, func(n int) int { return n },
	).Take(5).ForEach(func(v int) { got++ })

	if got != 5 {
		t.Fatalf("expected 5, got %d", got)
	}
	time.Sleep(200 * time.Millisecond)
	if leaked := runtime.NumGoroutine() - before; leaked > 3 {
		t.Errorf("goroutine leak (partial ForEach, no GC): before=%d delta=%d", before, leaked)
	}
}

func TestParallelStreamDoneUnblocksWorkers(t *testing.T) {
	ch := make(chan int, 100)
	for i := 1; i <= 50; i++ {
		ch <- i
	}
	close(ch)
	before := runtime.NumGoroutine()

	result := PipeMapParallelStream(FromChannel(ch), 4, 1, func(n int) int {
		return n * 2
	}).Take(2).Collect()

	if len(result) != 2 {
		t.Fatalf("expected 2, got %d", len(result))
	}
	runtime.GC()
	time.Sleep(300 * time.Millisecond)
	runtime.GC()
	time.Sleep(100 * time.Millisecond)
	if leaked := runtime.NumGoroutine() - before; leaked > 3 {
		t.Errorf("goroutine leak (done unblock): before=%d delta=%d", before, leaked)
	}
}

// ===========================================================================
// Context propagation through parallelResult (review #2)
// ===========================================================================

func TestParallelResultCtxPropagatesMap(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	q := PipeMapParallel(FromSlice([]int{1, 2, 3, 4, 5}).WithContext(ctx), 2,
		func(n int) int { return n * 2 })
	cancel()
	_ = q.Collect()
	if q.Err() == nil {
		t.Errorf("ctx not propagated through parallelResult: Err() is nil, expected context.Canceled")
	}
}

func TestParallelResultCtxPropagatesFilter(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	q := PipeFilterParallel(FromSlice([]int{1, 2, 3, 4, 5}).WithContext(ctx), 2,
		func(n int) bool { return n%2 == 0 })
	cancel()
	_ = q.Collect()
	if q.Err() == nil {
		t.Errorf("ctx not propagated through FilterParallel: Err() is nil, expected context.Canceled")
	}
}

func TestParallelResultCtxPropagatesMapErr(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	q := PipeMapParallelErr(FromSlice([]int{1, 2, 3}).WithContext(ctx), 2,
		func(n int) (int, error) { return n, nil })
	cancel()
	_ = q.Collect()
	if q.Err() == nil {
		t.Errorf("ctx not propagated through MapParallelErr: Err() is nil, expected context.Canceled")
	}
}

func TestParallelResultTimeoutStopsDownstream(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	data := make([]int, 10000)
	for i := range data {
		data[i] = i
	}
	var count atomic.Int64
	PipeMapParallel(FromSlice(data).WithContext(ctx), 4,
		func(n int) int { return n }).
		ForEach(func(n int) {
			count.Add(1)
			time.Sleep(10 * time.Microsecond)
		})
	// After fix: timeout ctx propagates, ForEach stops early.
	if count.Load() >= int64(len(data)) {
		t.Errorf("timeout did not stop downstream: all %d elements processed", count.Load())
	}
}

// ===========================================================================
// Order preservation and normal completion (from dr_test.go)
// ===========================================================================

func TestParallelStreamOrderPreserved(t *testing.T) {
	assertSliceEqual(t, []int{10, 20, 30, 40, 50, 60, 70, 80, 90, 100},
		PipeMapParallelStream(FromSlice([]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}), 4, 16,
			func(n int) int { return n * 10 }).Collect())
}

func TestParallelStreamNormalCompletion(t *testing.T) {
	before := runtime.NumGoroutine()
	assertSliceEqual(t, []int{3, 6, 9, 12, 15},
		PipeMapParallelStream(FromSlice([]int{1, 2, 3, 4, 5}), 2, 8,
			func(n int) int { return n * 3 }).Collect())
	time.Sleep(50 * time.Millisecond)
	if leaked := runtime.NumGoroutine() - before; leaked > 2 {
		t.Errorf("goroutines not cleaned up: delta=%d", leaked)
	}
}

// ===========================================================================
// stoppableSource (from dr_test.go)
// ===========================================================================

func TestStoppableSourceStopIdempotent(t *testing.T) {
	ch := make(chan int)
	close(ch)
	s := &stoppableSource[int]{ch: ch, done: make(chan struct{})}
	s.stop()
	s.stop()
	s.stop()
	select {
	case <-s.done:
	default:
		t.Error("done channel should be closed")
	}
}

func TestStoppableSourceCancelledCtx(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ss := &stoppableSource[int]{
		ch: make(chan int, 1), done: make(chan struct{}),
		ctx: ctx, cancelFn: cancel,
	}
	if _, ok := ss.Next(); ok {
		t.Error("expected ok=false with cancelled ctx")
	}
}

// ===========================================================================
// Edge cases
// ===========================================================================

func TestParallelStreamEmpty(t *testing.T) {
	if len(PipeMapParallelStream(FromSlice([]int{}), 4, 8,
		func(n int) int { return n }).Collect()) != 0 {
		t.Error("expected empty")
	}
}

func TestParallelStreamSingleElement(t *testing.T) {
	assertSliceEqual(t, []int{84},
		PipeMapParallelStream(FromSlice([]int{42}), 4, 8,
			func(n int) int { return n * 2 }).Collect())
}

func TestParallelStreamWorkersExceedElements(t *testing.T) {
	assertSliceEqual(t, []int{101, 102},
		PipeMapParallelStream(FromSlice([]int{1, 2}), 16, 32,
			func(n int) int { return n + 100 }).Collect())
}

func TestParallelStreamBufferSize1(t *testing.T) {
	assertSliceEqual(t, []int{3, 6, 9, 12, 15},
		PipeMapParallelStream(FromSlice([]int{1, 2, 3, 4, 5}), 2, 1,
			func(n int) int { return n * 3 }).Collect())
}

func TestParallelStreamWithHooks(t *testing.T) {
	var count atomic.Int64
	result := PipeMapParallelStream(
		FromSlice([]int{1, 2, 3}).WithElementHook(CountElements[int](&count)),
		2, 8, func(n int) int { return n * 10 }).Collect()
	assertSliceEqual(t, []int{10, 20, 30}, result)
	if count.Load() != 3 {
		t.Errorf("hook count: expected 3, got %d", count.Load())
	}
}

func TestMapParallelSingleWorker(t *testing.T) {
	assertSliceEqual(t, []int{5, 10, 15},
		PipeMapParallel(FromSlice([]int{1, 2, 3}), 1,
			func(n int) int { return n * 5 }).Collect())
}

func TestMapParallelWorkersExceedElements(t *testing.T) {
	assertSliceEqual(t, []int{1, 2},
		PipeMapParallel(FromSlice([]int{1, 2}), 100,
			func(n int) int { return n }).Collect())
}

func TestFilterParallelAllFiltered(t *testing.T) {
	if len(PipeFilterParallel(FromSlice([]int{1, 2, 3}), 4,
		func(n int) bool { return false }).Collect()) != 0 {
		t.Error("expected empty")
	}
}

func TestFilterParallelNoneFiltered(t *testing.T) {
	assertSliceEqual(t, []int{1, 2, 3},
		PipeFilterParallel(FromSlice([]int{1, 2, 3}), 4,
			func(n int) bool { return true }).Collect())
}

func TestMapParallelErrAllErrors(t *testing.T) {
	var errCount atomic.Int32
	result := PipeMapParallelErr(
		FromSlice([]int{1, 2, 3}).WithErrorHook(func(err error, v int) { errCount.Add(1) }),
		2, func(n int) (int, error) { return 0, errors.New("fail") }).Collect()
	if len(result) != 0 {
		t.Errorf("expected empty, got %v", result)
	}
	if errCount.Load() != 3 {
		t.Errorf("expected 3 errors, got %d", errCount.Load())
	}
}

func TestMapParallelErrMixed(t *testing.T) {
	assertSliceEqual(t, []int{10, 30, 50},
		PipeMapParallelErr(FromSlice([]int{1, 2, 3, 4, 5}), 2,
			func(n int) (int, error) {
				if n%2 == 0 {
					return 0, errors.New("even")
				}
				return n * 10, nil
			}).Collect())
}
