package gosplice

import (
	"sync/atomic"
	"testing"
	"time"
)

// ===========================================================================
// Window step<=0 (review #10)
// ===========================================================================

// step=0: the inner loop `for i := 0; i < step; i++` doesn't advance,
// so the same window is returned forever. After fix: step<=0 should
// either panic or be clamped to 1.

func TestWindowStepZeroMustNotProduceDuplicates(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			return // panic is acceptable fix
		}
	}()
	result := PipeWindow(FromSlice([]int{1, 2, 3, 4, 5}), 3, 0).Take(3).Collect()
	// Without fix: all 3 windows are [1,2,3] — degenerate.
	// After fix (clamp to 1): [1,2,3], [2,3,4], [3,4,5].
	if len(result) >= 2 {
		for i := 1; i < len(result); i++ {
			differs := false
			for j := range result[i] {
				if result[i][j] != result[0][j] {
					differs = true
					break
				}
			}
			if !differs {
				t.Errorf("PipeWindow(step=0): window[%d] is identical to window[0] — windows not advancing", i)
				break
			}
		}
	}
}

func TestWindowStepNegativeMustNotProduceDuplicates(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			return // panic is acceptable fix
		}
	}()
	result := PipeWindow(FromSlice([]int{1, 2, 3, 4, 5}), 3, -1).Take(3).Collect()
	if len(result) >= 2 {
		for i := 1; i < len(result); i++ {
			differs := false
			for j := range result[i] {
				if result[i][j] != result[0][j] {
					differs = true
					break
				}
			}
			if !differs {
				t.Errorf("PipeWindow(step=-1): window[%d] identical to window[0] — windows not advancing", i)
				break
			}
		}
	}
}

// ===========================================================================
// Window
// ===========================================================================

func TestWindowSliding(t *testing.T) {
	w := PipeWindow(FromSlice([]int{1, 2, 3, 4}), 3, 1).Collect()
	if len(w) != 2 {
		t.Fatalf("expected 2, got %d", len(w))
	}
	assertSliceEqual(t, []int{1, 2, 3}, w[0])
	assertSliceEqual(t, []int{2, 3, 4}, w[1])
}

func TestWindowNonOverlapping(t *testing.T) {
	w := PipeWindow(FromSlice([]int{1, 2, 3, 4, 5, 6}), 3, 3).Collect()
	if len(w) != 2 {
		t.Fatalf("expected 2, got %d", len(w))
	}
	assertSliceEqual(t, []int{1, 2, 3}, w[0])
	assertSliceEqual(t, []int{4, 5, 6}, w[1])
}

func TestWindowWithHooks(t *testing.T) {
	var count atomic.Int64
	w := PipeWindow(
		FromSlice([]int{1, 2, 3, 4, 5}).WithElementHook(CountElements[int](&count)),
		3, 1).Collect()
	if len(w) < 1 {
		t.Fatal("expected >=1 window")
	}
	if count.Load() != 5 {
		t.Errorf("expected 5, got %d", count.Load())
	}
}

// ===========================================================================
// Chunk
// ===========================================================================

func TestChunkExact(t *testing.T) {
	c := PipeChunk(FromSlice([]int{1, 2, 3, 4, 5, 6}), 2).Collect()
	if len(c) != 3 {
		t.Fatalf("expected 3, got %d", len(c))
	}
	assertSliceEqual(t, []int{1, 2}, c[0])
	assertSliceEqual(t, []int{5, 6}, c[2])
}

func TestChunkRemainder(t *testing.T) {
	c := PipeChunk(FromSlice([]int{1, 2, 3, 4, 5}), 3).Collect()
	if len(c) != 2 {
		t.Fatalf("expected 2, got %d", len(c))
	}
	assertSliceEqual(t, []int{4, 5}, c[1])
}

func TestChunkEmpty(t *testing.T) {
	if len(PipeChunk(FromSlice([]int{}), 5).Collect()) != 0 {
		t.Error("expected empty")
	}
}

func TestChunkSizeOne(t *testing.T) {
	if len(PipeChunk(FromSlice([]int{1, 2, 3}), 1).Collect()) != 3 {
		t.Error("expected 3")
	}
}

// ===========================================================================
// Distinct
// ===========================================================================

func TestDistinctEmpty(t *testing.T) {
	if len(PipeDistinct(FromSlice([]int{})).Collect()) != 0 {
		t.Error("expected empty")
	}
}

func TestDistinctAllSame(t *testing.T) {
	assertSliceEqual(t, []int{5}, PipeDistinct(FromSlice([]int{5, 5, 5, 5})).Collect())
}

func TestDistinctPreservesFirstOccurrence(t *testing.T) {
	assertSliceEqual(t, []int{3, 1, 2}, PipeDistinct(FromSlice([]int{3, 1, 2, 1, 3, 2})).Collect())
}

// ===========================================================================
// FlatMap
// ===========================================================================

func TestFlatMapAllEmpty(t *testing.T) {
	if len(PipeFlatMap(FromSlice([]int{1, 2, 3}),
		func(n int) []int { return nil }).Collect()) != 0 {
		t.Error("expected empty")
	}
}

func TestFlatMapMixed(t *testing.T) {
	assertSliceEqual(t, []int{1, 10, 3, 30}, PipeFlatMap(FromSlice([]int{1, 2, 3}),
		func(n int) []int {
			if n == 2 {
				return nil
			}
			return []int{n, n * 10}
		}).Collect())
}

// ===========================================================================
// Batch
// ===========================================================================

func TestBatchEmpty(t *testing.T) {
	if len(PipeBatch(FromSlice([]int{}), BatchConfig{Size: 5}).Collect()) != 0 {
		t.Error("expected empty")
	}
}

func TestBatchTimeoutEmitsPartial(t *testing.T) {
	ch := make(chan int)
	go func() {
		ch <- 1
		ch <- 2
		time.Sleep(200 * time.Millisecond)
		ch <- 3
		ch <- 4
		close(ch)
	}()
	batches := PipeBatch(FromChannel(ch), BatchConfig{Size: 10, MaxWait: 50 * time.Millisecond}).Collect()
	total := 0
	for _, b := range batches {
		total += len(b)
	}
	if total != 4 {
		t.Errorf("expected 4, got %d", total)
	}
}

// ===========================================================================
// collectAll fast paths
// ===========================================================================

func TestCollectAllFilter(t *testing.T) {
	assertSliceEqual(t, []int{4, 5}, FromSlice([]int{1, 2, 3, 4, 5}).
		Filter(func(n int) bool { return n > 3 }).Collect())
}

func TestCollectAllMap(t *testing.T) {
	assertSliceEqual(t, []int{2, 4, 6},
		PipeMap(FromSlice([]int{1, 2, 3}), func(n int) int { return n * 2 }).Collect())
}

func TestCollectAllChunk(t *testing.T) {
	if len(PipeChunk(FromSlice([]int{1, 2, 3, 4}), 2).Collect()) != 2 {
		t.Error("expected 2")
	}
}

func TestCollectAllWindow(t *testing.T) {
	if len(PipeWindow(FromSlice([]int{1, 2, 3, 4, 5}), 3, 1).Collect()) != 3 {
		t.Error("expected 3")
	}
}

// ===========================================================================
// Pipeline ops edge cases
// ===========================================================================

func TestSkipZero(t *testing.T) {
	assertSliceEqual(t, []int{1, 2, 3}, FromSlice([]int{1, 2, 3}).Skip(0).Collect())
}

func TestTakeZero(t *testing.T) {
	if len(FromSlice([]int{1, 2, 3}).Take(0).Collect()) != 0 {
		t.Error("expected empty")
	}
}

func TestPeekSideEffect(t *testing.T) {
	var sum int
	assertSliceEqual(t, []int{1, 2, 3},
		FromSlice([]int{1, 2, 3}).Peek(func(n int) { sum += n }).Collect())
	if sum != 6 {
		t.Errorf("expected 6, got %d", sum)
	}
}
