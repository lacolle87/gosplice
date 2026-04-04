package gosplice

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// ===========================================================================
// foldWhile idx on early exit (review #3)
// ===========================================================================

func TestFoldWhileAnyIdxOnEarlyExit(t *testing.T) {
	ss := &sliceSource[int]{data: []int{10, 20, 30, 40, 50}}
	p := &Pipeline[int]{source: ss, hooks: newHooks[int]()}
	if !p.Any(func(n int) bool { return n == 30 }) {
		t.Fatal("expected match")
	}
	// After fix: consumed 10,20,30 → idx must be 3, not len(data).
	if ss.idx != 3 {
		t.Errorf("foldWhile Any early exit: idx=%d, expected 3", ss.idx)
	}
}

func TestFoldWhileAllIdxOnEarlyExit(t *testing.T) {
	ss := &sliceSource[int]{data: []int{1, 1, 0, 1, 1}}
	p := &Pipeline[int]{source: ss, hooks: newHooks[int]()}
	if p.All(func(n int) bool { return n > 0 }) {
		t.Fatal("expected false")
	}
	// After fix: consumed 1,1,0 → idx must be 3.
	if ss.idx != 3 {
		t.Errorf("foldWhile All early exit: idx=%d, expected 3", ss.idx)
	}
}

func TestFoldWhileCtxPathAnyIdxOnEarlyExit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ss := &sliceSource[int]{data: []int{10, 20, 30, 40, 50}}
	p := &Pipeline[int]{source: ss, hooks: newHooks[int](), ctx: ctx}
	if !p.Any(func(n int) bool { return n == 30 }) {
		t.Fatal("expected match")
	}
	if ss.idx != 3 {
		t.Errorf("foldWhile Any (ctx path) early exit: idx=%d, expected 3", ss.idx)
	}
}

func TestFoldWhileAnyNoMatchIdxAtEnd(t *testing.T) {
	ss := &sliceSource[int]{data: []int{1, 2, 3}}
	p := &Pipeline[int]{source: ss, hooks: newHooks[int]()}
	if p.Any(func(n int) bool { return n > 100 }) {
		t.Fatal("expected no match")
	}
	if ss.idx != len(ss.data) {
		t.Errorf("expected idx=%d, got %d", len(ss.data), ss.idx)
	}
}

func TestFoldWhileAllFullMatchIdxAtEnd(t *testing.T) {
	ss := &sliceSource[int]{data: []int{2, 4, 6}}
	p := &Pipeline[int]{source: ss, hooks: newHooks[int]()}
	if !p.All(func(n int) bool { return n%2 == 0 }) {
		t.Fatal("expected all even")
	}
	if ss.idx != len(ss.data) {
		t.Errorf("expected idx=%d, got %d", len(ss.data), ss.idx)
	}
}

// ===========================================================================
// PipeReduce
// ===========================================================================

func TestPipeReduceEmpty(t *testing.T) {
	if PipeReduce(FromSlice([]int{}), 42, func(a, b int) int { return a + b }) != 42 {
		t.Error("expected initial value")
	}
}

func TestPipeReduceSliceFastPath(t *testing.T) {
	if PipeReduce(FromSlice([]int{1, 2, 3}), 0, func(a, b int) int { return a + b }) != 6 {
		t.Error("expected 6")
	}
}

func TestPipeReduceWithCtx(t *testing.T) {
	got := PipeReduce(FromSlice([]int{1, 2, 3, 4}).WithContext(context.Background()),
		0, func(a, b int) int { return a + b })
	if got != 10 {
		t.Errorf("expected 10, got %d", got)
	}
}

func TestPipeReduceWithHooks(t *testing.T) {
	var count atomic.Int64
	got := PipeReduce(
		FromSlice([]int{1, 2, 3}).WithElementHook(CountElements[int](&count)),
		0, func(a, b int) int { return a + b })
	if got != 6 || count.Load() != 3 {
		t.Errorf("result=%d count=%d", got, count.Load())
	}
}

// ===========================================================================
// Chaining — cancel propagation through transforms
// ===========================================================================

func TestChainingFilterSkipTake(t *testing.T) {
	assertSliceEqual(t, []int{3, 4},
		FromSlice([]int{1, 2, 3, 4, 5}).WithTimeout(5*time.Second).
			Filter(func(n int) bool { return n > 1 }).Skip(1).Take(2).Collect())
}

func TestChainingPipeMapPipeFlatMap(t *testing.T) {
	assertSliceEqual(t, []int{2, 3, 4, 5, 6, 7},
		PipeFlatMap(
			PipeMap(FromSlice([]int{1, 2, 3}).WithTimeout(5*time.Second),
				func(n int) int { return n * 2 }),
			func(n int) []int { return []int{n, n + 1} }).Collect())
}
