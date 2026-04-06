package gosplice

import (
	"context"
	"io"
	"strings"
	"testing"
)

func TestFuncSourceExhausted(t *testing.T) {
	called := 0
	assertSliceEqual(t, []int{1, 2}, FromFunc(func() (int, bool) {
		called++
		if called <= 2 {
			return called, true
		}
		return 0, false
	}).Collect())
}

func TestRangeSourceStartExceedsEnd(t *testing.T) {
	if len(FromRange(5, 3).Collect()) != 0 {
		t.Error("expected empty")
	}
}

func TestRangeSourceLarge(t *testing.T) {
	if FromRange(0, 10000).Count() != 10000 {
		t.Error("expected 10000")
	}
}

func TestChanCtxSourcePreCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if len(FromChannelCtx(ctx, make(chan int)).Collect()) != 0 {
		t.Error("expected empty")
	}
}

func TestChanCtxSourceClosedNormally(t *testing.T) {
	ch := make(chan int, 3)
	ch <- 1
	ch <- 2
	ch <- 3
	close(ch)
	assertSliceEqual(t, []int{1, 2, 3}, FromChannelCtx(context.Background(), ch).Collect())
}

func TestSizeHintSlicePartial(t *testing.T) {
	ss := &sliceSource[int]{data: []int{1, 2, 3, 4, 5}, idx: 2}
	if ss.SizeHint() != 3 {
		t.Errorf("expected 3, got %d", ss.SizeHint())
	}
}

func TestSizeHintRange(t *testing.T) {
	if (&rangeSource{cur: 5, end: 10}).SizeHint() != 5 {
		t.Error("expected 5")
	}
}

func TestSizeHintRangeExhausted(t *testing.T) {
	if (&rangeSource{cur: 10, end: 5}).SizeHint() != 0 {
		t.Error("expected 0")
	}
}

func TestSizeHintTake(t *testing.T) {
	ts := &takeSource[int]{inner: &sliceSource[int]{data: []int{1, 2, 3, 4, 5}}, n: 3}
	if ts.SizeHint() != 3 {
		t.Errorf("expected 3, got %d", ts.SizeHint())
	}
}

func TestSizeHintTakeInnerSmaller(t *testing.T) {
	ts := &takeSource[int]{inner: &sliceSource[int]{data: []int{1, 2}}, n: 10}
	if ts.SizeHint() != 2 {
		t.Errorf("expected 2, got %d", ts.SizeHint())
	}
}

func TestDrainSourceCtxNilCtx(t *testing.T) {
	ss := &sliceSource[int]{data: []int{1, 2, 3}}
	items, cancelled := drainSourceCtx[int](ss, nil)
	if cancelled {
		t.Error("should not be cancelled")
	}
	assertSliceEqual(t, []int{1, 2, 3}, items)
}

func TestDrainSourceCtxBackground(t *testing.T) {
	items, cancelled := drainSourceCtx[int](&sliceSource[int]{data: []int{1, 2, 3}}, context.Background())
	if cancelled {
		t.Error("should not be cancelled")
	}
	assertSliceEqual(t, []int{1, 2, 3}, items)
}

func TestDrainSourceCtxFuncSourcePreCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	i := 0
	items, cancelled := drainSourceCtx[int](&funcSource[int]{fn: func() (int, bool) { i++; return i, true }}, ctx)
	if !cancelled {
		t.Error("should be cancelled")
	}
	if len(items) > 1 {
		t.Logf("drained %d items before cancel", len(items))
	}
}

type errorReader struct {
	lines []string
	idx   int
}

func (r *errorReader) Read(p []byte) (int, error) {
	if r.idx >= len(r.lines) {
		return 0, io.EOF
	}
	s := r.lines[r.idx] + "\n"
	r.idx++
	n := copy(p, s)
	return n, nil
}

func TestFromReader_Basic(t *testing.T) {
	r := strings.NewReader("hello\nworld\n")
	result := FromReader(r).Collect()
	assertSliceEqual(t, []string{"hello", "world"}, result)
}

func TestFromReader_Empty(t *testing.T) {
	r := strings.NewReader("")
	result := FromReader(r).Collect()
	if len(result) != 0 {
		t.Fatalf("expected empty, got %v", result)
	}
}

func TestFromReader_Err(t *testing.T) {
	// bufio.Scanner with a line that exceeds max token size
	// Instead, test normal completion — Err() should be nil
	r := strings.NewReader("a\nb\n")
	p := FromReader(r)
	_ = p.Collect()
	if p.Err() != nil {
		t.Fatalf("unexpected error: %v", p.Err())
	}
}

func TestDrainSource_SizerPath(t *testing.T) {
	// rangeSource implements Sizer — exercises the Sizer branch in drainSource
	p := FromRange(0, 5)
	// Use parallel which calls drainSource internally
	result := PipeMapParallel(p, 2, func(n int) int { return n * 10 }).Collect()
	assertSliceEqual(t, []int{0, 10, 20, 30, 40}, result)
}

func TestDrainSource_GenericFallback(t *testing.T) {
	// funcSource has no Sizer — exercises the generic fallback in drainSource
	i := 0
	p := FromFunc(func() (int, bool) {
		if i >= 3 {
			return 0, false
		}
		i++
		return i, true
	})
	result := PipeMapParallel(p, 2, func(n int) int { return n * 10 }).Collect()
	assertSliceEqual(t, []int{10, 20, 30}, result)
}

func TestDrainSourceCtx_SizerNonSlice(t *testing.T) {
	// rangeSource + cancelable ctx — exercises Sizer+ctx path in drainSourceCtx
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := PipeMapParallel(
		FromRange(0, 5).WithContext(ctx), 2,
		func(n int) int { return n * 2 },
	).Collect()
	assertSliceEqual(t, []int{0, 2, 4, 6, 8}, result)
}
