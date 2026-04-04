package gosplice

import (
	"context"
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
