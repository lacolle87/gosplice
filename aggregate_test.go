package gosplice

import (
	"context"
	"testing"
)

func TestMaxBy_Empty(t *testing.T) {
	_, ok := MaxBy(FromSlice([]int{}), func(n int) int { return n })
	if ok {
		t.Fatal("expected ok=false for empty pipeline")
	}
}

func TestMinBy_Empty(t *testing.T) {
	_, ok := MinBy(FromSlice([]int{}), func(n int) int { return n })
	if ok {
		t.Fatal("expected ok=false for empty pipeline")
	}
}

func TestPartition_Empty(t *testing.T) {
	m, u := Partition(FromSlice([]int{}), func(n int) bool { return n > 0 })
	if len(m) != 0 || len(u) != 0 {
		t.Fatal("expected both empty")
	}
}

func TestSumBy_NonSlice(t *testing.T) {
	// channel source — exercises the fold fallback in SumBy
	ch := make(chan int, 3)
	ch <- 10
	ch <- 20
	ch <- 30
	close(ch)
	total := SumBy(FromChannel(ch), func(n int) int { return n })
	if total != 60 {
		t.Fatalf("expected 60, got %d", total)
	}
}

func TestSumBy_WithCtx(t *testing.T) {
	ctx := context.Background()
	total := SumBy(FromSlice([]int{1, 2, 3}).WithContext(ctx), func(n int) int { return n })
	if total != 6 {
		t.Fatalf("expected 6, got %d", total)
	}
}

func TestGroupBy_Empty(t *testing.T) {
	result := GroupBy(FromSlice([]string{}), func(s string) string { return s })
	if len(result) != 0 {
		t.Fatal("expected empty map")
	}
}

func TestCountBy_Empty(t *testing.T) {
	result := CountBy(FromSlice([]int{}), func(n int) int { return n })
	if len(result) != 0 {
		t.Fatal("expected empty map")
	}
}
