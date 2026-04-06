package gosplice

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestBatchTimeout_NoCtx(t *testing.T) {
	ch := make(chan int)
	go func() {
		ch <- 1
		ch <- 2
		time.Sleep(200 * time.Millisecond)
		ch <- 3
		close(ch)
	}()
	batches := PipeBatch(FromChannel(ch), BatchConfig{Size: 10, MaxWait: 50 * time.Millisecond}).Collect()
	total := 0
	for _, b := range batches {
		total += len(b)
	}
	if total != 3 {
		t.Fatalf("expected 3 total elements, got %d", total)
	}
}

func TestBatchTimeout_WithCtx_Cancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan int)
	go func() {
		for i := 0; ; i++ {
			select {
			case ch <- i:
				time.Sleep(5 * time.Millisecond)
			case <-ctx.Done():
				return
			}
		}
	}()
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	batches := PipeBatch(
		FromChannel(ch).WithContext(ctx),
		BatchConfig{Size: 100, MaxWait: 20 * time.Millisecond},
	).Collect()
	total := 0
	for _, b := range batches {
		total += len(b)
	}
	if total == 0 {
		t.Fatal("expected some elements before cancellation")
	}
}

func TestBatch_WithElementAndBatchHooks(t *testing.T) {
	var elemCount atomic.Int64
	var batchCount atomic.Int64
	batches := PipeBatch(
		FromSlice([]int{1, 2, 3, 4, 5}).
			WithElementHook(CountElements[int](&elemCount)).
			WithBatchHook(CountBatches[int](&batchCount)),
		BatchConfig{Size: 2},
	).Collect()
	if len(batches) != 3 {
		t.Fatalf("expected 3 batches, got %d", len(batches))
	}
	if elemCount.Load() != 5 {
		t.Fatalf("expected 5 element hooks, got %d", elemCount.Load())
	}
	if batchCount.Load() != 3 {
		t.Fatalf("expected 3 batch hooks, got %d", batchCount.Load())
	}
}
