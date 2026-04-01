package gosplice

import (
	"bytes"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
)

func TestToChannel(t *testing.T) {
	ch := make(chan int, 10)
	go FromSlice([]int{1, 2, 3, 4, 5}).
		Filter(func(n int) bool { return n%2 == 0 }).
		ToChannel(ch)

	var got []int
	for v := range ch {
		got = append(got, v)
	}
	assertSliceEqual(t, []int{2, 4}, got)
}

func TestToChannelEmpty(t *testing.T) {
	ch := make(chan int, 10)
	go FromSlice([]int{}).ToChannel(ch)

	var got []int
	for v := range ch {
		got = append(got, v)
	}
	if len(got) != 0 {
		t.Errorf("expected empty, got %v", got)
	}
}

func TestToChannelClosesOnDone(t *testing.T) {
	ch := make(chan int, 10)
	go FromSlice([]int{1, 2, 3}).ToChannel(ch)

	count := 0
	for range ch {
		count++
	}
	if count != 3 {
		t.Errorf("expected 3, got %d", count)
	}
}

func TestToWriter(t *testing.T) {
	var buf bytes.Buffer
	err := ToWriter(
		FromSlice([]string{"hello", "world"}),
		&buf,
		func(s string) []byte { return []byte(s + "\n") },
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if buf.String() != "hello\nworld\n" {
		t.Errorf("expected 'hello\\nworld\\n', got %q", buf.String())
	}
}

func TestToWriterString(t *testing.T) {
	var buf bytes.Buffer
	err := ToWriterString(
		FromSlice([]int{1, 2, 3}),
		&buf,
		func(n int) string { return fmt.Sprintf("%d,", n) },
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if buf.String() != "1,2,3," {
		t.Errorf("expected '1,2,3,', got %q", buf.String())
	}
}

func TestToWriterWithPipeline(t *testing.T) {
	var buf bytes.Buffer
	err := ToWriterString(
		FromSlice([]int{1, 2, 3, 4, 5}).Filter(func(n int) bool { return n > 3 }),
		&buf,
		func(n int) string { return fmt.Sprintf("%d ", n) },
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if buf.String() != "4 5 " {
		t.Errorf("expected '4 5 ', got %q", buf.String())
	}
}

func TestToChannelWithHooks(t *testing.T) {
	completed := false
	ch := make(chan int, 10)
	go FromSlice([]int{1, 2, 3}).
		WithCompletionHook(func() { completed = true }).
		ToChannel(ch)

	for range ch {
	}
	if !completed {
		t.Error("completion hook not fired")
	}
}

func TestErrorHandlerSkip(t *testing.T) {
	skipped := int32(0)
	p := FromSlice([]int{1, 0, 3, 0, 5}).
		WithErrorHandler(func(err error, elem int, attempt int) ErrorAction {
			atomic.AddInt32(&skipped, 1)
			return Skip
		})

	result := PipeMapErr(p, func(n int) (int, error) {
		if n == 0 {
			return 0, errors.New("zero")
		}
		return n * 10, nil
	}).Collect()

	assertSliceEqual(t, []int{10, 30, 50}, result)
	if atomic.LoadInt32(&skipped) != 2 {
		t.Errorf("expected 2, got %d", skipped)
	}
}

func TestErrorHandlerAbort(t *testing.T) {
	p := FromSlice([]int{1, 2, 0, 4, 5}).
		WithErrorHandler(AbortOnError[int]())

	result := PipeMapErr(p, func(n int) (int, error) {
		if n == 0 {
			return 0, errors.New("zero")
		}
		return n * 10, nil
	}).Collect()

	assertSliceEqual(t, []int{10, 20}, result)
}

func TestErrorHandlerRetry(t *testing.T) {
	callCount := int32(0)
	p := FromSlice([]int{1, 2, 3}).
		WithErrorHandler(RetryHandler[int](3, 0)).
		WithMaxRetries(5)

	result := PipeMapErr(p, func(n int) (int, error) {
		if n == 2 {
			c := atomic.AddInt32(&callCount, 1)
			if c <= 2 {
				return 0, errors.New("transient")
			}
		}
		return n * 10, nil
	}).Collect()

	assertSliceEqual(t, []int{10, 20, 30}, result)
}

func TestErrorHandlerRetryThenAbort(t *testing.T) {
	p := FromSlice([]int{1, 0, 3}).
		WithErrorHandler(RetryThenAbort[int](2, 0)).
		WithMaxRetries(5)

	result := PipeMapErr(p, func(n int) (int, error) {
		if n == 0 {
			return 0, errors.New("permanent")
		}
		return n * 10, nil
	}).Collect()

	assertSliceEqual(t, []int{10}, result)
}

func TestErrorHandlerSkipOnError(t *testing.T) {
	p := FromSlice([]int{1, 0, 3}).
		WithErrorHandler(SkipOnError[int]())

	result := PipeMapErr(p, func(n int) (int, error) {
		if n == 0 {
			return 0, errors.New("skip me")
		}
		return n * 10, nil
	}).Collect()

	assertSliceEqual(t, []int{10, 30}, result)
}

func TestLegacyErrorHookBackwardCompat(t *testing.T) {
	errCount := int32(0)
	p := FromSlice([]int{1, 0, 3}).
		WithErrorHook(func(err error, v int) { atomic.AddInt32(&errCount, 1) })

	result := PipeMapErr(p, func(n int) (int, error) {
		if n == 0 {
			return 0, errors.New("zero")
		}
		return n * 10, nil
	}).Collect()

	assertSliceEqual(t, []int{10, 30}, result)
	if atomic.LoadInt32(&errCount) != 1 {
		t.Errorf("expected 1, got %d", errCount)
	}
}

func TestErrorHandlerPrecedence(t *testing.T) {
	hookCalled := false
	handlerCalled := false

	p := FromSlice([]int{0}).
		WithErrorHook(func(err error, v int) { hookCalled = true }).
		WithErrorHandler(func(err error, v int, attempt int) ErrorAction {
			handlerCalled = true
			return Skip
		})

	PipeMapErr(p, func(n int) (int, error) {
		return 0, errors.New("err")
	}).Collect()

	if !handlerCalled {
		t.Error("handler should be called")
	}
	if hookCalled {
		t.Error("hook should NOT be called when handler is set")
	}
}

func TestCountElements(t *testing.T) {
	var counter atomic.Int64
	FromSlice([]int{1, 2, 3, 4, 5}).
		WithElementHook(CountElements[int](&counter)).
		Collect()
	if counter.Load() != 5 {
		t.Errorf("expected 5, got %d", counter.Load())
	}
}

func TestCountErrors(t *testing.T) {
	var counter atomic.Int64
	p := FromSlice([]int{1, 0, 3, 0}).
		WithErrorHook(CountErrors[int](&counter))
	PipeMapErr(p, func(n int) (int, error) {
		if n == 0 {
			return 0, errors.New("zero")
		}
		return n, nil
	}).Collect()
	if counter.Load() != 2 {
		t.Errorf("expected 2, got %d", counter.Load())
	}
}

func TestCollectErrors(t *testing.T) {
	var errs []error
	p := FromSlice([]int{1, 0, 3}).WithErrorHook(CollectErrors[int](&errs))
	PipeMapErr(p, func(n int) (int, error) {
		if n == 0 {
			return 0, errors.New("boom")
		}
		return n, nil
	}).Collect()
	if len(errs) != 1 || errs[0].Error() != "boom" {
		t.Errorf("unexpected: %v", errs)
	}
}

func TestLogErrorsTo(t *testing.T) {
	var buf bytes.Buffer
	p := FromSlice([]int{1, 0}).WithErrorHook(LogErrorsTo[int](&buf))
	PipeMapErr(p, func(n int) (int, error) {
		if n == 0 {
			return 0, errors.New("fail")
		}
		return n, nil
	}).Collect()
	if buf.Len() == 0 {
		t.Error("expected log output")
	}
}

func TestCountBatches(t *testing.T) {
	var counter atomic.Int64
	p := FromSlice([]int{1, 2, 3, 4, 5}).WithBatchHook(CountBatches[int](&counter))
	_ = PipeBatch(p, BatchConfig{Size: 2}).Collect()
	if counter.Load() != 3 {
		t.Errorf("expected 3, got %d", counter.Load())
	}
}

func TestWithMaxRetries(t *testing.T) {
	attempts := int32(0)
	p := FromSlice([]int{1}).
		WithErrorHandler(func(err error, v int, attempt int) ErrorAction { return Retry }).
		WithMaxRetries(5)

	PipeMapErr(p, func(n int) (int, error) {
		atomic.AddInt32(&attempts, 1)
		return 0, errors.New("always fail")
	}).Collect()

	got := atomic.LoadInt32(&attempts)
	if got < 2 || got > 6 {
		t.Errorf("expected attempts between 2 and 6, got %d", got)
	}
}
