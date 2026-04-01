package gosplice

import (
	"fmt"
	"io"
	"sync/atomic"
	"time"
)

func RetryHandler[T any](maxAttempts int, backoff time.Duration) ErrorHandler[T] {
	return func(err error, elem T, attempt int) ErrorAction {
		if attempt >= maxAttempts {
			return Skip
		}
		if backoff > 0 {
			time.Sleep(backoff * time.Duration(attempt+1))
		}
		return Retry
	}
}

func AbortOnError[T any]() ErrorHandler[T] {
	return func(err error, elem T, attempt int) ErrorAction {
		return Abort
	}
}

func SkipOnError[T any]() ErrorHandler[T] {
	return func(err error, elem T, attempt int) ErrorAction {
		return Skip
	}
}

func RetryThenAbort[T any](maxAttempts int, backoff time.Duration) ErrorHandler[T] {
	return func(err error, elem T, attempt int) ErrorAction {
		if attempt >= maxAttempts {
			return Abort
		}
		if backoff > 0 {
			time.Sleep(backoff * time.Duration(attempt+1))
		}
		return Retry
	}
}

func LogErrorsTo[T any](w io.Writer) ErrorHook[T] {
	return func(err error, elem T) {
		fmt.Fprintf(w, "error processing %v: %v\n", elem, err)
	}
}

func CountElements[T any](counter *atomic.Int64) ElementHook[T] {
	return func(v T) {
		counter.Add(1)
	}
}

func CountErrors[T any](counter *atomic.Int64) ErrorHook[T] {
	return func(err error, elem T) {
		counter.Add(1)
	}
}

func CountBatches[T any](counter *atomic.Int64) BatchHook[T] {
	return func(batch []T) {
		counter.Add(1)
	}
}

func CollectErrors[T any](errs *[]error) ErrorHook[T] {
	return func(err error, elem T) {
		*errs = append(*errs, err)
	}
}
