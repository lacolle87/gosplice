package gosplice

import "time"

type ErrorAction int

const (
	Skip  ErrorAction = iota // drop element, continue pipeline
	Retry                    // re-run the operation on same element
	Abort                    // stop pipeline gracefully
)

// ElementHook is called for each element that reaches a terminal's iteration loop.
type ElementHook[T any] func(T)

// ErrorHook is called when PipeMapErr encounters an error and no ErrorHandler is set.
// The element is always skipped after all error hooks fire.
type ErrorHook[T any] func(error, T)

// ErrorHandler decides the fate of a failed element: Skip, Retry, or Abort.
// When set, ErrorHandler takes precedence over ErrorHook — hooks are not called.
// The attempt parameter starts at 1 and increments on each Retry.
type ErrorHandler[T any] func(err error, elem T, attempt int) ErrorAction

// BatchHook is called when PipeBatch emits a complete batch.
type BatchHook[T any] func([]T)

// CompletionHook is called exactly once during pipeline finalization,
// after the terminal operation completes or the pipeline is cancelled.
type CompletionHook func()

// TimeoutHook is called during finalization if the pipeline finished
// with an error and Timeout > 0 was configured via WithTimeout.
type TimeoutHook func(time.Duration)

type Hooks[T any] struct {
	OnElement    []ElementHook[T]
	OnError      []ErrorHook[T]
	OnBatch      []BatchHook[T]
	OnCompletion []CompletionHook
	OnTimeout    []TimeoutHook
	ErrHandler   ErrorHandler[T]
	Timeout      time.Duration
	MaxRetries   int
}

func newHooks[T any]() *Hooks[T] {
	return &Hooks[T]{MaxRetries: 3}
}

func (h *Hooks[T]) hasElement() bool { return len(h.OnElement) > 0 }
func (h *Hooks[T]) hasError() bool   { return len(h.OnError) > 0 || h.ErrHandler != nil }

func (h *Hooks[T]) fireElement(v T) {
	for _, hook := range h.OnElement {
		hook(v)
	}
}

func (h *Hooks[T]) handleError(err error, v T, attempt int) ErrorAction {
	if h.ErrHandler != nil {
		return h.ErrHandler(err, v, attempt)
	}
	for _, hook := range h.OnError {
		hook(err, v)
	}
	return Skip
}

func (h *Hooks[T]) fireBatch(batch []T) {
	for _, hook := range h.OnBatch {
		hook(batch)
	}
}

func (h *Hooks[T]) fireCompletion() {
	for _, hook := range h.OnCompletion {
		hook()
	}
}

func (h *Hooks[T]) fireTimeout(d time.Duration) {
	for _, hook := range h.OnTimeout {
		hook(d)
	}
}
