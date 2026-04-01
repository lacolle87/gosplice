package gosplice

import "time"

type ErrorAction int

const (
	Skip  ErrorAction = iota // drop element, continue pipeline
	Retry                    // re-run the operation on same element
	Abort                    // stop pipeline gracefully
)

type ElementHook[T any] func(T)
type ErrorHook[T any] func(error, T)
type ErrorHandler[T any] func(err error, elem T, attempt int) ErrorAction
type BatchHook[T any] func([]T)
type CompletionHook func()
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

func (h *Hooks[T]) clone() *Hooks[T] {
	if h == nil {
		return newHooks[T]()
	}
	return &Hooks[T]{
		OnElement:    append([]ElementHook[T]{}, h.OnElement...),
		OnError:      append([]ErrorHook[T]{}, h.OnError...),
		OnBatch:      append([]BatchHook[T]{}, h.OnBatch...),
		OnCompletion: append([]CompletionHook{}, h.OnCompletion...),
		OnTimeout:    append([]TimeoutHook{}, h.OnTimeout...),
		ErrHandler:   h.ErrHandler,
		Timeout:      h.Timeout,
		MaxRetries:   h.MaxRetries,
	}
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
