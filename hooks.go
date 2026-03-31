package gosplice

import "time"

type ElementHook[T any] func(T)
type ErrorHook[T any] func(error, T)
type BatchHook[T any] func([]T)
type CompletionHook func()
type TimeoutHook func(time.Duration)

type Hooks[T any] struct {
	OnElement    []ElementHook[T]
	OnError      []ErrorHook[T]
	OnBatch      []BatchHook[T]
	OnCompletion []CompletionHook
	OnTimeout    []TimeoutHook
	Timeout      time.Duration
}

func newHooks[T any]() *Hooks[T] {
	return &Hooks[T]{}
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
		Timeout:      h.Timeout,
	}
}

func (h *Hooks[T]) hasElement() bool {
	return len(h.OnElement) > 0
}

func (h *Hooks[T]) hasError() bool {
	return len(h.OnError) > 0
}

func (h *Hooks[T]) fireElement(v T) {
	for _, hook := range h.OnElement {
		hook(v)
	}
}

func (h *Hooks[T]) fireError(err error, v T) {
	for _, hook := range h.OnError {
		hook(err, v)
	}
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
