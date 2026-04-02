package gosplice

import "time"

type Pipeline[T any] struct {
	source Source[T]
	hooks  *Hooks[T]
}

func newPipeline[T any](src Source[T]) *Pipeline[T] {
	return &Pipeline[T]{source: src, hooks: newHooks[T]()}
}

func (p *Pipeline[T]) WithElementHook(fn ElementHook[T]) *Pipeline[T] {
	p.hooks.OnElement = append(p.hooks.OnElement, fn)
	return p
}

func (p *Pipeline[T]) WithErrorHook(fn ErrorHook[T]) *Pipeline[T] {
	p.hooks.OnError = append(p.hooks.OnError, fn)
	return p
}

func (p *Pipeline[T]) WithErrorHandler(fn ErrorHandler[T]) *Pipeline[T] {
	p.hooks.ErrHandler = fn
	return p
}

func (p *Pipeline[T]) WithMaxRetries(n int) *Pipeline[T] {
	p.hooks.MaxRetries = n
	return p
}

func (p *Pipeline[T]) WithBatchHook(fn BatchHook[T]) *Pipeline[T] {
	p.hooks.OnBatch = append(p.hooks.OnBatch, fn)
	return p
}

func (p *Pipeline[T]) WithCompletionHook(fn CompletionHook) *Pipeline[T] {
	p.hooks.OnCompletion = append(p.hooks.OnCompletion, fn)
	return p
}

func (p *Pipeline[T]) WithTimeoutHook(fn TimeoutHook) *Pipeline[T] {
	p.hooks.OnTimeout = append(p.hooks.OnTimeout, fn)
	return p
}

func (p *Pipeline[T]) WithTimeout(d time.Duration) *Pipeline[T] {
	p.hooks.Timeout = d
	return p
}

func (p *Pipeline[T]) Filter(fn func(T) bool) *Pipeline[T] {
	return &Pipeline[T]{source: &filterSource[T]{inner: p.source, pred: fn}, hooks: p.hooks}
}

func (p *Pipeline[T]) Take(n int) *Pipeline[T] {
	return &Pipeline[T]{source: &takeSource[T]{inner: p.source, n: n}, hooks: p.hooks}
}

func (p *Pipeline[T]) Skip(n int) *Pipeline[T] {
	return &Pipeline[T]{source: &skipSource[T]{inner: p.source, n: n}, hooks: p.hooks}
}

func (p *Pipeline[T]) Peek(fn func(T)) *Pipeline[T] {
	return &Pipeline[T]{source: &peekSource[T]{inner: p.source, fn: fn}, hooks: p.hooks}
}

// Collect retains directCollectable fast path for Map→Filter→Collect chains.
// Fallback uses drain.
func (p *Pipeline[T]) Collect() []T {
	defer p.hooks.fireCompletion()

	if !p.hooks.hasElement() {
		if dc, ok := p.source.(directCollectable[T]); ok {
			if result := dc.collectAll(); result != nil {
				return result
			}
		}
	}

	var result []T
	if hint := sizeHint(p.source); hint > 0 {
		result = make([]T, 0, hint)
	}
	drain(p, func(v T) { result = append(result, v) })
	return result
}

// CollectTo reuses the provided buffer.
func (p *Pipeline[T]) CollectTo(dst []T) []T {
	defer p.hooks.fireCompletion()
	var result []T
	if dst != nil {
		result = dst[:0]
	} else if hint := sizeHint(p.source); hint > 0 {
		result = make([]T, 0, hint)
	}
	drain(p, func(v T) { result = append(result, v) })
	return result
}

func (p *Pipeline[T]) Reduce(initial T, fn func(T, T) T) T {
	defer p.hooks.fireCompletion()
	if !p.hooks.hasElement() {
		if ss, ok := p.source.(*sliceSource[T]); ok {
			acc := initial
			for _, v := range ss.remaining() {
				acc = fn(acc, v)
			}
			ss.idx = len(ss.data)
			return acc
		}
	}
	return fold(p, initial, func(acc, v T) T { return fn(acc, v) })
}

func (p *Pipeline[T]) ForEach(fn func(T)) {
	defer p.hooks.fireCompletion()
	drain(p, fn)
}

// Count retains O(1) sliceSource fast path. Fallback uses fold.
func (p *Pipeline[T]) Count() int {
	defer p.hooks.fireCompletion()
	if !p.hooks.hasElement() {
		if ss, ok := p.source.(*sliceSource[T]); ok {
			n := len(ss.remaining())
			ss.idx = len(ss.data)
			return n
		}
	}
	return fold(p, 0, func(n int, _ T) int { return n + 1 })
}

func (p *Pipeline[T]) First() (T, bool) {
	defer p.hooks.fireCompletion()
	v, ok := p.source.Next()
	if ok && p.hooks.hasElement() {
		p.hooks.fireElement(v)
	}
	return v, ok
}

func (p *Pipeline[T]) Any(pred func(T) bool) bool {
	defer p.hooks.fireCompletion()
	return foldWhile(p, false, func(_ bool, v T) (bool, bool) {
		if pred(v) {
			return true, false
		}
		return false, true
	})
}

func (p *Pipeline[T]) All(pred func(T) bool) bool {
	defer p.hooks.fireCompletion()
	return !foldWhile(p, false, func(_ bool, v T) (bool, bool) {
		if !pred(v) {
			return true, false
		}
		return false, true
	})
}

func (p *Pipeline[T]) WriteTo(fn func(T)) {
	p.ForEach(fn)
}
