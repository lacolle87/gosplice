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

func (p *Pipeline[T]) Collect() []T {
	defer p.hooks.fireCompletion()
	noHooks := !p.hooks.hasElement()

	if noHooks {
		if dc, ok := p.source.(directCollectable[T]); ok {
			if result := dc.collectAll(); result != nil {
				return result
			}
		}
	}

	src := p.source
	var result []T
	if hint := sizeHint(src); hint > 0 {
		result = make([]T, 0, hint)
	}

	if !noHooks {
		for {
			v, ok := src.Next()
			if !ok {
				return result
			}
			p.hooks.fireElement(v)
			result = append(result, v)
		}
	}

	for {
		v, ok := src.Next()
		if !ok {
			return result
		}
		result = append(result, v)
	}
}

func (p *Pipeline[T]) CollectTo(dst []T) []T {
	defer p.hooks.fireCompletion()
	src := p.source
	var result []T
	if dst != nil {
		result = dst[:0]
	} else if hint := sizeHint(src); hint > 0 {
		result = make([]T, 0, hint)
	}

	if p.hooks.hasElement() {
		for {
			v, ok := src.Next()
			if !ok {
				return result
			}
			p.hooks.fireElement(v)
			result = append(result, v)
		}
	}

	for {
		v, ok := src.Next()
		if !ok {
			return result
		}
		result = append(result, v)
	}
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

	src := p.source
	acc := initial
	if p.hooks.hasElement() {
		for {
			v, ok := src.Next()
			if !ok {
				return acc
			}
			p.hooks.fireElement(v)
			acc = fn(acc, v)
		}
	}
	for {
		v, ok := src.Next()
		if !ok {
			return acc
		}
		acc = fn(acc, v)
	}
}

func (p *Pipeline[T]) ForEach(fn func(T)) {
	defer p.hooks.fireCompletion()
	if !p.hooks.hasElement() {
		if ss, ok := p.source.(*sliceSource[T]); ok {
			for _, v := range ss.remaining() {
				fn(v)
			}
			ss.idx = len(ss.data)
			return
		}
	}

	src := p.source
	if p.hooks.hasElement() {
		for {
			v, ok := src.Next()
			if !ok {
				return
			}
			p.hooks.fireElement(v)
			fn(v)
		}
	}
	for {
		v, ok := src.Next()
		if !ok {
			return
		}
		fn(v)
	}
}

func (p *Pipeline[T]) Count() int {
	defer p.hooks.fireCompletion()
	if !p.hooks.hasElement() {
		if ss, ok := p.source.(*sliceSource[T]); ok {
			n := len(ss.remaining())
			ss.idx = len(ss.data)
			return n
		}
	}

	src := p.source
	n := 0
	if p.hooks.hasElement() {
		for {
			v, ok := src.Next()
			if !ok {
				return n
			}
			p.hooks.fireElement(v)
			n++
			_ = v
		}
	}
	for {
		if _, ok := src.Next(); !ok {
			return n
		}
		n++
	}
}

func (p *Pipeline[T]) First() (T, bool) {
	defer p.hooks.fireCompletion()
	v, ok := p.source.Next()
	if ok && p.hooks.hasElement() {
		p.hooks.fireElement(v)
	}
	return v, ok
}

func (p *Pipeline[T]) Any(fn func(T) bool) bool {
	defer p.hooks.fireCompletion()
	if !p.hooks.hasElement() {
		if ss, ok := p.source.(*sliceSource[T]); ok {
			for _, v := range ss.remaining() {
				if fn(v) {
					return true
				}
			}
			ss.idx = len(ss.data)
			return false
		}
	}

	src := p.source
	if p.hooks.hasElement() {
		for {
			v, ok := src.Next()
			if !ok {
				return false
			}
			p.hooks.fireElement(v)
			if fn(v) {
				return true
			}
		}
	}
	for {
		v, ok := src.Next()
		if !ok {
			return false
		}
		if fn(v) {
			return true
		}
	}
}

func (p *Pipeline[T]) All(fn func(T) bool) bool {
	defer p.hooks.fireCompletion()
	if !p.hooks.hasElement() {
		if ss, ok := p.source.(*sliceSource[T]); ok {
			for _, v := range ss.remaining() {
				if !fn(v) {
					return false
				}
			}
			ss.idx = len(ss.data)
			return true
		}
	}

	src := p.source
	if p.hooks.hasElement() {
		for {
			v, ok := src.Next()
			if !ok {
				return true
			}
			p.hooks.fireElement(v)
			if !fn(v) {
				return false
			}
		}
	}
	for {
		v, ok := src.Next()
		if !ok {
			return true
		}
		if !fn(v) {
			return false
		}
	}
}

func (p *Pipeline[T]) WriteTo(fn func(T)) {
	p.ForEach(fn)
}
