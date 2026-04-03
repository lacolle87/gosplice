package gosplice

func PipeMap[T any, U any](p *Pipeline[T], fn func(T) U) *Pipeline[U] {
	return &Pipeline[U]{
		source: &mapSource[T, U]{
			inner: p.source, fn: fn,
			hooks: p.hooks, hasHooks: p.hooks.hasElement(),
		},
		hooks: newHooks[U](),
		ctx:   p.ctx,
	}
}

func PipeMapErr[T any, U any](p *Pipeline[T], fn func(T) (U, error)) *Pipeline[U] {
	return &Pipeline[U]{
		source: &mapErrSource[T, U]{
			inner: p.source, fn: fn,
			hooks: p.hooks, hasHooks: p.hooks.hasElement(),
			hasErr: p.hooks.hasError(), maxRetries: p.hooks.MaxRetries,
		},
		hooks: newHooks[U](),
		ctx:   p.ctx,
	}
}

func PipeFlatMap[T any, U any](p *Pipeline[T], fn func(T) []U) *Pipeline[U] {
	return &Pipeline[U]{
		source: &flatMapSource[T, U]{
			inner: p.source, fn: fn,
			hooks: p.hooks, hasHooks: p.hooks.hasElement(),
		},
		hooks: newHooks[U](),
		ctx:   p.ctx,
	}
}

func PipeDistinct[T comparable](p *Pipeline[T]) *Pipeline[T] {
	return &Pipeline[T]{
		source: &distinctSource[T]{inner: p.source, seen: make(map[T]struct{})},
		hooks:  p.hooks,
		ctx:    p.ctx,
	}
}

func PipeChunk[T any](p *Pipeline[T], size int) *Pipeline[[]T] {
	return &Pipeline[[]T]{
		source: &chunkSource[T]{
			inner: p.source, size: size,
			hooks: p.hooks, hasHooks: p.hooks.hasElement(),
		},
		hooks: newHooks[[]T](),
		ctx:   p.ctx,
	}
}

func PipeWindow[T any](p *Pipeline[T], size, step int) *Pipeline[[]T] {
	return &Pipeline[[]T]{
		source: &windowSource[T]{
			inner: p.source, size: size, step: step,
			hooks: p.hooks, hasHooks: p.hooks.hasElement(),
		},
		hooks: newHooks[[]T](),
		ctx:   p.ctx,
	}
}

func PipeReduce[T any, U any](p *Pipeline[T], init U, fn func(U, T) U) U {
	defer p.finalize()
	if p.ctx == nil && !p.hooks.hasElement() {
		if ss, ok := p.source.(*sliceSource[T]); ok {
			acc := init
			for _, v := range ss.remaining() {
				acc = fn(acc, v)
			}
			ss.idx = len(ss.data)
			return acc
		}
	}
	return fold(p, init, fn)
}
