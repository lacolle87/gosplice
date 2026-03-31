package gosplice

func PipeMap[T any, U any](p *Pipeline[T], fn func(T) U) *Pipeline[U] {
	return &Pipeline[U]{
		source: &mapSource[T, U]{
			inner:    p.source,
			fn:       fn,
			hooks:    p.hooks,
			hasHooks: p.hooks.hasElement(),
		},
		hooks: newHooks[U](),
	}
}

func PipeMapErr[T any, U any](p *Pipeline[T], fn func(T) (U, error)) *Pipeline[U] {
	return &Pipeline[U]{
		source: &mapErrSource[T, U]{
			inner:    p.source,
			fn:       fn,
			hooks:    p.hooks,
			hasHooks: p.hooks.hasElement(),
			hasErr:   p.hooks.hasError(),
		},
		hooks: newHooks[U](),
	}
}

func PipeFlatMap[T any, U any](p *Pipeline[T], fn func(T) []U) *Pipeline[U] {
	return &Pipeline[U]{
		source: &flatMapSource[T, U]{
			inner:    p.source,
			fn:       fn,
			hooks:    p.hooks,
			hasHooks: p.hooks.hasElement(),
		},
		hooks: newHooks[U](),
	}
}

func PipeDistinct[T comparable](p *Pipeline[T]) *Pipeline[T] {
	return &Pipeline[T]{
		source: &distinctSource[T]{
			inner: p.source,
			seen:  make(map[T]struct{}),
		},
		hooks: p.hooks,
	}
}

func PipeChunk[T any](p *Pipeline[T], size int) *Pipeline[[]T] {
	return &Pipeline[[]T]{
		source: &chunkSource[T]{
			inner:    p.source,
			size:     size,
			hooks:    p.hooks,
			hasHooks: p.hooks.hasElement(),
		},
		hooks: newHooks[[]T](),
	}
}

func PipeWindow[T any](p *Pipeline[T], size, step int) *Pipeline[[]T] {
	return &Pipeline[[]T]{
		source: &windowSource[T]{
			inner:    p.source,
			size:     size,
			step:     step,
			hooks:    p.hooks,
			hasHooks: p.hooks.hasElement(),
		},
		hooks: newHooks[[]T](),
	}
}

func PipeReduce[T any, U any](p *Pipeline[T], acc U, fn func(U, T) U) U {
	defer p.hooks.fireCompletion()
	src := p.source

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
