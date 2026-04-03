package gosplice

// ctxCheckInterval controls how often the slice fast path checks ctx.Done().
// Amortized to avoid per-element overhead on tight range loops.
const ctxCheckInterval = 64

func ctxDone[T any](p *Pipeline[T]) bool {
	select {
	case <-p.ctx.Done():
		p.setErr(p.ctx.Err())
		return true
	default:
		return false
	}
}

func drain[T any](p *Pipeline[T], fn func(T)) {
	fireHooks := p.hooks.hasElement()

	// --- ctx-aware path ---
	if p.ctx != nil {
		if !fireHooks {
			if ss, ok := p.source.(*sliceSource[T]); ok {
				rem := ss.remaining()
				for i, v := range rem {
					if i&(ctxCheckInterval-1) == 0 && ctxDone(p) {
						ss.idx += i
						return
					}
					fn(v)
				}
				ss.idx = len(ss.data)
				return
			}
		}
		src := p.source
		if fireHooks {
			for {
				if ctxDone(p) {
					return
				}
				v, ok := src.Next()
				if !ok {
					ctxDone(p) // record error if ctx caused end-of-source
					return
				}
				p.hooks.fireElement(v)
				fn(v)
			}
		}
		for {
			if ctxDone(p) {
				return
			}
			v, ok := src.Next()
			if !ok {
				ctxDone(p)
				return
			}
			fn(v)
		}
	}

	// --- original zero-overhead paths (ctx == nil) ---
	if !fireHooks {
		if ss, ok := p.source.(*sliceSource[T]); ok {
			for _, v := range ss.remaining() {
				fn(v)
			}
			ss.idx = len(ss.data)
			return
		}
	}

	src := p.source
	if fireHooks {
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

func fold[T any, A any](p *Pipeline[T], init A, fn func(A, T) A) A {
	fireHooks := p.hooks.hasElement()
	acc := init

	// --- ctx-aware path ---
	if p.ctx != nil {
		if !fireHooks {
			if ss, ok := p.source.(*sliceSource[T]); ok {
				rem := ss.remaining()
				for i, v := range rem {
					if i&(ctxCheckInterval-1) == 0 && ctxDone(p) {
						ss.idx += i
						return acc
					}
					acc = fn(acc, v)
				}
				ss.idx = len(ss.data)
				return acc
			}
		}
		src := p.source
		if fireHooks {
			for {
				if ctxDone(p) {
					return acc
				}
				v, ok := src.Next()
				if !ok {
					ctxDone(p)
					return acc
				}
				p.hooks.fireElement(v)
				acc = fn(acc, v)
			}
		}
		for {
			if ctxDone(p) {
				return acc
			}
			v, ok := src.Next()
			if !ok {
				ctxDone(p)
				return acc
			}
			acc = fn(acc, v)
		}
	}

	// --- original zero-overhead paths (ctx == nil) ---
	if !fireHooks {
		if ss, ok := p.source.(*sliceSource[T]); ok {
			for _, v := range ss.remaining() {
				acc = fn(acc, v)
			}
			ss.idx = len(ss.data)
			return acc
		}
	}

	src := p.source
	if fireHooks {
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

func foldWhile[T any, A any](p *Pipeline[T], init A, fn func(A, T) (A, bool)) A {
	fireHooks := p.hooks.hasElement()
	acc := init

	// --- ctx-aware path ---
	if p.ctx != nil {
		if !fireHooks {
			if ss, ok := p.source.(*sliceSource[T]); ok {
				rem := ss.remaining()
				for i, v := range rem {
					if i&(ctxCheckInterval-1) == 0 && ctxDone(p) {
						ss.idx += i
						return acc
					}
					var cont bool
					acc, cont = fn(acc, v)
					if !cont {
						ss.idx = len(ss.data)
						return acc
					}
				}
				ss.idx = len(ss.data)
				return acc
			}
		}
		src := p.source
		if fireHooks {
			for {
				if ctxDone(p) {
					return acc
				}
				v, ok := src.Next()
				if !ok {
					ctxDone(p)
					return acc
				}
				p.hooks.fireElement(v)
				var cont bool
				acc, cont = fn(acc, v)
				if !cont {
					return acc
				}
			}
		}
		for {
			if ctxDone(p) {
				return acc
			}
			v, ok := src.Next()
			if !ok {
				ctxDone(p)
				return acc
			}
			var cont bool
			acc, cont = fn(acc, v)
			if !cont {
				return acc
			}
		}
	}

	// --- original zero-overhead paths (ctx == nil) ---
	if !fireHooks {
		if ss, ok := p.source.(*sliceSource[T]); ok {
			for _, v := range ss.remaining() {
				var cont bool
				acc, cont = fn(acc, v)
				if !cont {
					ss.idx = len(ss.data)
					return acc
				}
			}
			ss.idx = len(ss.data)
			return acc
		}
	}

	src := p.source
	if fireHooks {
		for {
			v, ok := src.Next()
			if !ok {
				return acc
			}
			p.hooks.fireElement(v)
			var cont bool
			acc, cont = fn(acc, v)
			if !cont {
				return acc
			}
		}
	}
	for {
		v, ok := src.Next()
		if !ok {
			return acc
		}
		var cont bool
		acc, cont = fn(acc, v)
		if !cont {
			return acc
		}
	}
}
