package gosplice

func drain[T any](p *Pipeline[T], fn func(T)) {
	fireHooks := p.hooks.hasElement()

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
