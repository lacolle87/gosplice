package gosplice

import "cmp"

func GroupBy[T any, K comparable](p *Pipeline[T], keyFn func(T) K) map[K][]T {
	defer p.hooks.fireCompletion()
	groups := make(map[K][]T)

	if !p.hooks.hasElement() {
		if ss, ok := p.source.(*sliceSource[T]); ok {
			for _, v := range ss.remaining() {
				k := keyFn(v)
				groups[k] = append(groups[k], v)
			}
			ss.idx = len(ss.data)
			return groups
		}
	}

	src := p.source
	if p.hooks.hasElement() {
		for {
			v, ok := src.Next()
			if !ok {
				return groups
			}
			p.hooks.fireElement(v)
			k := keyFn(v)
			groups[k] = append(groups[k], v)
		}
	}
	for {
		v, ok := src.Next()
		if !ok {
			return groups
		}
		k := keyFn(v)
		groups[k] = append(groups[k], v)
	}
}

func CountBy[T any, K comparable](p *Pipeline[T], keyFn func(T) K) map[K]int {
	defer p.hooks.fireCompletion()
	counts := make(map[K]int)

	if !p.hooks.hasElement() {
		if ss, ok := p.source.(*sliceSource[T]); ok {
			for _, v := range ss.remaining() {
				counts[keyFn(v)]++
			}
			ss.idx = len(ss.data)
			return counts
		}
	}

	src := p.source
	if p.hooks.hasElement() {
		for {
			v, ok := src.Next()
			if !ok {
				return counts
			}
			p.hooks.fireElement(v)
			counts[keyFn(v)]++
		}
	}
	for {
		v, ok := src.Next()
		if !ok {
			return counts
		}
		counts[keyFn(v)]++
	}
}

func SumBy[T any, N cmp.Ordered](p *Pipeline[T], fn func(T) N) N {
	defer p.hooks.fireCompletion()
	var sum N

	if !p.hooks.hasElement() {
		if ss, ok := p.source.(*sliceSource[T]); ok {
			for _, v := range ss.remaining() {
				sum += fn(v)
			}
			ss.idx = len(ss.data)
			return sum
		}
	}

	src := p.source
	if p.hooks.hasElement() {
		for {
			v, ok := src.Next()
			if !ok {
				return sum
			}
			p.hooks.fireElement(v)
			sum += fn(v)
		}
	}
	for {
		v, ok := src.Next()
		if !ok {
			return sum
		}
		sum += fn(v)
	}
}

func MaxBy[T any, N cmp.Ordered](p *Pipeline[T], fn func(T) N) (T, bool) {
	defer p.hooks.fireCompletion()
	var maxElem T
	var maxVal N
	found := false
	fireHooks := p.hooks.hasElement()

	if !fireHooks {
		if ss, ok := p.source.(*sliceSource[T]); ok {
			for _, v := range ss.remaining() {
				val := fn(v)
				if !found || val > maxVal {
					maxVal = val
					maxElem = v
					found = true
				}
			}
			ss.idx = len(ss.data)
			return maxElem, found
		}
	}

	src := p.source
	for {
		v, ok := src.Next()
		if !ok {
			return maxElem, found
		}
		if fireHooks {
			p.hooks.fireElement(v)
		}
		val := fn(v)
		if !found || val > maxVal {
			maxVal = val
			maxElem = v
			found = true
		}
	}
}

func MinBy[T any, N cmp.Ordered](p *Pipeline[T], fn func(T) N) (T, bool) {
	defer p.hooks.fireCompletion()
	var minElem T
	var minVal N
	found := false
	fireHooks := p.hooks.hasElement()

	if !fireHooks {
		if ss, ok := p.source.(*sliceSource[T]); ok {
			for _, v := range ss.remaining() {
				val := fn(v)
				if !found || val < minVal {
					minVal = val
					minElem = v
					found = true
				}
			}
			ss.idx = len(ss.data)
			return minElem, found
		}
	}

	src := p.source
	for {
		v, ok := src.Next()
		if !ok {
			return minElem, found
		}
		if fireHooks {
			p.hooks.fireElement(v)
		}
		val := fn(v)
		if !found || val < minVal {
			minVal = val
			minElem = v
			found = true
		}
	}
}

func Partition[T any](p *Pipeline[T], fn func(T) bool) (matched []T, unmatched []T) {
	defer p.hooks.fireCompletion()
	fireHooks := p.hooks.hasElement()

	if !fireHooks {
		if ss, ok := p.source.(*sliceSource[T]); ok {
			for _, v := range ss.remaining() {
				if fn(v) {
					matched = append(matched, v)
				} else {
					unmatched = append(unmatched, v)
				}
			}
			ss.idx = len(ss.data)
			return
		}
	}

	src := p.source
	for {
		v, ok := src.Next()
		if !ok {
			return
		}
		if fireHooks {
			p.hooks.fireElement(v)
		}
		if fn(v) {
			matched = append(matched, v)
		} else {
			unmatched = append(unmatched, v)
		}
	}
}
