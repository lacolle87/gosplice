package gosplice

import "cmp"

func GroupBy[T any, K comparable](p *Pipeline[T], keyFn func(T) K) map[K][]T {
	defer p.finalize()
	return fold(p, make(map[K][]T), func(g map[K][]T, v T) map[K][]T {
		k := keyFn(v)
		g[k] = append(g[k], v)
		return g
	})
}

func CountBy[T any, K comparable](p *Pipeline[T], keyFn func(T) K) map[K]int {
	defer p.finalize()
	return fold(p, make(map[K]int), func(c map[K]int, v T) map[K]int {
		c[keyFn(v)]++
		return c
	})
}

func SumBy[T any, N cmp.Ordered](p *Pipeline[T], fn func(T) N) N {
	defer p.finalize()
	if !p.ctxActive() && !p.hooks.hasElement() {
		if ss, ok := p.source.(*sliceSource[T]); ok {
			var sum N
			for _, v := range ss.remaining() {
				sum += fn(v)
			}
			ss.idx = len(ss.data)
			return sum
		}
	}
	var zero N
	return fold(p, zero, func(sum N, v T) N { return sum + fn(v) })
}

func MaxBy[T any, N cmp.Ordered](p *Pipeline[T], fn func(T) N) (T, bool) {
	defer p.finalize()
	var maxElem T
	var maxVal N
	found := false
	drain(p, func(v T) {
		val := fn(v)
		if !found || val > maxVal {
			maxVal = val
			maxElem = v
			found = true
		}
	})
	return maxElem, found
}

func MinBy[T any, N cmp.Ordered](p *Pipeline[T], fn func(T) N) (T, bool) {
	defer p.finalize()
	var minElem T
	var minVal N
	found := false
	drain(p, func(v T) {
		val := fn(v)
		if !found || val < minVal {
			minVal = val
			minElem = v
			found = true
		}
	})
	return minElem, found
}

func Partition[T any](p *Pipeline[T], pred func(T) bool) (matched []T, unmatched []T) {
	defer p.finalize()
	drain(p, func(v T) {
		if pred(v) {
			matched = append(matched, v)
		} else {
			unmatched = append(unmatched, v)
		}
	})
	return
}
