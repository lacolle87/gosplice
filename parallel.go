package gosplice

import "sync"

func PipeMapParallel[T any, U any](p *Pipeline[T], workers int, fn func(T) U) *Pipeline[U] {
	items := drainSource(p.source)
	n := len(items)
	if n == 0 {
		return FromSlice([]U{})
	}

	results := make([]U, n)
	var wg sync.WaitGroup
	batchSize := (n + workers - 1) / workers

	for w := 0; w < workers; w++ {
		lo := w * batchSize
		hi := lo + batchSize
		if hi > n {
			hi = n
		}
		if lo >= n {
			break
		}
		wg.Add(1)
		go func(lo, hi int) {
			defer wg.Done()
			for i := lo; i < hi; i++ {
				results[i] = fn(items[i])
			}
		}(lo, hi)
	}
	wg.Wait()
	return FromSlice(results)
}

func PipeFilterParallel[T any](p *Pipeline[T], workers int, fn func(T) bool) *Pipeline[T] {
	items := drainSource(p.source)
	n := len(items)
	if n == 0 {
		return FromSlice([]T{})
	}

	keep := make([]bool, n)
	var wg sync.WaitGroup
	batchSize := (n + workers - 1) / workers

	for w := 0; w < workers; w++ {
		lo := w * batchSize
		hi := lo + batchSize
		if hi > n {
			hi = n
		}
		if lo >= n {
			break
		}
		wg.Add(1)
		go func(lo, hi int) {
			defer wg.Done()
			for i := lo; i < hi; i++ {
				keep[i] = fn(items[i])
			}
		}(lo, hi)
	}
	wg.Wait()

	count := 0
	for _, k := range keep {
		if k {
			count++
		}
	}
	result := make([]T, 0, count)
	for i, v := range items {
		if keep[i] {
			result = append(result, v)
		}
	}
	return FromSlice(result)
}

func PipeMapParallelErr[T any, U any](p *Pipeline[T], workers int, fn func(T) (U, error)) *Pipeline[U] {
	items := drainSource(p.source)
	n := len(items)
	if n == 0 {
		return FromSlice([]U{})
	}

	vals := make([]U, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	batchSize := (n + workers - 1) / workers

	for w := 0; w < workers; w++ {
		lo := w * batchSize
		hi := lo + batchSize
		if hi > n {
			hi = n
		}
		if lo >= n {
			break
		}
		wg.Add(1)
		go func(lo, hi int) {
			defer wg.Done()
			for i := lo; i < hi; i++ {
				vals[i], errs[i] = fn(items[i])
			}
		}(lo, hi)
	}
	wg.Wait()

	out := make([]U, 0, n)
	for i := range items {
		if errs[i] != nil {
			p.hooks.handleError(errs[i], items[i], 1)
			continue
		}
		out = append(out, vals[i])
	}
	return FromSlice(out)
}

func PipeMapParallelStream[T any, U any](p *Pipeline[T], workers int, bufSize int, fn func(T) U) *Pipeline[U] {
	src := p.source
	hooks := p.hooks
	fireHooks := hooks.hasElement()
	outCh := make(chan U, bufSize)

	go func() {
		defer close(outCh)
		var wg sync.WaitGroup
		sem := make(chan struct{}, workers)

		pending := make(map[int]U)
		var mu sync.Mutex
		nextOut := 0
		idx := 0

		for {
			v, ok := src.Next()
			if !ok {
				break
			}
			if fireHooks {
				hooks.fireElement(v)
			}
			myIdx := idx
			idx++

			sem <- struct{}{}
			wg.Add(1)
			go func(item T, i int) {
				defer func() { <-sem; wg.Done() }()
				result := fn(item)
				mu.Lock()
				pending[i] = result
				var toSend []U
				for {
					if r, exists := pending[nextOut]; exists {
						toSend = append(toSend, r)
						delete(pending, nextOut)
						nextOut++
					} else {
						break
					}
				}
				mu.Unlock()
				for _, r := range toSend {
					outCh <- r
				}
			}(v, myIdx)
		}
		wg.Wait()
	}()

	return FromChannel(outCh)
}
