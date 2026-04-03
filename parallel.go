package gosplice

import (
	"runtime"
	"sync"
)

type indexed[U any] struct {
	i int
	v U
}

func PipeMapParallel[T any, U any](p *Pipeline[T], workers int, fn func(T) U) *Pipeline[U] {
	items := drainSource(p.source)
	n := len(items)
	if n == 0 {
		r := FromSlice([]U{})
		r.ctx = p.ctx
		return r
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
	r := FromSlice(results)
	r.ctx = p.ctx
	return r
}

func PipeFilterParallel[T any](p *Pipeline[T], workers int, fn func(T) bool) *Pipeline[T] {
	items := drainSource(p.source)
	n := len(items)
	if n == 0 {
		r := FromSlice([]T{})
		r.ctx = p.ctx
		return r
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
	r := FromSlice(result)
	r.ctx = p.ctx
	return r
}

func PipeMapParallelErr[T any, U any](p *Pipeline[T], workers int, fn func(T) (U, error)) *Pipeline[U] {
	items := drainSource(p.source)
	n := len(items)
	if n == 0 {
		r := FromSlice([]U{})
		r.ctx = p.ctx
		return r
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
	r := FromSlice(out)
	r.ctx = p.ctx
	return r
}

func PipeMapParallelStream[T any, U any](p *Pipeline[T], workers int, bufSize int, fn func(T) U) *Pipeline[U] {
	src := p.source
	hooks := p.hooks
	fireHooks := hooks.hasElement()
	pipeCtx := p.ctx
	outCh := make(chan U, bufSize)
	done := make(chan struct{})

	go func() {
		var wg sync.WaitGroup
		sem := make(chan struct{}, workers)

		resultCh := make(chan indexed[U], workers)

		senderDone := make(chan struct{})
		go func() {
			defer close(senderDone)
			pending := make(map[int]U)
			nextOut := 0
			for ir := range resultCh {
				pending[ir.i] = ir.v
				for {
					v, exists := pending[nextOut]
					if !exists {
						break
					}
					delete(pending, nextOut)
					nextOut++
					select {
					case outCh <- v:
					case <-done:
						return
					}
				}
			}
			for {
				v, exists := pending[nextOut]
				if !exists {
					return
				}
				delete(pending, nextOut)
				nextOut++
				select {
				case outCh <- v:
				case <-done:
					return
				}
			}
		}()

		idx := 0
		cancelled := false

		for {
			// Check ctx cancellation in the read loop.
			if pipeCtx != nil {
				select {
				case <-pipeCtx.Done():
					cancelled = true
				default:
				}
				if cancelled {
					break
				}
			}

			v, ok := src.Next()
			if !ok {
				break
			}
			if fireHooks {
				hooks.fireElement(v)
			}
			myIdx := idx
			idx++

			select {
			case sem <- struct{}{}:
			case <-done:
				cancelled = true
			}
			if cancelled {
				break
			}
			wg.Add(1)
			go func(item T, i int) {
				defer func() { <-sem; wg.Done() }()
				result := fn(item)
				select {
				case resultCh <- indexed[U]{i: i, v: result}:
				case <-done:
				}
			}(v, myIdx)
		}

		wg.Wait()
		close(resultCh)
		<-senderDone
		close(outCh)
	}()

	ss := &stoppableSource[U]{ch: outCh, done: done}
	runtime.SetFinalizer(ss, (*stoppableSource[U]).stop)
	r := newPipeline[U](ss)
	r.ctx = p.ctx
	return r
}

type stoppableSource[T any] struct {
	ch   <-chan T
	done chan struct{}
	once sync.Once
}

func (s *stoppableSource[T]) Next() (T, bool) {
	v, ok := <-s.ch
	if !ok {
		s.stop()
	}
	return v, ok
}

func (s *stoppableSource[T]) stop() {
	s.once.Do(func() { close(s.done) })
}
