package gosplice

import (
	"context"
	"runtime"
	"sync"
)

type indexed[U any] struct {
	i int
	v U
}

func parallelResult[T any, U any](p *Pipeline[T], data []U, cancelled bool) *Pipeline[U] {
	r := FromSlice(data)
	r.cancel = p.cancel
	if cancelled {
		r.err = p.ctx.Err()
	}
	return r
}

func PipeMapParallel[T any, U any](p *Pipeline[T], workers int, fn func(T) U) *Pipeline[U] {
	items, cancelled := drainSourceCtx(p.source, p.ctx)
	n := len(items)
	if n == 0 {
		return parallelResult[T, U](p, []U{}, cancelled)
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
	return parallelResult[T, U](p, results, cancelled)
}

func PipeFilterParallel[T any](p *Pipeline[T], workers int, fn func(T) bool) *Pipeline[T] {
	items, cancelled := drainSourceCtx(p.source, p.ctx)
	n := len(items)
	if n == 0 {
		return parallelResult[T, T](p, []T{}, cancelled)
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
	return parallelResult[T, T](p, result, cancelled)
}

func PipeMapParallelErr[T any, U any](p *Pipeline[T], workers int, fn func(T) (U, error)) *Pipeline[U] {
	items, cancelled := drainSourceCtx(p.source, p.ctx)
	n := len(items)
	if n == 0 {
		return parallelResult[T, U](p, []U{}, cancelled)
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
	return parallelResult[T, U](p, out, cancelled)
}

func PipeMapParallelStream[T any, U any](p *Pipeline[T], workers int, bufSize int, fn func(T) U) *Pipeline[U] {
	src := p.source
	hooks := p.hooks
	fireHooks := hooks.hasElement()
	pipeCtx := p.ctx
	outCh := make(chan U, bufSize)
	done := make(chan struct{})

	var mergedCtx context.Context
	var mergedCancel context.CancelFunc
	if pipeCtx != nil {
		mergedCtx, mergedCancel = context.WithCancel(pipeCtx)
	} else {
		mergedCtx, mergedCancel = context.WithCancel(context.Background())
	}

	go func() {
		var wg sync.WaitGroup
		sem := make(chan struct{}, workers)
		resultCh := make(chan indexed[U], workers)

		go func() {
			select {
			case <-done:
				mergedCancel()
			case <-mergedCtx.Done():
			}
		}()

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
					case <-mergedCtx.Done():
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
				case <-mergedCtx.Done():
					return
				}
			}
		}()

		idx := 0
		for {
			select {
			case <-mergedCtx.Done():
				goto cleanup
			default:
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
			case <-mergedCtx.Done():
				goto cleanup
			}
			wg.Add(1)
			go func(item T, i int) {
				defer func() { <-sem; wg.Done() }()
				result := fn(item)
				select {
				case resultCh <- indexed[U]{i: i, v: result}:
				case <-mergedCtx.Done():
				}
			}(v, myIdx)
		}

	cleanup:
		wg.Wait()
		close(resultCh)
		<-senderDone
		close(outCh)
	}()

	ss := &stoppableSource[U]{ch: outCh, done: done, ctx: mergedCtx, cancelFn: mergedCancel}
	runtime.SetFinalizer(ss, (*stoppableSource[U]).stop)
	r := newPipeline[U](ss)
	r.ctx = p.ctx
	r.cancel = p.cancel
	return r
}

type stoppableSource[T any] struct {
	ch       <-chan T
	done     chan struct{}
	ctx      context.Context
	cancelFn context.CancelFunc
	once     sync.Once
}

func (s *stoppableSource[T]) Next() (T, bool) {
	if s.ctx != nil {
		select {
		case <-s.ctx.Done():
			s.stop()
			var zero T
			return zero, false
		case v, ok := <-s.ch:
			if !ok {
				s.stop()
			}
			return v, ok
		}
	}
	v, ok := <-s.ch
	if !ok {
		s.stop()
	}
	return v, ok
}

func (s *stoppableSource[T]) stop() {
	s.once.Do(func() {
		close(s.done)
		if s.cancelFn != nil {
			s.cancelFn()
		}
	})
}
