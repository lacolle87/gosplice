package gosplice

import (
	"bufio"
	"context"
	"io"
	"sync"
)

type Source[T any] interface {
	Next() (T, bool)
}

type Sizer interface {
	SizeHint() int
}

type directCollectable[T any] interface {
	collectAll() []T
}

type sliceSource[T any] struct {
	data []T
	idx  int
}

func (s *sliceSource[T]) Next() (T, bool) {
	if s.idx >= len(s.data) {
		var zero T
		return zero, false
	}
	v := s.data[s.idx]
	s.idx++
	return v, true
}

func (s *sliceSource[T]) SizeHint() int { return len(s.data) - s.idx }

func (s *sliceSource[T]) remaining() []T { return s.data[s.idx:] }

func (s *sliceSource[T]) collectAll() []T {
	rem := s.data[s.idx:]
	s.idx = len(s.data)
	result := make([]T, len(rem))
	copy(result, rem)
	return result
}

func FromSlice[T any](data []T) *Pipeline[T] {
	return newPipeline[T](&sliceSource[T]{data: data})
}

type chanSource[T any] struct {
	ch <-chan T
}

func (s *chanSource[T]) Next() (T, bool) {
	v, ok := <-s.ch
	return v, ok
}

func FromChannel[T any](ch <-chan T) *Pipeline[T] {
	return newPipeline[T](&chanSource[T]{ch: ch})
}

type chanCtxSource[T any] struct {
	ch   <-chan T
	ctx  context.Context
	done bool
}

func (s *chanCtxSource[T]) Next() (T, bool) {
	if s.done {
		var zero T
		return zero, false
	}
	select {
	case <-s.ctx.Done():
		s.done = true
		var zero T
		return zero, false
	case v, ok := <-s.ch:
		if !ok {
			s.done = true
		}
		return v, ok
	}
}

// FromChannelCtx creates a pipeline from a channel with context-aware reads.
// When ctx is cancelled, the source stops reading from ch immediately.
func FromChannelCtx[T any](ctx context.Context, ch <-chan T) *Pipeline[T] {
	p := newPipeline[T](&chanCtxSource[T]{ch: ch, ctx: ctx})
	p.ctx = ctx
	return p
}

type readerSource struct {
	scanner *bufio.Scanner
	once    sync.Once
	err     error
}

func (s *readerSource) Next() (string, bool) {
	if s.scanner.Scan() {
		return s.scanner.Text(), true
	}
	s.once.Do(func() { s.err = s.scanner.Err() })
	return "", false
}

func (s *readerSource) Err() error { return s.err }

func FromReader(r io.Reader) *Pipeline[string] {
	return newPipeline[string](&readerSource{scanner: bufio.NewScanner(r)})
}

type funcSource[T any] struct {
	fn func() (T, bool)
}

func (s *funcSource[T]) Next() (T, bool) { return s.fn() }

func FromFunc[T any](fn func() (T, bool)) *Pipeline[T] {
	return newPipeline[T](&funcSource[T]{fn: fn})
}

type rangeSource struct {
	cur int
	end int
}

func (s *rangeSource) Next() (int, bool) {
	if s.cur >= s.end {
		return 0, false
	}
	v := s.cur
	s.cur++
	return v, true
}

func (s *rangeSource) SizeHint() int {
	if r := s.end - s.cur; r > 0 {
		return r
	}
	return 0
}

func FromRange(start, end int) *Pipeline[int] {
	return newPipeline[int](&rangeSource{cur: start, end: end})
}

func sizeHint[T any](src Source[T]) int {
	if s, ok := src.(Sizer); ok {
		return s.SizeHint()
	}
	return -1
}

func drainSource[T any](src Source[T]) []T {
	if ss, ok := src.(*sliceSource[T]); ok {
		rem := ss.remaining()
		ss.idx = len(ss.data)
		result := make([]T, len(rem))
		copy(result, rem)
		return result
	}
	if s, ok := src.(Sizer); ok {
		if hint := s.SizeHint(); hint > 0 {
			items := make([]T, 0, hint)
			for {
				v, ok := src.Next()
				if !ok {
					return items
				}
				items = append(items, v)
			}
		}
	}
	var items []T
	for {
		v, ok := src.Next()
		if !ok {
			return items
		}
		items = append(items, v)
	}
}
