package gosplice

import (
	"bufio"
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

// --- sliceSource ---

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

func (s *sliceSource[T]) SizeHint() int {
	return len(s.data) - s.idx
}

func (s *sliceSource[T]) remaining() []T {
	return s.data[s.idx:]
}

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

// --- chanSource ---

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

// --- readerSource ---

type readerSource struct {
	scanner *bufio.Scanner
	once    sync.Once
	err     error
}

func (s *readerSource) Next() (string, bool) {
	if s.scanner.Scan() {
		return s.scanner.Text(), true
	}
	s.once.Do(func() {
		s.err = s.scanner.Err()
	})
	return "", false
}

func (s *readerSource) Err() error {
	return s.err
}

func FromReader(r io.Reader) *Pipeline[string] {
	return newPipeline[string](&readerSource{scanner: bufio.NewScanner(r)})
}

// --- funcSource ---

type funcSource[T any] struct {
	fn func() (T, bool)
}

func (s *funcSource[T]) Next() (T, bool) {
	return s.fn()
}

func FromFunc[T any](fn func() (T, bool)) *Pipeline[T] {
	return newPipeline[T](&funcSource[T]{fn: fn})
}

// --- rangeSource ---

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
	r := s.end - s.cur
	if r < 0 {
		return 0
	}
	return r
}

func FromRange(start, end int) *Pipeline[int] {
	return newPipeline[int](&rangeSource{cur: start, end: end})
}

// --- filterSource ---

type filterSource[T any] struct {
	inner Source[T]
	pred  func(T) bool
}

func (s *filterSource[T]) Next() (T, bool) {
	for {
		v, ok := s.inner.Next()
		if !ok {
			var zero T
			return zero, false
		}
		if s.pred(v) {
			return v, true
		}
	}
}

func (s *filterSource[T]) collectAll() []T {
	if ss, ok := s.inner.(*sliceSource[T]); ok {
		data := ss.remaining()
		ss.idx = len(ss.data)
		result := make([]T, 0, len(data)>>2+4)
		for _, v := range data {
			if s.pred(v) {
				result = append(result, v)
			}
		}
		return result
	}
	return nil
}

// --- takeSource ---

type takeSource[T any] struct {
	inner Source[T]
	n     int
	count int
}

func (s *takeSource[T]) Next() (T, bool) {
	if s.count >= s.n {
		var zero T
		return zero, false
	}
	v, ok := s.inner.Next()
	if ok {
		s.count++
	}
	return v, ok
}

func (s *takeSource[T]) SizeHint() int {
	remaining := s.n - s.count
	if remaining <= 0 {
		return 0
	}
	if sizer, ok := s.inner.(Sizer); ok {
		if hint := sizer.SizeHint(); hint >= 0 && hint < remaining {
			return hint
		}
	}
	return remaining
}

// --- skipSource ---

type skipSource[T any] struct {
	inner   Source[T]
	n       int
	skipped bool
}

func (s *skipSource[T]) Next() (T, bool) {
	if !s.skipped {
		s.skipped = true
		for i := 0; i < s.n; i++ {
			if _, ok := s.inner.Next(); !ok {
				var zero T
				return zero, false
			}
		}
	}
	return s.inner.Next()
}

// --- peekSource ---

type peekSource[T any] struct {
	inner Source[T]
	fn    func(T)
}

func (s *peekSource[T]) Next() (T, bool) {
	v, ok := s.inner.Next()
	if ok {
		s.fn(v)
	}
	return v, ok
}

func (s *peekSource[T]) SizeHint() int {
	if sizer, ok := s.inner.(Sizer); ok {
		return sizer.SizeHint()
	}
	return -1
}

// --- distinctSource ---

type distinctSource[T comparable] struct {
	inner Source[T]
	seen  map[T]struct{}
}

func (s *distinctSource[T]) Next() (T, bool) {
	for {
		v, ok := s.inner.Next()
		if !ok {
			var zero T
			return zero, false
		}
		if _, exists := s.seen[v]; !exists {
			s.seen[v] = struct{}{}
			return v, true
		}
	}
}

// --- mapSource ---

type mapSource[T any, U any] struct {
	inner    Source[T]
	fn       func(T) U
	hooks    *Hooks[T]
	hasHooks bool
}

func (s *mapSource[T, U]) Next() (U, bool) {
	v, ok := s.inner.Next()
	if !ok {
		var zero U
		return zero, false
	}
	if s.hasHooks {
		s.hooks.fireElement(v)
	}
	return s.fn(v), true
}

func (s *mapSource[T, U]) SizeHint() int {
	if sizer, ok := s.inner.(Sizer); ok {
		return sizer.SizeHint()
	}
	return -1
}

func (s *mapSource[T, U]) collectAll() []U {
	if ss, ok := s.inner.(*sliceSource[T]); ok && !s.hasHooks {
		data := ss.remaining()
		ss.idx = len(ss.data)
		result := make([]U, len(data))
		for i, v := range data {
			result[i] = s.fn(v)
		}
		return result
	}
	return nil
}

// --- mapErrSource ---

type mapErrSource[T any, U any] struct {
	inner    Source[T]
	fn       func(T) (U, error)
	hooks    *Hooks[T]
	hasHooks bool
	hasErr   bool
}

func (s *mapErrSource[T, U]) Next() (U, bool) {
	for {
		v, ok := s.inner.Next()
		if !ok {
			var zero U
			return zero, false
		}
		if s.hasHooks {
			s.hooks.fireElement(v)
		}
		result, err := s.fn(v)
		if err != nil {
			if s.hasErr {
				s.hooks.fireError(err, v)
			}
			continue
		}
		return result, true
	}
}

// --- flatMapSource ---

type flatMapSource[T any, U any] struct {
	inner    Source[T]
	fn       func(T) []U
	hooks    *Hooks[T]
	hasHooks bool
	buf      []U
	idx      int
}

func (s *flatMapSource[T, U]) Next() (U, bool) {
	for {
		if s.idx < len(s.buf) {
			v := s.buf[s.idx]
			s.idx++
			return v, true
		}
		elem, ok := s.inner.Next()
		if !ok {
			var zero U
			return zero, false
		}
		if s.hasHooks {
			s.hooks.fireElement(elem)
		}
		s.buf = s.fn(elem)
		s.idx = 0
	}
}

// --- chunkSource ---

type chunkSource[T any] struct {
	inner    Source[T]
	size     int
	hooks    *Hooks[T]
	hasHooks bool
	done     bool
}

func (s *chunkSource[T]) Next() ([]T, bool) {
	if s.done {
		return nil, false
	}
	chunk := make([]T, 0, s.size)
	for len(chunk) < s.size {
		v, ok := s.inner.Next()
		if !ok {
			s.done = true
			break
		}
		if s.hasHooks {
			s.hooks.fireElement(v)
		}
		chunk = append(chunk, v)
	}
	if len(chunk) == 0 {
		return nil, false
	}
	return chunk, true
}

func (s *chunkSource[T]) collectAll() [][]T {
	if ss, ok := s.inner.(*sliceSource[T]); ok && !s.hasHooks {
		data := ss.remaining()
		ss.idx = len(ss.data)
		n := len(data)
		if n == 0 {
			return nil
		}
		numChunks := (n + s.size - 1) / s.size
		chunks := make([][]T, numChunks)
		for i := 0; i < numChunks; i++ {
			start := i * s.size
			end := start + s.size
			if end > n {
				end = n
			}
			chunk := make([]T, end-start)
			copy(chunk, data[start:end])
			chunks[i] = chunk
		}
		return chunks
	}
	return nil
}

// --- windowSource ---

type windowSource[T any] struct {
	inner    Source[T]
	size     int
	step     int
	hooks    *Hooks[T]
	hasHooks bool
	ring     []T
	start    int
	count    int
	inited   bool
	done     bool
}

func (s *windowSource[T]) Next() ([]T, bool) {
	if s.done {
		return nil, false
	}
	if !s.inited {
		s.ring = make([]T, 0, s.size)
		for s.count < s.size {
			v, ok := s.inner.Next()
			if !ok {
				s.done = true
				if s.count > 0 {
					s.inited = true
					out := make([]T, s.count)
					copy(out, s.ring)
					return out, true
				}
				return nil, false
			}
			if s.hasHooks {
				s.hooks.fireElement(v)
			}
			s.ring = append(s.ring, v)
			s.count++
		}
		s.inited = true
		out := make([]T, s.size)
		copy(out, s.ring)
		return out, true
	}
	for i := 0; i < s.step; i++ {
		v, ok := s.inner.Next()
		if !ok {
			s.done = true
			return nil, false
		}
		if s.hasHooks {
			s.hooks.fireElement(v)
		}
		s.ring[s.start%s.size] = v
		s.start++
	}
	out := make([]T, s.size)
	for i := 0; i < s.size; i++ {
		out[i] = s.ring[(s.start+i)%s.size]
	}
	return out, true
}

func (s *windowSource[T]) collectAll() [][]T {
	if ss, ok := s.inner.(*sliceSource[T]); ok && !s.hasHooks {
		data := ss.remaining()
		ss.idx = len(ss.data)
		n := len(data)
		if n == 0 {
			return nil
		}
		if n < s.size {
			out := make([]T, n)
			copy(out, data)
			return [][]T{out}
		}
		numWindows := (n-s.size)/s.step + 1
		windows := make([][]T, numWindows)
		for i := 0; i < numWindows; i++ {
			start := i * s.step
			w := make([]T, s.size)
			copy(w, data[start:start+s.size])
			windows[i] = w
		}
		return windows
	}
	return nil
}

// --- helpers ---

func sizeHint[T any](src Source[T]) int {
	if s, ok := src.(Sizer); ok {
		return s.SizeHint()
	}
	return -1
}
