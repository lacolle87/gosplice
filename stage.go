package gosplice

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

type mapErrSource[T any, U any] struct {
	inner      Source[T]
	fn         func(T) (U, error)
	hooks      *Hooks[T]
	hasHooks   bool
	hasErr     bool
	maxRetries int
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

		for attempt := 0; ; attempt++ {
			if attempt >= s.maxRetries {
				goto nextElem
			}
			result, err := s.fn(v)
			if err == nil {
				return result, true
			}
			if !s.hasErr {
				goto nextElem
			}
			action := s.hooks.handleError(err, v, attempt+1)
			switch action {
			case Skip:
				goto nextElem
			case Abort:
				var zero U
				return zero, false
			case Retry:
				// continue to next attempt
			}
		}
	nextElem:
	}
}

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
