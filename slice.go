package gosplice

func Map[T any, U any](slice []T, f func(T) U) []U {
	result := make([]U, len(slice))
	for i, v := range slice {
		result[i] = f(v)
	}
	return result
}

// MapInPlace applies f to each element in place. Zero allocations.
func MapInPlace[T any](slice []T, f func(T) T) {
	for i, v := range slice {
		slice[i] = f(v)
	}
}

func Reduce[T any, U any](slice []T, acc U, f func(U, T) U) U {
	for _, v := range slice {
		acc = f(acc, v)
	}
	return acc
}

func Filter[T any](slice []T, f func(T) bool) []T {
	n := len(slice)
	if n == 0 {
		return nil
	}
	result := make([]T, 0, n>>2+4)
	for _, v := range slice {
		if f(v) {
			result = append(result, v)
		}
	}
	return result
}

// FilterInPlace compacts the slice in place. Modifies and returns a sub-slice of the original.
func FilterInPlace[T any](slice []T, f func(T) bool) []T {
	n := 0
	for _, v := range slice {
		if f(v) {
			slice[n] = v
			n++
		}
	}
	clear(slice[n:])
	return slice[:n]
}

func Some[T any](slice []T, predicate func(T) bool) bool {
	for _, v := range slice {
		if predicate(v) {
			return true
		}
	}
	return false
}

func Every[T any](slice []T, f func(T) bool) bool {
	for _, v := range slice {
		if !f(v) {
			return false
		}
	}
	return true
}

func Find[T any](slice []T, f func(T) bool) (T, bool) {
	for _, v := range slice {
		if f(v) {
			return v, true
		}
	}
	var zero T
	return zero, false
}

func FindIndex[T any](slice []T, f func(T) bool) int {
	for i, v := range slice {
		if f(v) {
			return i
		}
	}
	return -1
}

func FindLast[T any](slice []T, f func(T) bool) (T, bool) {
	for i := len(slice) - 1; i >= 0; i-- {
		if f(slice[i]) {
			return slice[i], true
		}
	}
	var zero T
	return zero, false
}

func ForEach[T any](slice []T, f func(T)) {
	for _, v := range slice {
		f(v)
	}
}

func Includes[T comparable](slice []T, value T) bool {
	for _, v := range slice {
		if v == value {
			return true
		}
	}
	return false
}

func IndexOf[T comparable](slice []T, value T) int {
	for i, v := range slice {
		if v == value {
			return i
		}
	}
	return -1
}

func LastIndexOf[T comparable](slice []T, value T) int {
	for i := len(slice) - 1; i >= 0; i-- {
		if slice[i] == value {
			return i
		}
	}
	return -1
}

func Flat[T any](slice [][]T) []T {
	total := 0
	for _, v := range slice {
		total += len(v)
	}
	result := make([]T, total)
	off := 0
	for _, v := range slice {
		off += copy(result[off:], v)
	}
	return result
}

func FlatMap[T any, U any](slice []T, f func(T) []U) []U {
	n := len(slice)
	if n == 0 {
		return nil
	}
	result := make([]U, 0, n<<1)
	for _, v := range slice {
		result = append(result, f(v)...)
	}
	return result
}

func Reverse[T any](slice []T) []T {
	n := len(slice)
	res := make([]T, n)
	for i, v := range slice {
		res[n-1-i] = v
	}
	return res
}

func ReverseInPlace[T any](slice []T) {
	for i, j := 0, len(slice)-1; i < j; i, j = i+1, j-1 {
		slice[i], slice[j] = slice[j], slice[i]
	}
}

func Unique[T comparable](slice []T) []T {
	n := len(slice)
	if n < 2 {
		return append([]T{}, slice...)
	}
	seen := make(map[T]struct{}, n)
	result := make([]T, 0, n)
	for _, v := range slice {
		if _, exists := seen[v]; !exists {
			seen[v] = struct{}{}
			result = append(result, v)
		}
	}
	return result
}

func Chunk[T any](slice []T, size int) [][]T {
	n := len(slice)
	if size <= 0 || n == 0 {
		return nil
	}
	numChunks := (n + size - 1) / size
	chunks := make([][]T, numChunks)
	for i := 0; i < numChunks; i++ {
		start := i * size
		end := start + size
		if end > n {
			end = n
		}
		chunks[i] = slice[start:end]
	}
	return chunks
}

const removeLinearThreshold = 8

func Remove[T comparable](slice []T, remove []T) []T {
	if len(slice) == 0 {
		return []T{}
	}
	if len(remove) == 0 {
		result := make([]T, len(slice))
		copy(result, slice)
		return result
	}

	capacity := len(slice) - len(remove)
	if capacity < 0 {
		capacity = 0
	}

	if len(remove) <= removeLinearThreshold {
		return removeLinear(slice, remove, capacity)
	}
	return removeMap(slice, remove, capacity)
}

func removeLinear[T comparable](slice []T, remove []T, capacity int) []T {
	result := make([]T, 0, capacity)
	for _, v := range slice {
		found := false
		for _, r := range remove {
			if v == r {
				found = true
				break
			}
		}
		if !found {
			result = append(result, v)
		}
	}
	return result
}

func removeMap[T comparable](slice []T, remove []T, capacity int) []T {
	removeSet := make(map[T]struct{}, len(remove))
	for _, v := range remove {
		removeSet[v] = struct{}{}
	}
	result := make([]T, 0, capacity)
	for _, v := range slice {
		if _, exists := removeSet[v]; !exists {
			result = append(result, v)
		}
	}
	return result
}

func Count[T any](slice []T, f func(T) bool) int {
	n := 0
	for _, v := range slice {
		if f(v) {
			n++
		}
	}
	return n
}

func Zip[T any, U any](a []T, b []U) []struct {
	First  T
	Second U
} {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	result := make([]struct {
		First  T
		Second U
	}, n)
	for i := 0; i < n; i++ {
		result[i].First = a[i]
		result[i].Second = b[i]
	}
	return result
}
