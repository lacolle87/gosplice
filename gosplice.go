package gosplice

// Map applies a function to each element of the slice and returns a new slice.
func Map[T any, U any](slice []T, f func(T) U) []U {
	result := make([]U, len(slice))
	for i := range slice {
		result[i] = f(slice[i])
	}
	return result
}

// Reduce reduces the slice to a single value using a function.
func Reduce[T any](slice []T, f func(T, T) T, initial T) T {
	result := initial
	for _, v := range slice {
		result = f(result, v)
	}
	return result
}

// Filter filters elements of the slice based on a function.
func Filter[T any](slice []T, f func(T) bool) []T {
	var result []T
	for _, v := range slice {
		if f(v) {
			result = append(result, v)
		}
	}
	return result
}

// Some checks if any element in the slice satisfies the predicate.
func Some[T any](slice []T, predicate func(T) bool) bool {
	for _, v := range slice {
		if predicate(v) {
			return true
		}
	}
	return false
}

// Every returns true if all elements in the slice satisfy the function.
func Every[T any](slice []T, f func(T) bool) bool {
	for _, v := range slice {
		if !f(v) {
			return false
		}
	}
	return true
}

// Find returns the first element that satisfies the function, along with a boolean indicating success.
func Find[T any](slice []T, f func(T) bool) (T, bool) {
	for _, v := range slice {
		if f(v) {
			return v, true
		}
	}
	var zero T
	return zero, false
}

// FindIndex returns the index of the first element that satisfies the function, or -1 if none found.
func FindIndex[T any](slice []T, f func(T) bool) int {
	for i, v := range slice {
		if f(v) {
			return i
		}
	}
	return -1
}

// ForEach applies a function to each element of the slice.
func ForEach[T any](slice []T, f func(T)) {
	for _, v := range slice {
		f(v)
	}
}

// Includes returns true if the value exists in the slice.
func Includes[T comparable](slice []T, value T) bool {
	for _, v := range slice {
		if v == value {
			return true
		}
	}
	return false
}

// IndexOf returns the index of the first occurrence of the value, or -1 if not found.
func IndexOf[T comparable](slice []T, value T) int {
	for i, v := range slice {
		if v == value {
			return i
		}
	}
	return -1
}

// LastIndexOf returns the index of the last occurrence of the value, or -1 if not found.
func LastIndexOf[T comparable](slice []T, value T) int {
	for i := len(slice) - 1; i >= 0; i-- {
		if slice[i] == value {
			return i
		}
	}
	return -1
}

// Flat flattens a 2D slice into a 1D slice.
func Flat[T any](slice [][]T) []T {
	var result []T
	for _, v := range slice {
		result = append(result, v...)
	}
	return result
}

// FlatMap applies a function to each element and flattens the result.
func FlatMap[T any, U any](slice []T, f func(T) []U) []U {
	var result []U
	for _, v := range slice {
		result = append(result, f(v)...)
	}
	return result
}

// Reverse reverses the elements of the slice.
func Reverse[T any](slice []T) []T {
	n := len(slice)
	res := make([]T, n)
	for i, v := range slice {
		res[n-1-i] = v
	}
	return res
}

// Unique returns a new slice with unique elements, preserving the order.
func Unique[T comparable](slice []T) []T {
	unique := make(map[T]struct{})
	var result []T

	for _, v := range slice {
		if _, exists := unique[v]; !exists {
			unique[v] = struct{}{}
			result = append(result, v)
		}
	}
	return result
}

// Chunk divides a slice into smaller slices of the specified size.
func Chunk[T any](slice []T, size int) [][]T {
	if size <= 0 {
		return nil
	}

	var chunks [][]T
	for i := 0; i < len(slice); i += size {
		end := i + size
		if end > len(slice) {
			end = len(slice)
		}
		chunks = append(chunks, slice[i:end])
	}
	return chunks
}

// Remove removes elements from the first slice based on the values in the second slice.
func Remove[T comparable](slice []T, remove []T) []T {
	removeMap := make(map[T]struct{}, len(remove))
	for _, v := range remove {
		removeMap[v] = struct{}{}
	}

	if len(slice) == 0 {
		return []T{}
	}

	capacity := len(slice) - len(remove)
	if capacity < 0 {
		capacity = 0
	}
	result := make([]T, 0, capacity)

	for _, v := range slice {
		if _, exists := removeMap[v]; !exists {
			result = append(result, v)
		}
	}
	return result
}
