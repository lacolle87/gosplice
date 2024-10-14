# GoSplice

`gosplice` is a Go package inspired by popular JavaScript libraries for arrays, providing a variety of utility functions to manipulate slices using generics. This package allows developers to work with slices in a more functional and expressive manner, similar to array methods available in JS.

## Installation

This project requires Go version 1.23.2. Ensure you have it installed before running the project.

To install `gosplice`, use the following command:

```bash
go get github.com/lacolle87/gosplice
```

## Functions

### 1. Map

```go
func Map[T any, U any](slice []T, f func(T) U) []U
```

Applies a function to each element of the slice and returns a new slice containing the results.

### 2. Reduce

```go
func Reduce[T any](slice []T, f func(T, T) T, initial T) T
```

Reduces the slice to a single value using a specified function and an initial value.

### 3. Filter

```go
func Filter[T any](slice []T, f func(T) bool) []T
```

Filters elements of the slice based on a predicate function, returning a new slice with only the elements that satisfy the condition.

### 4. Some

```go
func Some[T any](slice []T, predicate func(T) bool) bool
```

Checks if any element in the slice satisfies the given predicate function.

### 5. Every

```go
func Every[T any](slice []T, f func(T) bool) bool
```

Returns `true` if all elements in the slice satisfy the specified function; otherwise, returns `false`.

### 6. Find

```go
func Find[T any](slice []T, f func(T) bool) (T, bool)
```

Returns the first element that satisfies the predicate function, along with a boolean indicating success.

### 7. FindIndex

```go
func FindIndex[T any](slice []T, f func(T) bool) int
```

Returns the index of the first element that satisfies the predicate function, or `-1` if none found.

### 8. ForEach

```go
func ForEach[T any](slice []T, f func(T))
```

Applies a function to each element of the slice, performing side effects.

### 9. Includes

```go
func Includes[T comparable](slice []T, value T) bool
```

Returns `true` if the value exists in the slice; otherwise, returns `false`.

### 10. IndexOf

```go
func IndexOf[T comparable](slice []T, value T) int
```

Returns the index of the first occurrence of the value, or `-1` if not found.

### 11. LastIndexOf

```go
func LastIndexOf[T comparable](slice []T, value T) int
```

Returns the index of the last occurrence of the value, or `-1` if not found.

### 12. Flat

```go
func Flat[T any](slice [][]T) []T
```

Flattens a 2D slice into a 1D slice.

### 13. FlatMap

```go
func FlatMap[T any, U any](slice []T, f func(T) []U) []U
```

Applies a function to each element and flattens the result into a single slice.

### 14. Reverse

```go
func Reverse[T any](slice []T) []T
```

Reverses the elements of the slice in place.

### 15. Unique

```go
func Unique[T comparable](slice []T) []T
```

Returns a new slice containing only unique elements from the input slice, preserving their original order.


### 16. Chunk

```go
func Chunk[T any](slice []T, size int) [][]T
```

Divides a slice into smaller slices (chunks) of the specified size. If the input slice cannot be evenly divided, the last chunk will contain the remaining elements.

### 17. Remove

```go
func Remove[T comparable](slice []T, remove []T) []T
```

Returns a new slice with elements from the input `slice` that do not exist in the `remove` slice. This function allows for efficient removal of specified elements while preserving the original order of the remaining elements.

## Usage Example

Below is an examples of how to use some of the functions in the `gosplice` package:

```go
package main

import (
	"fmt"
	"github.com/lacolle87/gosplice"
)

func main() {
	// Example with integers
	nums := []int{1, 2, 3, 4, 5}

	// Map: square each number
	squared := gosplice.Map(nums, func(n int) int {
		return n * n
	})
	fmt.Println("Squared:", squared) // Output: Squared: [1 4 9 16 25]

	// Reduce: sum all numbers
	sum := gosplice.Reduce(nums, func(a, b int) int {
		return a + b
	}, 0)
	fmt.Println("Sum:", sum) // Output: Sum: 15

	// Filter: get even numbers
	even := gosplice.Filter(nums, func(n int) bool {
		return n%2 == 0
	})
	fmt.Println("Even numbers:", even) // Output: Even numbers: [2 4]

	// Some: check if any number is greater than 3
	hasGreaterThanThree := gosplice.Some(nums, func(n int) bool {
		return n > 3
	})
	fmt.Println("Any number greater than 3:", hasGreaterThanThree) // Output: true

	// Every: check if all numbers are positive
	allPositive := gosplice.Every(nums, func(n int) bool {
		return n > 0
	})
	fmt.Println("All numbers positive:", allPositive) // Output: true
}
```

```go
package main

import (
	"fmt"
	"github.com/lacolle87/gosplice"
	"strings"
)

func main() {
	// Example with strings
	names := []string{"Alice", "Bob", "Charlie", "John Doe"}

	// Original name
	name := "John Doe"

	// Transform the name to initials in one line
	initials := strings.Join(gosplice.Map(strings.Split(name, " "), func(part string) string {
		return strings.ToUpper(part[:1]) // Return the uppercase initial
	}), ". ") + "."

	// Output the initials
	fmt.Println("Initials:", initials) // Output: Initials: J. D.

	// Find: Get the first name that starts with 'C'
	foundName, found := gosplice.Find(names, func(name string) bool {
		return strings.HasPrefix(name, "C")
	})
	if found {
		fmt.Println("Found name starting with 'C':", foundName) // Output: Found name starting with 'C': Charlie
	}

	// ForEach: Print each name
	gosplice.ForEach(names, func(name string) {
		fmt.Println("Name:", name)
	})

	// Includes: Check if 'Alice' is in the list
	includesAlice := gosplice.Includes(names, "Alice")
	fmt.Println("Includes Alice:", includesAlice) // Output: true
}
```

## Testing

To see how to use the functions, refer to the tests in the `gosplice` package. The tests demonstrate various use cases for each function.

### Running Tests

You can run the tests using:

```bash
go test -v
```

## Generics

The `gosplice` package utilizes Go's generics feature, allowing you to work with any data type seamlessly.

## Contributions
Contributions are welcome! You can fork the repository, open issues, or submit pull requests. Please ensure your contributions align with the project's goals and coding standards.

## License

This project is licensed under the MIT License.