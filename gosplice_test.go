package gosplice

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestMap tests the Map function.
func TestMap(t *testing.T) {
	// Test case for integers: doubling each element
	input := []int{1, 2, 3}
	expected := []int{2, 4, 6}
	result := Map(input, func(n int) int {
		return n * 2
	})
	assert.Equal(t, expected, result)

	// Test case for strings: converting each string to uppercase
	strInput := []string{"hello", "world"}
	strExpected := []string{"HELLO", "WORLD"}
	strResult := Map(strInput, func(s string) string {
		return strings.ToUpper(s)
	})
	assert.Equal(t, strExpected, strResult)

	// Test case for floats: doubling each float
	floatInput := []float64{1.1, 2.2, 3.3}
	floatExpected := []float64{2.2, 4.4, 6.6}
	floatResult := Map(floatInput, func(f float64) float64 {
		return f * 2
	})
	assert.Equal(t, floatExpected, floatResult)
}

// TestReduce tests the Reduce function.
func TestReduce(t *testing.T) {
	// Test case for summing integers
	input := []int{1, 2, 3}
	expected := 6 // 1 + 2 + 3
	result := Reduce(input, func(acc int, n int) int {
		return acc + n
	}, 0)
	assert.Equal(t, expected, result)
}

// TestFilter tests the Filter function.
func TestFilter(t *testing.T) {
	// Test case for filtering even numbers
	input := []int{1, 2, 3, 4, 5}
	expected := []int{2, 4} // Even numbers in the input
	result := Filter(input, func(n int) bool {
		return n%2 == 0
	})
	assert.Equal(t, expected, result)
}

// TestSome tests the Some function.
func TestSome(t *testing.T) {
	// Test cases for integers
	intInput := []int{1, 3, 5}

	// Test case for no even numbers
	assert.False(t, Some(intInput, func(n int) bool {
		return n%2 == 0 // Should be false since there are no even numbers
	}))

	// Test case for numbers greater than 4
	assert.True(t, Some(intInput, func(n int) bool {
		return n > 4 // Should be true since 5 is greater than 4
	}))

	// Test cases for strings
	stringInput := []string{"apple", "banana", "cherry"}

	// Test case for no strings containing 'z'
	assert.False(t, Some(stringInput, func(s string) bool {
		return s == "z" // Should be false since 'z' is not present
	}))

	// Test case for at least one string starting with 'b'
	assert.True(t, Some(stringInput, func(s string) bool {
		return s[0] == 'b' // Should be true since 'banana' starts with 'b'
	}))
}

// TestEvery tests the Every function.
func TestEvery(t *testing.T) {
	// Test case for all even numbers
	input := []int{2, 4, 6}
	assert.True(t, Every(input, func(n int) bool {
		return n%2 == 0
	}))

	// Test case for not all numbers greater than 3
	assert.False(t, Every(input, func(n int) bool {
		return n > 3
	}))
}

// TestFind tests the Find function.
func TestFind(t *testing.T) {
	// Test case for finding a number greater than 3
	input := []int{1, 2, 3, 4, 5}
	expected, found := Find(input, func(n int) bool {
		return n > 3
	})
	assert.Equal(t, 4, expected) // Should find 4
	assert.True(t, found)

	// Test case for finding a number greater than 5
	_, found = Find(input, func(n int) bool {
		return n > 5
	})
	assert.False(t, found) // Should not find any number greater than 5
}

// TestFindIndex tests the FindIndex function.
func TestFindIndex(t *testing.T) {
	// Test case for finding the index of a specific number
	input := []int{10, 20, 30, 40}
	assert.Equal(t, 2, FindIndex(input, func(n int) bool {
		return n == 30
	})) // Should find index 2 for number 30

	// Test case for a number not present in the slice
	assert.Equal(t, -1, FindIndex(input, func(n int) bool {
		return n == 100
	})) // Should return -1 for non-existent number
}

// TestForEach tests the ForEach function.
func TestForEach(t *testing.T) {
	// Test case for applying a function to each element
	var results []int
	input := []int{1, 2, 3}
	ForEach(input, func(n int) {
		results = append(results, n*2) // Doubling each number
	})
	expected := []int{2, 4, 6} // Expected results
	assert.Equal(t, expected, results)
}

// TestIncludes tests the Includes function.
func TestIncludes(t *testing.T) {
	// Test case for checking if a value exists in the slice
	input := []int{1, 2, 3, 4, 5}
	assert.True(t, Includes(input, 3))  // Should return true for 3
	assert.False(t, Includes(input, 6)) // Should return false for 6
}

// TestIndexOf tests the IndexOf function.
func TestIndexOf(t *testing.T) {
	// Test case for finding the index of a specific string
	input := []string{"apple", "banana", "cherry"}
	assert.Equal(t, 1, IndexOf(input, "banana")) // Should find index 1 for 'banana'
	assert.Equal(t, -1, IndexOf(input, "grape")) // Should return -1 for non-existent string
}

// TestLastIndexOf tests the LastIndexOf function.
func TestLastIndexOf(t *testing.T) {
	// Test case for finding the last index of a specific number
	input := []int{1, 2, 3, 2, 1}
	assert.Equal(t, 3, LastIndexOf(input, 2))  // Should find last index 3 for '2'
	assert.Equal(t, -1, LastIndexOf(input, 4)) // Should return -1 for non-existent number
}

// TestFlat tests the Flat function.
func TestFlat(t *testing.T) {
	// Test case for flattening a slice of slices
	input := [][]int{{1, 2}, {3, 4}, {5}}
	expected := []int{1, 2, 3, 4, 5} // Expected flat result
	result := Flat(input)
	assert.Equal(t, expected, result)
}

// TestFlatMap tests the FlatMap function.
func TestFlatMap(t *testing.T) {
	// Test case for mapping and flattening a slice of integers
	input := []int{1, 2, 3}
	expected := []int{1, 1, 2, 2, 3, 3} // Corrected expected result after duplicating each number
	result := FlatMap(input, func(n int) []int {
		return []int{n, n} // Duplicating each number
	})
	assert.Equal(t, expected, result)

	// Test case with an empty slice
	var emptyInput []int
	var emptyExpected []int // Should return an empty slice
	emptyResult := FlatMap(emptyInput, func(n int) []int {
		return []int{n, n}
	})
	assert.Equal(t, emptyExpected, emptyResult) // Check for empty slice

	// Test case with negative integers
	negativeInput := []int{-1, -2, -3}
	negativeExpected := []int{-1, -1, -2, -2, -3, -3} // Duplicated each negative number
	negativeResult := FlatMap(negativeInput, func(n int) []int {
		return []int{n, n}
	})
	assert.Equal(t, negativeExpected, negativeResult)

	// Test case for mapping with varying lengths
	varyingInput := []int{4, 5}
	varyingExpected := []int{4, 4, 5} // Should return 4 twice, and 5 once
	varyingResult := FlatMap(varyingInput, func(n int) []int {
		if n == 4 {
			return []int{n, n} // Duplicating 4
		}
		return []int{n} // Returning 5 once
	})
	assert.Equal(t, varyingExpected, varyingResult)

	// Test case for mapping that returns varying slices
	varyingSizeInput := []int{1, 2, 3}
	varyingSizeExpected := []int{1, 2, 2, 3, 3, 3} // Expected output
	varyingSizeResult := FlatMap(varyingSizeInput, func(n int) []int {
		varyingResultSlices := make([]int, 0, n) // Create a slice with capacity `n`
		for i := 0; i < n; i++ {
			varyingResultSlices = append(varyingResultSlices, n) // Append `n` to the result
		}
		return varyingResultSlices // Return the filled slice
	})
	assert.Equal(t, varyingSizeExpected, varyingSizeResult)

	// Test case with large numbers
	largeInput := []int{1000000, 2000000}
	largeExpected := []int{1000000, 1000000, 2000000, 2000000} // Each number duplicated
	largeResult := FlatMap(largeInput, func(n int) []int {
		return []int{n, n}
	})
	assert.Equal(t, largeExpected, largeResult)

	// Test cases for mapping and flattening a slice of strings
	stringInput := []string{"hello", "world"}
	stringExpected := []string{"hello", "hello", "world", "world"} // Duplicated each string
	stringResult := FlatMap(stringInput, func(s string) []string {
		return []string{s, s} // Duplicating each string
	})
	assert.Equal(t, stringExpected, stringResult)

	// Test case with an empty slice of strings
	var emptyStringInput []string
	var emptyStringExpected []string // Should return an empty slice
	emptyStringResult := FlatMap(emptyStringInput, func(s string) []string {
		return []string{s, s}
	})
	assert.Equal(t, emptyStringExpected, emptyStringResult) // Check for empty slice

	// Test case with string manipulation (concatenation)
	concatInput := []string{"foo", "bar"}
	concatExpected := []string{"foo1", "foo2", "bar1", "bar2"} // Concatenating 1 and 2 to each string
	concatResult := FlatMap(concatInput, func(s string) []string {
		return []string{s + "1", s + "2"} // Duplicating and modifying each string
	})
	assert.Equal(t, concatExpected, concatResult)
}

// TestReverse tests the Reverse function.
func TestReverse(t *testing.T) {
	// Standard test case for integers
	intInput := []int{1, 2, 3, 4, 5}
	intExpected := []int{5, 4, 3, 2, 1}
	intResult := Reverse(intInput)
	assert.Equal(t, intExpected, intResult)

	// Empty slice test case for integers
	intInput = []int{}
	intExpected = []int{}
	intResult = Reverse(intInput)
	assert.Equal(t, intExpected, intResult)

	// Single element slice test case for integers
	intInput = []int{42}
	intExpected = []int{42}
	intResult = Reverse(intInput)
	assert.Equal(t, intExpected, intResult)

	// Negative numbers test case for integers
	intInput = []int{-1, -2, -3, -4, -5}
	intExpected = []int{-5, -4, -3, -2, -1}
	intResult = Reverse(intInput)
	assert.Equal(t, intExpected, intResult)

	// Test case for strings
	strInput := []string{"a", "b", "c", "d", "e"}
	strExpected := []string{"e", "d", "c", "b", "a"}
	strResult := Reverse(strInput)
	assert.Equal(t, strExpected, strResult)

	// Test case for empty slice of strings
	strInput = []string{}
	strExpected = []string{}
	strResult = Reverse(strInput)
	assert.Equal(t, strExpected, strResult)

	// Single element slice test case for strings
	strInput = []string{"x"}
	strExpected = []string{"x"}
	strResult = Reverse(strInput)
	assert.Equal(t, strExpected, strResult)

	// Two elements slice test case for strings
	strInput = []string{"hello", "world"}
	strExpected = []string{"world", "hello"}
	strResult = Reverse(strInput)
	assert.Equal(t, strExpected, strResult)
}

func TestUnique(t *testing.T) {
	// Test with integers
	intInput := []int{1, 2, 2, 3, 4, 4, 5}
	intExpected := []int{1, 2, 3, 4, 5}
	intResult := Unique(intInput)
	assert.Equal(t, intExpected, intResult)

	// Test with strings
	stringInput := []string{"Alice", "Bob", "Alice", "Charlie", "Bob"}
	stringExpected := []string{"Alice", "Bob", "Charlie"}
	stringResult := Unique(stringInput)
	assert.Equal(t, stringExpected, stringResult)
}
