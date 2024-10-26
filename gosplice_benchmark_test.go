package gosplice

import (
	"testing"
)

func BenchmarkMap(b *testing.B) {
	slice := make([]int, 1000)
	for i := range slice {
		slice[i] = i
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Map(slice, func(x int) int { return x * 2 })
	}
}

func BenchmarkReduce(b *testing.B) {
	slice := make([]int, 1000)
	for i := range slice {
		slice[i] = i
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Reduce(slice, func(a, b int) int { return a + b }, 0)
	}
}

func BenchmarkFilter(b *testing.B) {
	slice := make([]int, 1000)
	for i := range slice {
		slice[i] = i
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Filter(slice, func(x int) bool { return x%2 == 0 })
	}
}

func BenchmarkSome(b *testing.B) {
	slice := make([]int, 1000)
	for i := range slice {
		slice[i] = i
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Some(slice, func(x int) bool { return x == 999 })
	}
}

func BenchmarkEvery(b *testing.B) {
	slice := make([]int, 1000)
	for i := range slice {
		slice[i] = i
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Every(slice, func(x int) bool { return x < 1000 })
	}
}

func BenchmarkFind(b *testing.B) {
	slice := make([]int, 1000)
	for i := range slice {
		slice[i] = i
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Find(slice, func(x int) bool { return x == 999 })
	}
}

func BenchmarkFindIndex(b *testing.B) {
	slice := make([]int, 1000)
	for i := range slice {
		slice[i] = i
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		FindIndex(slice, func(x int) bool { return x == 999 })
	}
}

func BenchmarkForEach(b *testing.B) {
	slice := make([]int, 1000)
	for i := range slice {
		slice[i] = i
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ForEach(slice, func(x int) { _ = x * 2 })
	}
}

func BenchmarkIncludes(b *testing.B) {
	slice := make([]int, 1000)
	for i := range slice {
		slice[i] = i
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Includes(slice, 999)
	}
}

func BenchmarkIndexOf(b *testing.B) {
	slice := make([]int, 1000)
	for i := range slice {
		slice[i] = i
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		IndexOf(slice, 999)
	}
}

func BenchmarkLastIndexOf(b *testing.B) {
	slice := make([]int, 1000)
	for i := range slice {
		slice[i] = i
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		LastIndexOf(slice, 999)
	}
}

func BenchmarkFlat(b *testing.B) {
	slice := make([][]int, 100)
	for i := range slice {
		slice[i] = make([]int, 10)
		for j := range slice[i] {
			slice[i][j] = j
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Flat(slice)
	}
}

func BenchmarkFlatMap(b *testing.B) {
	slice := make([]int, 100)
	for i := range slice {
		slice[i] = i
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		FlatMap(slice, func(x int) []int { return []int{x, x + 1} })
	}
}

func BenchmarkReverse(b *testing.B) {
	slice := make([]int, 1000)
	for i := range slice {
		slice[i] = i
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Reverse(slice)
	}
}

func BenchmarkUnique(b *testing.B) {
	slice := make([]int, 1000)
	for i := 0; i < len(slice); i++ {
		slice[i] = i % 100 // Add some duplicates
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Unique(slice)
	}
}

func BenchmarkChunk(b *testing.B) {
	slice := make([]int, 1000)
	for i := range slice {
		slice[i] = i
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Chunk(slice, 10)
	}
}

func BenchmarkRemove(b *testing.B) {
	slice := make([]int, 1000)
	remove := []int{1, 2, 3, 4, 5}
	for i := range slice {
		slice[i] = i
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Remove(slice, remove)
	}
}
