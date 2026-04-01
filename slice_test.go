package gosplice

import (
	"strings"
	"testing"
)

func TestMap(t *testing.T) {
	assertSliceEqual(t, []int{2, 4, 6}, Map([]int{1, 2, 3}, func(n int) int { return n * 2 }))
	assertSliceEqual(t, []string{"HELLO", "WORLD"}, Map([]string{"hello", "world"}, strings.ToUpper))
	assertSliceEqual(t, []int{}, Map([]int{}, func(n int) int { return n }))
}

func TestMapInPlace(t *testing.T) {
	s := []int{1, 2, 3}
	MapInPlace(s, func(n int) int { return n * 3 })
	assertSliceEqual(t, []int{3, 6, 9}, s)
}

func TestMapInPlaceEmpty(t *testing.T) {
	MapInPlace([]int{}, func(n int) int { return n })
}

func TestReduce(t *testing.T) {
	sum := Reduce([]int{1, 2, 3, 4}, 0, func(acc, n int) int { return acc + n })
	if sum != 10 {
		t.Errorf("expected 10, got %d", sum)
	}
}

func TestReduceCrossTypeStandalone(t *testing.T) {
	total := Reduce([]string{"ab", "cde", "f"}, 0, func(acc int, s string) int { return acc + len(s) })
	if total != 6 {
		t.Errorf("expected 6, got %d", total)
	}
}

func TestReduceToMapStandalone(t *testing.T) {
	words := []string{"go", "is", "go", "fun"}
	freq := Reduce(words, make(map[string]int), func(m map[string]int, w string) map[string]int {
		m[w]++
		return m
	})
	if freq["go"] != 2 || freq["is"] != 1 || freq["fun"] != 1 {
		t.Errorf("unexpected: %v", freq)
	}
}

func TestReduceEmpty(t *testing.T) {
	r := Reduce([]int{}, 42, func(a, b int) int { return a + b })
	if r != 42 {
		t.Errorf("expected initial value 42, got %d", r)
	}
}

func TestFilter(t *testing.T) {
	assertSliceEqual(t, []int{2, 4}, Filter([]int{1, 2, 3, 4, 5}, func(n int) bool { return n%2 == 0 }))
}

func TestFilterNoneMatch(t *testing.T) {
	r := Filter([]int{1, 3, 5}, func(n int) bool { return n%2 == 0 })
	if len(r) != 0 {
		t.Errorf("expected empty, got %v", r)
	}
}

func TestFilterEmpty(t *testing.T) {
	r := Filter([]int{}, func(n int) bool { return true })
	if r != nil {
		t.Errorf("expected nil for empty input, got %v", r)
	}
}

func TestFilterInPlace(t *testing.T) {
	s := []int{1, 2, 3, 4, 5, 6}
	r := FilterInPlace(s, func(n int) bool { return n%2 == 0 })
	assertSliceEqual(t, []int{2, 4, 6}, r)
}

func TestFilterInPlaceAll(t *testing.T) {
	s := []int{1, 2, 3}
	assertSliceEqual(t, []int{1, 2, 3}, FilterInPlace(s, func(n int) bool { return true }))
}

func TestFilterInPlaceNone(t *testing.T) {
	s := []int{1, 2, 3}
	r := FilterInPlace(s, func(n int) bool { return false })
	if len(r) != 0 {
		t.Errorf("expected empty, got %v", r)
	}
}

func TestSome(t *testing.T) {
	if !Some([]int{1, 2, 3}, func(n int) bool { return n == 2 }) {
		t.Error("expected true")
	}
	if Some([]int{1, 3, 5}, func(n int) bool { return n%2 == 0 }) {
		t.Error("expected false")
	}
	if Some([]int{}, func(n int) bool { return true }) {
		t.Error("expected false for empty")
	}
}

func TestEvery(t *testing.T) {
	if !Every([]int{2, 4, 6}, func(n int) bool { return n%2 == 0 }) {
		t.Error("expected true")
	}
	if Every([]int{2, 3, 6}, func(n int) bool { return n%2 == 0 }) {
		t.Error("expected false")
	}
	if !Every([]int{}, func(n int) bool { return false }) {
		t.Error("expected true for empty (vacuous truth)")
	}
}

func TestFind(t *testing.T) {
	v, ok := Find([]int{1, 2, 3, 4}, func(n int) bool { return n > 2 })
	if !ok || v != 3 {
		t.Errorf("expected (3, true), got (%d, %v)", v, ok)
	}
	_, ok = Find([]int{1, 2}, func(n int) bool { return n > 10 })
	if ok {
		t.Error("expected false")
	}
}

func TestFindIndex(t *testing.T) {
	idx := FindIndex([]int{10, 20, 30}, func(n int) bool { return n == 20 })
	if idx != 1 {
		t.Errorf("expected 1, got %d", idx)
	}
	if FindIndex([]int{10, 20}, func(n int) bool { return n == 99 }) != -1 {
		t.Error("expected -1")
	}
}

func TestFindLast(t *testing.T) {
	v, ok := FindLast([]int{1, 2, 3, 4, 3}, func(n int) bool { return n == 3 })
	if !ok || v != 3 {
		t.Errorf("expected (3, true), got (%d, %v)", v, ok)
	}
	_, ok = FindLast([]int{1, 2}, func(n int) bool { return n > 10 })
	if ok {
		t.Error("expected false")
	}
}

func TestForEachStandalone(t *testing.T) {
	var sum int
	ForEach([]int{1, 2, 3}, func(n int) { sum += n })
	if sum != 6 {
		t.Errorf("expected 6, got %d", sum)
	}
}

func TestIncludes(t *testing.T) {
	if !Includes([]int{1, 2, 3}, 2) {
		t.Error("expected true")
	}
	if Includes([]int{1, 2, 3}, 4) {
		t.Error("expected false")
	}
	if Includes([]int{}, 1) {
		t.Error("expected false for empty")
	}
}

func TestIndexOf(t *testing.T) {
	if IndexOf([]string{"a", "b", "c"}, "b") != 1 {
		t.Error("expected 1")
	}
	if IndexOf([]string{"a", "b"}, "z") != -1 {
		t.Error("expected -1")
	}
}

func TestLastIndexOf(t *testing.T) {
	if LastIndexOf([]int{1, 2, 3, 2, 1}, 2) != 3 {
		t.Error("expected 3")
	}
	if LastIndexOf([]int{1, 2, 3}, 9) != -1 {
		t.Error("expected -1")
	}
}

func TestFlat(t *testing.T) {
	assertSliceEqual(t, []int{1, 2, 3, 4, 5}, Flat([][]int{{1, 2}, {3, 4}, {5}}))
	assertSliceEqual(t, []int{}, Flat([][]int{{}, {}, {}}))
}

func TestFlatMap(t *testing.T) {
	r := FlatMap([]int{1, 2, 3}, func(n int) []int { return []int{n, n * 10} })
	assertSliceEqual(t, []int{1, 10, 2, 20, 3, 30}, r)
}

func TestFlatMapEmpty(t *testing.T) {
	if FlatMap([]int{}, func(n int) []int { return []int{n} }) != nil {
		t.Error("expected nil")
	}
}

func TestReverse(t *testing.T) {
	assertSliceEqual(t, []int{3, 2, 1}, Reverse([]int{1, 2, 3}))
	assertSliceEqual(t, []int{}, Reverse([]int{}))
	assertSliceEqual(t, []int{42}, Reverse([]int{42}))
}

func TestReverseInPlace(t *testing.T) {
	s := []int{1, 2, 3, 4, 5}
	ReverseInPlace(s)
	assertSliceEqual(t, []int{5, 4, 3, 2, 1}, s)
}

func TestReverseInPlaceEmpty(t *testing.T) {
	ReverseInPlace([]int{})
}

func TestUnique(t *testing.T) {
	assertSliceEqual(t, []int{1, 2, 3, 4, 5}, Unique([]int{1, 2, 2, 3, 4, 4, 5}))
	assertSliceEqual(t, []string{"a", "b"}, Unique([]string{"a", "b", "a"}))
	assertSliceEqual(t, []int{1}, Unique([]int{1}))
	assertSliceEqual(t, []int{}, Unique([]int{}))
}

func TestChunk(t *testing.T) {
	c := Chunk([]int{1, 2, 3, 4, 5}, 2)
	if len(c) != 3 {
		t.Fatalf("expected 3 chunks, got %d", len(c))
	}
	assertSliceEqual(t, []int{1, 2}, c[0])
	assertSliceEqual(t, []int{3, 4}, c[1])
	assertSliceEqual(t, []int{5}, c[2])

	if Chunk([]int{}, 2) != nil {
		t.Error("expected nil for empty")
	}
	if Chunk([]int{1, 2}, 0) != nil {
		t.Error("expected nil for size 0")
	}
}

func TestRemove(t *testing.T) {
	assertSliceEqual(t, []int{1, 3, 5}, Remove([]int{1, 2, 3, 4, 5}, []int{2, 4}))
	assertSliceEqual(t, []int{1, 2, 3}, Remove([]int{1, 2, 3}, []int{}))
	if len(Remove([]int{}, []int{1})) != 0 {
		t.Error("expected empty")
	}
}

func TestRemoveLargeSet(t *testing.T) {
	s := make([]int, 100)
	for i := range s {
		s[i] = i
	}
	rm := make([]int, 20)
	for i := range rm {
		rm[i] = i * 5
	}
	result := Remove(s, rm)
	for _, v := range result {
		if v%5 == 0 && v < 100 {
			t.Errorf("element %d should have been removed", v)
		}
	}
}

func TestCount(t *testing.T) {
	if Count([]int{1, 2, 3, 4, 5}, func(n int) bool { return n%2 == 0 }) != 2 {
		t.Error("expected 2")
	}
	if Count([]int{}, func(n int) bool { return true }) != 0 {
		t.Error("expected 0")
	}
}

func TestZip(t *testing.T) {
	r := Zip([]int{1, 2, 3}, []string{"a", "b", "c"})
	if len(r) != 3 || r[0].First != 1 || r[0].Second != "a" || r[2].First != 3 || r[2].Second != "c" {
		t.Errorf("unexpected: %v", r)
	}
}

func TestZipUnequal(t *testing.T) {
	r := Zip([]int{1, 2, 3, 4}, []string{"a", "b"})
	if len(r) != 2 {
		t.Errorf("expected 2, got %d", len(r))
	}
}

func assertSliceEqual[T comparable](t *testing.T, expected, actual []T) {
	t.Helper()
	if len(expected) != len(actual) {
		t.Errorf("length mismatch: expected %d, got %d\nexpected: %v\nactual:   %v", len(expected), len(actual), expected, actual)
		return
	}
	for i := range expected {
		if expected[i] != actual[i] {
			t.Errorf("mismatch at index %d: expected %v, got %v", i, expected[i], actual[i])
		}
	}
}
