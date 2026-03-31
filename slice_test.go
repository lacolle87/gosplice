package gosplice

import "testing"

func TestMapInPlace(t *testing.T) {
	input := []int{1, 2, 3, 4, 5}
	MapInPlace(input, func(n int) int { return n * 3 })
	expected := []int{3, 6, 9, 12, 15}
	assertSliceEqual(t, expected, input)
}

func TestMapInPlaceEmpty(t *testing.T) {
	var input []int
	MapInPlace(input, func(n int) int { return n * 3 })
	if len(input) != 0 {
		t.Error("expected empty")
	}
}

func TestFilterPrealloc(t *testing.T) {
	input := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	result := Filter(input, func(n int) bool { return n%2 == 0 })
	assertSliceEqual(t, []int{2, 4, 6, 8, 10}, result)
}

func TestFilterEmpty(t *testing.T) {
	var input []int
	result := Filter(input, func(n int) bool { return true })
	if result != nil {
		t.Errorf("expected nil for empty input, got %v", result)
	}
}

func TestFilterInPlace(t *testing.T) {
	input := []int{1, 2, 3, 4, 5, 6}
	result := FilterInPlace(input, func(n int) bool { return n%2 == 0 })
	assertSliceEqual(t, []int{2, 4, 6}, result)
}

func TestFilterInPlaceAll(t *testing.T) {
	input := []int{1, 2, 3}
	result := FilterInPlace(input, func(n int) bool { return true })
	assertSliceEqual(t, []int{1, 2, 3}, result)
}

func TestFilterInPlaceNone(t *testing.T) {
	input := []int{1, 2, 3}
	result := FilterInPlace(input, func(n int) bool { return false })
	if len(result) != 0 {
		t.Errorf("expected empty, got %v", result)
	}
}

func TestFindLast(t *testing.T) {
	input := []int{1, 2, 3, 4, 5}
	v, ok := FindLast(input, func(n int) bool { return n%2 == 0 })
	if !ok || v != 4 {
		t.Errorf("expected (4, true), got (%d, %v)", v, ok)
	}
}

func TestFindLastNotFound(t *testing.T) {
	input := []int{1, 3, 5}
	_, ok := FindLast(input, func(n int) bool { return n%2 == 0 })
	if ok {
		t.Error("expected false")
	}
}

func TestReverseInPlace(t *testing.T) {
	input := []int{1, 2, 3, 4, 5}
	ReverseInPlace(input)
	assertSliceEqual(t, []int{5, 4, 3, 2, 1}, input)
}

func TestReverseInPlaceEven(t *testing.T) {
	input := []int{1, 2, 3, 4}
	ReverseInPlace(input)
	assertSliceEqual(t, []int{4, 3, 2, 1}, input)
}

func TestReverseInPlaceEmpty(t *testing.T) {
	var input []int
	ReverseInPlace(input)
	if len(input) != 0 {
		t.Error("expected empty")
	}
}

func TestReverseInPlaceSingle(t *testing.T) {
	input := []int{42}
	ReverseInPlace(input)
	assertSliceEqual(t, []int{42}, input)
}

func TestFlatCopy(t *testing.T) {
	input := [][]int{{1, 2}, {3, 4}, {5}}
	expected := []int{1, 2, 3, 4, 5}
	result := Flat(input)
	assertSliceEqual(t, expected, result)
}

func TestFlatEmpty(t *testing.T) {
	input := [][]int{{}, {}, {}}
	result := Flat(input)
	if len(result) != 0 {
		t.Errorf("expected empty, got %v", result)
	}
}

func TestRemoveLinearPath(t *testing.T) {
	input := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	result := Remove(input, []int{2, 5, 8})
	assertSliceEqual(t, []int{1, 3, 4, 6, 7, 9, 10}, result)
}

func TestRemoveMapPath(t *testing.T) {
	input := make([]int, 100)
	for i := range input {
		input[i] = i
	}
	rm := make([]int, 20)
	for i := range rm {
		rm[i] = i * 5
	}
	result := Remove(input, rm)
	for _, v := range result {
		if v%5 == 0 && v < 100 {
			t.Errorf("element %d should have been removed", v)
		}
	}
}

func TestRemoveEmpty(t *testing.T) {
	result := Remove([]int{1, 2, 3}, []int{})
	assertSliceEqual(t, []int{1, 2, 3}, result)
}

func TestRemoveFromEmpty(t *testing.T) {
	result := Remove([]int{}, []int{1, 2})
	if len(result) != 0 {
		t.Errorf("expected empty, got %v", result)
	}
}

func TestCount(t *testing.T) {
	input := []int{1, 2, 3, 4, 5, 6}
	n := Count(input, func(v int) bool { return v%2 == 0 })
	if n != 3 {
		t.Errorf("expected 3, got %d", n)
	}
}

func TestCountNone(t *testing.T) {
	input := []int{1, 3, 5}
	n := Count(input, func(v int) bool { return v%2 == 0 })
	if n != 0 {
		t.Errorf("expected 0, got %d", n)
	}
}

func TestZip(t *testing.T) {
	a := []int{1, 2, 3}
	b := []string{"a", "b", "c"}
	result := Zip(a, b)

	if len(result) != 3 {
		t.Fatalf("expected 3 pairs, got %d", len(result))
	}
	if result[0].First != 1 || result[0].Second != "a" {
		t.Errorf("pair 0 mismatch")
	}
	if result[2].First != 3 || result[2].Second != "c" {
		t.Errorf("pair 2 mismatch")
	}
}

func TestZipUnequal(t *testing.T) {
	a := []int{1, 2, 3, 4, 5}
	b := []string{"a", "b"}
	result := Zip(a, b)

	if len(result) != 2 {
		t.Fatalf("expected 2 pairs (min length), got %d", len(result))
	}
}

func TestChunkDirectIndex(t *testing.T) {
	input := []int{1, 2, 3, 4, 5, 6, 7}
	chunks := Chunk(input, 3)
	if len(chunks) != 3 {
		t.Fatalf("expected 3 chunks, got %d", len(chunks))
	}
	assertSliceEqual(t, []int{1, 2, 3}, chunks[0])
	assertSliceEqual(t, []int{4, 5, 6}, chunks[1])
	assertSliceEqual(t, []int{7}, chunks[2])
}

func TestFlatMapPrealloc(t *testing.T) {
	input := []int{1, 2, 3}
	result := FlatMap(input, func(n int) []int { return []int{n, n * 10} })
	assertSliceEqual(t, []int{1, 10, 2, 20, 3, 30}, result)
}

func TestFlatMapEmpty(t *testing.T) {
	var input []int
	result := FlatMap(input, func(n int) []int { return []int{n} })
	if result != nil {
		t.Errorf("expected nil, got %v", result)
	}
}
