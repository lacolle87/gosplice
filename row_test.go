package gosplice

import (
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// Row access
// ---------------------------------------------------------------------------

func TestRow_Get(t *testing.T) {
	r := Row{
		header: []string{"name", "age", "city"},
		fields: []string{"Alice", "30", "Berlin"},
		colIdx: map[string]int{"name": 0, "age": 1, "city": 2},
	}
	if r.Get("name") != "Alice" {
		t.Errorf("expected Alice, got %q", r.Get("name"))
	}
	if r.Get("age") != "30" {
		t.Errorf("expected 30, got %q", r.Get("age"))
	}
	if r.Get("missing") != "" {
		t.Errorf("expected empty for missing column, got %q", r.Get("missing"))
	}
}

func TestRow_Index(t *testing.T) {
	r := Row{fields: []string{"a", "b", "c"}}
	if r.Index(0) != "a" || r.Index(2) != "c" {
		t.Error("index access failed")
	}
	if r.Index(-1) != "" || r.Index(99) != "" {
		t.Error("out of bounds should return empty")
	}
}

func TestRow_GetInt(t *testing.T) {
	r := Row{
		header: []string{"id", "name"},
		fields: []string{"42", "Alice"},
		colIdx: map[string]int{"id": 0, "name": 1},
	}
	v, err := r.GetInt("id")
	if err != nil || v != 42 {
		t.Errorf("expected 42, got %d err=%v", v, err)
	}
	_, err = r.GetInt("name")
	if err == nil {
		t.Error("expected error for non-numeric")
	}
	_, err = r.GetInt("missing")
	if err == nil {
		t.Error("expected error for missing column")
	}
}

func TestRow_GetFloat(t *testing.T) {
	r := Row{
		header: []string{"price"},
		fields: []string{"19.99"},
		colIdx: map[string]int{"price": 0},
	}
	v, err := r.GetFloat("price")
	if err != nil || v != 19.99 {
		t.Errorf("expected 19.99, got %f err=%v", v, err)
	}
}

func TestRow_GetBool(t *testing.T) {
	r := Row{
		header: []string{"active", "deleted"},
		fields: []string{"true", "0"},
		colIdx: map[string]int{"active": 0, "deleted": 1},
	}
	v, err := r.GetBool("active")
	if err != nil || !v {
		t.Errorf("expected true, got %v err=%v", v, err)
	}
	v, err = r.GetBool("deleted")
	if err != nil || v {
		t.Errorf("expected false, got %v err=%v", v, err)
	}
}

func TestRow_Has(t *testing.T) {
	r := Row{colIdx: map[string]int{"name": 0}}
	if !r.Has("name") {
		t.Error("expected true")
	}
	if r.Has("nope") {
		t.Error("expected false")
	}
}

func TestRow_Len(t *testing.T) {
	r := Row{fields: []string{"a", "b", "c"}}
	if r.Len() != 3 {
		t.Errorf("expected 3, got %d", r.Len())
	}
}

func TestRow_Fields(t *testing.T) {
	original := []string{"a", "b"}
	r := Row{fields: original}
	f := r.Fields()
	f[0] = "mutated"
	if original[0] != "a" {
		t.Error("Fields() should return a copy")
	}
}

func TestRow_AsMap(t *testing.T) {
	r := Row{
		header: []string{"x", "y"},
		fields: []string{"1", "2"},
		colIdx: map[string]int{"x": 0, "y": 1},
	}
	m := r.AsMap()
	if m["x"] != "1" || m["y"] != "2" {
		t.Errorf("unexpected map: %v", m)
	}
}

func TestRow_Columns(t *testing.T) {
	r := Row{header: []string{"a", "b"}}
	if len(r.Columns()) != 2 {
		t.Errorf("expected 2 columns, got %d", len(r.Columns()))
	}
}

func TestRow_String(t *testing.T) {
	r := Row{
		header: []string{"name", "age"},
		fields: []string{"Alice", "30"},
		colIdx: map[string]int{"name": 0, "age": 1},
	}
	s := r.String()
	if !strings.Contains(s, "name:Alice") || !strings.Contains(s, "age:30") {
		t.Errorf("unexpected String(): %s", s)
	}
}

func TestRow_String_NoHeader(t *testing.T) {
	r := Row{fields: []string{"a", "b"}}
	s := r.String()
	if !strings.Contains(s, "Row[a b]") {
		t.Errorf("unexpected String(): %s", s)
	}
}

// ---------------------------------------------------------------------------
// FromCSVRows — pipeline integration
// ---------------------------------------------------------------------------

func TestFromCSVRows_WithHeader(t *testing.T) {
	input := "name,age,city\nAlice,30,Berlin\nBob,25,Paris\n"
	rows := FromCSVRows(strings.NewReader(input), CSVConfig{Header: true}).Collect()

	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if rows[0].Get("name") != "Alice" {
		t.Errorf("expected Alice, got %q", rows[0].Get("name"))
	}
	if rows[1].Get("city") != "Paris" {
		t.Errorf("expected Paris, got %q", rows[1].Get("city"))
	}
}

func TestFromCSVRows_NoHeader(t *testing.T) {
	input := "Alice,30\nBob,25\n"
	rows := FromCSVRows(strings.NewReader(input), CSVConfig{Header: false}).Collect()

	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if rows[0].Index(0) != "Alice" {
		t.Errorf("expected Alice, got %q", rows[0].Index(0))
	}
	if rows[0].Has("name") {
		t.Error("should not have named columns without header")
	}
}

func TestFromCSVRows_Empty(t *testing.T) {
	rows := FromCSVRows(strings.NewReader(""), CSVConfig{}).Collect()
	if len(rows) != 0 {
		t.Fatalf("expected empty, got %d", len(rows))
	}
}

func TestFromCSVRows_HeaderOnly(t *testing.T) {
	rows := FromCSVRows(strings.NewReader("a,b,c\n"), CSVConfig{Header: true}).Collect()
	if len(rows) != 0 {
		t.Fatalf("expected empty, got %d", len(rows))
	}
}

func TestFromCSVRows_Filter(t *testing.T) {
	input := "name,age,active\nAlice,30,true\nBob,25,false\nCharlie,35,true\n"
	rows := FromCSVRows(strings.NewReader(input), CSVConfig{Header: true}).
		Filter(func(r Row) bool { return r.Get("active") == "true" }).
		Collect()

	if len(rows) != 2 {
		t.Fatalf("expected 2, got %d", len(rows))
	}
	if rows[0].Get("name") != "Alice" || rows[1].Get("name") != "Charlie" {
		t.Errorf("unexpected filter result: %v, %v", rows[0].Get("name"), rows[1].Get("name"))
	}
}

func TestFromCSVRows_PipeMap(t *testing.T) {
	input := "name,score\nAlice,95\nBob,87\nCharlie,92\n"
	names := PipeMap(
		FromCSVRows(strings.NewReader(input), CSVConfig{Header: true}).
			Filter(func(r Row) bool {
				score, _ := r.GetInt("score")
				return score >= 90
			}),
		func(r Row) string { return r.Get("name") },
	).Collect()

	assertSliceEqual(t, []string{"Alice", "Charlie"}, names)
}

func TestFromCSVRows_GetFloat_Pipeline(t *testing.T) {
	input := "product,price\nApple,1.50\nBanana,0.75\nCherry,3.20\n"
	total := PipeReduce(
		FromCSVRows(strings.NewReader(input), CSVConfig{Header: true}),
		0.0,
		func(sum float64, r Row) float64 {
			v, _ := r.GetFloat("price")
			return sum + v
		},
	)
	if total != 5.45 {
		t.Errorf("expected 5.45, got %f", total)
	}
}

func TestFromCSVRows_GroupBy(t *testing.T) {
	input := "name,dept\nAlice,eng\nBob,sales\nCharlie,eng\nDiana,sales\nEve,eng\n"
	groups := GroupBy(
		FromCSVRows(strings.NewReader(input), CSVConfig{Header: true}),
		func(r Row) string { return r.Get("dept") },
	)
	if len(groups["eng"]) != 3 {
		t.Errorf("expected 3 in eng, got %d", len(groups["eng"]))
	}
	if len(groups["sales"]) != 2 {
		t.Errorf("expected 2 in sales, got %d", len(groups["sales"]))
	}
}

func TestFromCSVRows_CustomDelimiter(t *testing.T) {
	input := "name;age\nAlice;30\n"
	rows := FromCSVRows(strings.NewReader(input), CSVConfig{Header: true, Comma: ';'}).Collect()
	if len(rows) != 1 || rows[0].Get("name") != "Alice" {
		t.Errorf("unexpected: %v", rows)
	}
}

func TestFromCSVRows_VariableLengthRows(t *testing.T) {
	input := "a,b,c\n1,2,3\n4,5\n6,7,8,9\n"
	rows := FromCSVRows(strings.NewReader(input), CSVConfig{Header: true}).Collect()
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
	// Short row — missing column returns ""
	if rows[1].Get("c") != "" {
		t.Errorf("expected empty for short row, got %q", rows[1].Get("c"))
	}
	// Long row — extra field accessible by index
	if rows[2].Index(3) != "9" {
		t.Errorf("expected 9 at index 3, got %q", rows[2].Index(3))
	}
}

func TestFromCSVRows_AsMap_InPipeline(t *testing.T) {
	input := "k,v\nfoo,1\nbar,2\n"
	maps := PipeMap(
		FromCSVRows(strings.NewReader(input), CSVConfig{Header: true}),
		func(r Row) map[string]string { return r.AsMap() },
	).Collect()

	if len(maps) != 2 {
		t.Fatalf("expected 2, got %d", len(maps))
	}
	if maps[0]["k"] != "foo" || maps[0]["v"] != "1" {
		t.Errorf("unexpected: %v", maps[0])
	}
}

func TestFromCSVRows_Err(t *testing.T) {
	// Well-formed CSV — no error
	p := FromCSVRows(strings.NewReader("a,b\n1,2\n"), CSVConfig{Header: true})
	_ = p.Collect()
	if p.Err() != nil {
		t.Errorf("unexpected error: %v", p.Err())
	}
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

func BenchmarkFromCSVRows_1k(b *testing.B) {
	data := buildCSVData(1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		FromCSVRows(strings.NewReader(data), CSVConfig{Header: true}).
			Filter(func(r Row) bool { return r.Get("status") == "completed" }).
			Collect()
	}
}

func BenchmarkRow_Get(b *testing.B) {
	r := Row{
		header: []string{"name", "age", "city", "score", "active"},
		fields: []string{"Alice", "30", "Berlin", "95.5", "true"},
		colIdx: map[string]int{"name": 0, "age": 1, "city": 2, "score": 3, "active": 4},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = r.Get("score")
	}
}

func BenchmarkRow_GetFloat(b *testing.B) {
	r := Row{
		header: []string{"price"},
		fields: []string{"19.99"},
		colIdx: map[string]int{"price": 0},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.GetFloat("price")
	}
}
