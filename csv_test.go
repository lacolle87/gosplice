package gosplice

import (
	"bytes"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
)

// ---------------------------------------------------------------------------
// Test types
// ---------------------------------------------------------------------------

type testOrder struct {
	ID     string  `csv:"order_id"`
	Amount float64 `csv:"amount"`
	Status string  `csv:"status"`
}

type testUser struct {
	Name   string `csv:"name"`
	Age    int    `csv:"age"`
	Active bool   `csv:"active"`
}

// Positional — no header, fields mapped by index
type testPoint struct {
	X float64
	Y float64
}

// ---------------------------------------------------------------------------
// FromCSVFunc
// ---------------------------------------------------------------------------

func TestFromCSVFunc_Basic(t *testing.T) {
	input := "order_id,amount,status\n1,150.5,completed\n2,0,failed\n3,320,completed\n"
	r := strings.NewReader(input)

	result := FromCSVFunc(r, CSVConfig{Header: true}, func(row []string) (testOrder, error) {
		var o testOrder
		o.ID = row[0]
		fmt.Sscanf(row[1], "%f", &o.Amount)
		o.Status = row[2]
		return o, nil
	}).Collect()

	if len(result) != 3 {
		t.Fatalf("expected 3, got %d", len(result))
	}
	if result[0].ID != "1" || result[0].Amount != 150.5 {
		t.Errorf("unexpected first row: %+v", result[0])
	}
	if result[2].Status != "completed" {
		t.Errorf("unexpected third row: %+v", result[2])
	}
}

func TestFromCSVFunc_NoHeader(t *testing.T) {
	input := "1,150.5,done\n2,0,fail\n"
	r := strings.NewReader(input)

	result := FromCSVFunc(r, CSVConfig{Header: false}, func(row []string) (testOrder, error) {
		var o testOrder
		o.ID = row[0]
		fmt.Sscanf(row[1], "%f", &o.Amount)
		o.Status = row[2]
		return o, nil
	}).Collect()

	if len(result) != 2 {
		t.Fatalf("expected 2, got %d", len(result))
	}
	if result[0].ID != "1" {
		t.Errorf("unexpected first row: %+v", result[0])
	}
}

func TestFromCSVFunc_Empty(t *testing.T) {
	r := strings.NewReader("")
	result := FromCSVFunc(r, CSVConfig{}, func(row []string) (int, error) {
		return 0, nil
	}).Collect()
	if len(result) != 0 {
		t.Fatalf("expected empty, got %d", len(result))
	}
}

func TestFromCSVFunc_HeaderOnly(t *testing.T) {
	r := strings.NewReader("a,b,c\n")
	result := FromCSVFunc(r, CSVConfig{Header: true}, func(row []string) (string, error) {
		return row[0], nil
	}).Collect()
	if len(result) != 0 {
		t.Fatalf("expected empty, got %d", len(result))
	}
}

func TestFromCSVFunc_MapperError_SkipsRow(t *testing.T) {
	input := "id,val\n1,good\n2,bad\n3,good\n"
	r := strings.NewReader(input)

	p := FromCSVFunc(r, CSVConfig{Header: true}, func(row []string) (string, error) {
		if row[1] == "bad" {
			return "", fmt.Errorf("bad row")
		}
		return row[0], nil
	})
	result := p.Collect()

	if len(result) != 2 {
		t.Fatalf("expected 2, got %d: %v", len(result), result)
	}
	if result[0] != "1" || result[1] != "3" {
		t.Errorf("unexpected: %v", result)
	}
	if p.Err() == nil {
		t.Error("expected error to be set")
	}
}

func TestFromCSVFunc_CustomDelimiter(t *testing.T) {
	input := "a;b;c\n1;2;3\n"
	r := strings.NewReader(input)

	result := FromCSVFunc(r, CSVConfig{Comma: ';', Header: true}, func(row []string) (string, error) {
		return row[0] + "-" + row[2], nil
	}).Collect()

	if len(result) != 1 || result[0] != "1-3" {
		t.Errorf("unexpected: %v", result)
	}
}

func TestFromCSVFunc_WithPipelineOps(t *testing.T) {
	input := "n\n1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n"
	r := strings.NewReader(input)

	var count atomic.Int64
	result := FromCSVFunc(r, CSVConfig{Header: true}, func(row []string) (int, error) {
		var n int
		fmt.Sscanf(row[0], "%d", &n)
		return n, nil
	}).
		WithElementHook(CountElements[int](&count)).
		Filter(func(n int) bool { return n%2 == 0 }).
		Take(3).
		Collect()

	assertSliceEqual(t, []int{2, 4, 6}, result)
	// Hook fires on elements reaching terminal (post-filter, post-take)
	if count.Load() != 3 {
		t.Errorf("expected 3 hook calls, got %d", count.Load())
	}
}

// ---------------------------------------------------------------------------
// FromCSV (struct tags)
// ---------------------------------------------------------------------------

func TestFromCSV_Basic(t *testing.T) {
	input := "order_id,amount,status\n1,150.5,completed\n2,0,failed\n3,320,completed\n"
	r := strings.NewReader(input)

	result := FromCSV[testOrder](r, CSVConfig{Header: true}).Collect()

	if len(result) != 3 {
		t.Fatalf("expected 3, got %d", len(result))
	}
	if result[0].ID != "1" || result[0].Amount != 150.5 || result[0].Status != "completed" {
		t.Errorf("unexpected first row: %+v", result[0])
	}
}

func TestFromCSV_BoolAndInt(t *testing.T) {
	input := "name,age,active\nAlice,30,true\nBob,25,false\n"
	r := strings.NewReader(input)

	result := FromCSV[testUser](r, CSVConfig{Header: true}).Collect()

	if len(result) != 2 {
		t.Fatalf("expected 2, got %d", len(result))
	}
	if result[0].Name != "Alice" || result[0].Age != 30 || !result[0].Active {
		t.Errorf("unexpected: %+v", result[0])
	}
	if result[1].Active {
		t.Errorf("expected false, got true")
	}
}

func TestFromCSV_Positional(t *testing.T) {
	input := "1.5,2.5\n3.0,4.0\n"
	r := strings.NewReader(input)

	result := FromCSV[testPoint](r, CSVConfig{Header: false}).Collect()

	if len(result) != 2 {
		t.Fatalf("expected 2, got %d", len(result))
	}
	if result[0].X != 1.5 || result[0].Y != 2.5 {
		t.Errorf("unexpected: %+v", result[0])
	}
}

func TestFromCSV_MalformedRow_Skips(t *testing.T) {
	input := "name,age,active\nAlice,30,true\nBob,notanumber,false\nCharlie,40,true\n"
	r := strings.NewReader(input)

	p := FromCSV[testUser](r, CSVConfig{Header: true})
	result := p.Collect()

	// Bob's row should be skipped (age parse error)
	if len(result) != 2 {
		t.Fatalf("expected 2, got %d: %v", len(result), result)
	}
	if result[0].Name != "Alice" || result[1].Name != "Charlie" {
		t.Errorf("unexpected: %v", result)
	}
	if p.Err() == nil {
		t.Error("expected parse error")
	}
}

func TestFromCSV_ColumnReorder(t *testing.T) {
	// Header columns in different order than struct fields
	input := "status,amount,order_id\ncompleted,100.5,42\n"
	r := strings.NewReader(input)

	result := FromCSV[testOrder](r, CSVConfig{Header: true}).Collect()

	if len(result) != 1 {
		t.Fatalf("expected 1, got %d", len(result))
	}
	if result[0].ID != "42" || result[0].Amount != 100.5 || result[0].Status != "completed" {
		t.Errorf("unexpected: %+v", result[0])
	}
}

func TestFromCSV_ExtraColumns(t *testing.T) {
	// CSV has extra columns not in struct — should be ignored
	input := "order_id,extra1,amount,extra2,status\n1,foo,50.0,bar,done\n"
	r := strings.NewReader(input)

	result := FromCSV[testOrder](r, CSVConfig{Header: true}).Collect()

	if len(result) != 1 {
		t.Fatalf("expected 1, got %d", len(result))
	}
	if result[0].ID != "1" || result[0].Amount != 50.0 || result[0].Status != "done" {
		t.Errorf("unexpected: %+v", result[0])
	}
}

func TestFromCSV_Empty(t *testing.T) {
	r := strings.NewReader("")
	result := FromCSV[testOrder](r, CSVConfig{Header: true}).Collect()
	if len(result) != 0 {
		t.Fatalf("expected empty, got %d", len(result))
	}
}

// ---------------------------------------------------------------------------
// ToCSV
// ---------------------------------------------------------------------------

func TestToCSV_Basic(t *testing.T) {
	var buf bytes.Buffer
	orders := []testOrder{
		{"1", 150.5, "completed"},
		{"2", 0, "failed"},
	}

	err := ToCSV(FromSlice(orders), &buf, CSVConfig{Header: true},
		[]string{"order_id", "amount", "status"},
		func(o testOrder) []string {
			return []string{o.ID, fmt.Sprintf("%.1f", o.Amount), o.Status}
		},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := "order_id,amount,status\n1,150.5,completed\n2,0.0,failed\n"
	if buf.String() != expected {
		t.Errorf("expected:\n%s\ngot:\n%s", expected, buf.String())
	}
}

func TestToCSV_NoHeader(t *testing.T) {
	var buf bytes.Buffer
	err := ToCSV(FromSlice([]testOrder{{"1", 10, "ok"}}), &buf, CSVConfig{},
		nil,
		func(o testOrder) []string {
			return []string{o.ID, fmt.Sprintf("%.0f", o.Amount), o.Status}
		},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if buf.String() != "1,10,ok\n" {
		t.Errorf("unexpected: %q", buf.String())
	}
}

func TestToCSV_Empty(t *testing.T) {
	var buf bytes.Buffer
	err := ToCSV(FromSlice([]testOrder{}), &buf, CSVConfig{Header: true},
		[]string{"a", "b"},
		func(o testOrder) []string { return nil },
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if buf.String() != "a,b\n" {
		t.Errorf("expected header only, got %q", buf.String())
	}
}

func TestToCSV_CustomDelimiter(t *testing.T) {
	var buf bytes.Buffer
	err := ToCSV(FromSlice([]testOrder{{"1", 10, "ok"}}), &buf,
		CSVConfig{Comma: ';'},
		nil,
		func(o testOrder) []string { return []string{o.ID, "10", o.Status} },
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if buf.String() != "1;10;ok\n" {
		t.Errorf("unexpected: %q", buf.String())
	}
}

func TestToCSV_WithPipelineFilter(t *testing.T) {
	var buf bytes.Buffer
	orders := []testOrder{
		{"1", 150, "completed"},
		{"2", 0, "failed"},
		{"3", 320, "completed"},
	}

	err := ToCSV(
		FromSlice(orders).Filter(func(o testOrder) bool { return o.Status == "completed" }),
		&buf, CSVConfig{},
		nil,
		func(o testOrder) []string {
			return []string{o.ID, fmt.Sprintf("%.0f", o.Amount)}
		},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if buf.String() != "1,150\n3,320\n" {
		t.Errorf("unexpected: %q", buf.String())
	}
}

// ---------------------------------------------------------------------------
// ToCSVStruct
// ---------------------------------------------------------------------------

func TestToCSVStruct_Basic(t *testing.T) {
	var buf bytes.Buffer
	orders := []testOrder{
		{"1", 150.5, "completed"},
		{"2", 0, "failed"},
	}

	err := ToCSVStruct[testOrder](FromSlice(orders), &buf, CSVConfig{Header: true})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 3 {
		t.Fatalf("expected 3 lines, got %d: %v", len(lines), lines)
	}
	if lines[0] != "order_id,amount,status" {
		t.Errorf("unexpected header: %q", lines[0])
	}
}

func TestToCSVStruct_NoHeader(t *testing.T) {
	var buf bytes.Buffer
	err := ToCSVStruct[testOrder](
		FromSlice([]testOrder{{"1", 10, "ok"}}),
		&buf, CSVConfig{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Should have exactly one data line, no header
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 1 {
		t.Fatalf("expected 1 line, got %d", len(lines))
	}
}

// ---------------------------------------------------------------------------
// Round-trip: FromCSV → pipeline → ToCSVStruct
// ---------------------------------------------------------------------------

func TestCSV_RoundTrip(t *testing.T) {
	input := "order_id,amount,status\n1,100.5,ok\n2,200.0,ok\n3,50.0,fail\n"

	// Read → filter → write
	var buf bytes.Buffer
	pipeline := FromCSV[testOrder](strings.NewReader(input), CSVConfig{Header: true}).
		Filter(func(o testOrder) bool { return o.Status == "ok" })

	err := ToCSVStruct[testOrder](pipeline, &buf, CSVConfig{Header: true})
	if err != nil {
		t.Fatalf("round-trip error: %v", err)
	}

	// Re-read written output
	result := FromCSV[testOrder](strings.NewReader(buf.String()), CSVConfig{Header: true}).Collect()
	if len(result) != 2 {
		t.Fatalf("expected 2, got %d", len(result))
	}
	if result[0].ID != "1" || result[1].ID != "2" {
		t.Errorf("unexpected round-trip: %v", result)
	}
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

func BenchmarkFromCSVFunc_1k(b *testing.B) {
	data := buildCSVData(1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		FromCSVFunc(strings.NewReader(data), CSVConfig{Header: true}, func(row []string) (testOrder, error) {
			var o testOrder
			o.ID = row[0]
			fmt.Sscanf(row[1], "%f", &o.Amount)
			o.Status = row[2]
			return o, nil
		}).Collect()
	}
}

func BenchmarkFromCSV_StructTags_1k(b *testing.B) {
	data := buildCSVData(1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		FromCSV[testOrder](strings.NewReader(data), CSVConfig{Header: true}).Collect()
	}
}

func BenchmarkToCSV_1k(b *testing.B) {
	orders := make([]testOrder, 1000)
	for i := range orders {
		orders[i] = testOrder{
			ID:     fmt.Sprintf("%d", i),
			Amount: float64(i) * 1.5,
			Status: "ok",
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		ToCSV(FromSlice(orders), &buf, CSVConfig{},
			nil,
			func(o testOrder) []string {
				return []string{o.ID, fmt.Sprintf("%.2f", o.Amount), o.Status}
			},
		)
	}
}

func BenchmarkToCSVStruct_1k(b *testing.B) {
	orders := make([]testOrder, 1000)
	for i := range orders {
		orders[i] = testOrder{
			ID:     fmt.Sprintf("%d", i),
			Amount: float64(i) * 1.5,
			Status: "ok",
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		ToCSVStruct[testOrder](FromSlice(orders), &buf, CSVConfig{})
	}
}

func buildCSVData(n int) string {
	var sb strings.Builder
	sb.WriteString("order_id,amount,status\n")
	for i := 0; i < n; i++ {
		fmt.Fprintf(&sb, "%d,%.2f,completed\n", i, float64(i)*1.5)
	}
	return sb.String()
}
