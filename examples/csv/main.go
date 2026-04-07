// CSV pipeline example — two approaches to the same task.
//
// Reads employee data from an in-memory CSV string, filters and enriches
// records through a pipeline, then writes the result back to CSV.
// Everything stays in memory — no disk I/O.
//
// Approach 1: FromCSVFunc / ToCSV  — zero-reflect, explicit mapper functions
// Approach 2: FromCSV / ToCSVStruct — struct tags, reflection done once at init
//
// Both approaches produce identical output. The example shows how they compose
// with the same pipeline operations (Filter, PipeMap, hooks, aggregations).
//
// Uses: FromCSVFunc, FromCSV, ToCSV, ToCSVStruct, CSVConfig, Filter, PipeMap,
// PipeMapErr, WithElementHook, WithErrorHook, CountElements, CollectErrors,
// GroupBy, SumBy, MaxBy, Collect, Err, ToWriterString.

package main

import (
	"fmt"
	gs "github.com/lacolle87/gosplice"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
)

// ---------------------------------------------------------------------------
// Shared data — same CSV for both approaches
// ---------------------------------------------------------------------------

const employeeCSV = `name,department,salary,active,rating
Alice,Engineering,95000,true,4.5
Bob,Marketing,62000,true,3.2
Charlie,Engineering,105000,true,4.8
Diana,Marketing,58000,false,2.9
Eve,Engineering,88000,true,4.1
Frank,Sales,71000,true,3.7
Grace,Engineering,112000,true,4.9
Hank,Sales,65000,false,3.0
Ivy,Marketing,73000,true,3.8
Jack,Engineering,98000,true,4.3
`

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

// Employee is the raw record — used by both approaches.
// Struct tags drive the reflect-based FromCSV / ToCSVStruct path.
type Employee struct {
	Name       string  `csv:"name"`
	Department string  `csv:"department"`
	Salary     float64 `csv:"salary"`
	Active     bool    `csv:"active"`
	Rating     float64 `csv:"rating"`
}

// EnrichedEmployee is the pipeline output after filtering and enrichment.
type EnrichedEmployee struct {
	Name       string  `csv:"name"`
	Department string  `csv:"department"`
	Salary     float64 `csv:"salary"`
	Rating     float64 `csv:"rating"`
	Bonus      float64 `csv:"bonus"`
	Level      string  `csv:"level"`
}

// ---------------------------------------------------------------------------
// Shared logic
// ---------------------------------------------------------------------------

func enrich(e Employee) EnrichedEmployee {
	bonus := e.Salary * (e.Rating / 5.0) * 0.1
	level := "junior"
	switch {
	case e.Rating >= 4.5:
		level = "senior"
	case e.Rating >= 3.5:
		level = "mid"
	}
	return EnrichedEmployee{
		Name:       e.Name,
		Department: e.Department,
		Salary:     e.Salary,
		Rating:     e.Rating,
		Bonus:      float64(int(bonus*100)) / 100,
		Level:      level,
	}
}

func printSeparator(title string) {
	fmt.Printf("\n%s\n%s\n", title, strings.Repeat("─", len(title)))
}

// ---------------------------------------------------------------------------
// Approach 1: Functional — FromCSVFunc + ToCSV (zero reflect)
// ---------------------------------------------------------------------------

func approach1_functional() []EnrichedEmployee {
	printSeparator("Approach 1: Functional (FromCSVFunc → ToCSV)")

	var processed atomic.Int64
	var errs []error

	// Read CSV with explicit mapper — no reflection
	pipeline := gs.FromCSVFunc(
		strings.NewReader(employeeCSV),
		gs.CSVConfig{Header: true},
		func(row []string) (Employee, error) {
			salary, err := strconv.ParseFloat(row[2], 64)
			if err != nil {
				return Employee{}, fmt.Errorf("bad salary %q: %w", row[2], err)
			}
			active, err := strconv.ParseBool(row[3])
			if err != nil {
				return Employee{}, fmt.Errorf("bad active %q: %w", row[3], err)
			}
			rating, err := strconv.ParseFloat(row[4], 64)
			if err != nil {
				return Employee{}, fmt.Errorf("bad rating %q: %w", row[4], err)
			}
			return Employee{
				Name:       row[0],
				Department: row[1],
				Salary:     salary,
				Active:     active,
				Rating:     rating,
			}, nil
		},
	)

	// Pipeline: filter active → enrich
	enriched := gs.PipeMap(
		pipeline.
			WithElementHook(gs.CountElements[Employee](&processed)).
			Filter(func(e Employee) bool { return e.Active }).
			Filter(func(e Employee) bool { return e.Rating >= 3.5 }),
		enrich,
	).Collect()

	fmt.Printf("  Read: %d employees\n", processed.Load())
	fmt.Printf("  After filter (active + rating >= 3.5): %d\n", len(enriched))

	// Write to CSV with explicit formatter — no reflection
	var out strings.Builder
	err := gs.ToCSV(gs.FromSlice(enriched), &out,
		gs.CSVConfig{Header: true},
		[]string{"name", "department", "salary", "rating", "bonus", "level"},
		func(e EnrichedEmployee) []string {
			return []string{
				e.Name,
				e.Department,
				fmt.Sprintf("%.0f", e.Salary),
				fmt.Sprintf("%.1f", e.Rating),
				fmt.Sprintf("%.2f", e.Bonus),
				e.Level,
			}
		},
	)
	if err != nil {
		fmt.Printf("  Write error: %v\n", err)
	}
	if len(errs) > 0 {
		fmt.Printf("  Parse errors: %d\n", len(errs))
	}

	fmt.Println("\n  Output CSV:")
	for _, line := range strings.Split(strings.TrimSpace(out.String()), "\n") {
		fmt.Printf("    %s\n", line)
	}

	return enriched
}

// ---------------------------------------------------------------------------
// Approach 2: Struct tags — FromCSV + ToCSVStruct (reflect once)
// ---------------------------------------------------------------------------

func approach2_structTags() []EnrichedEmployee {
	printSeparator("Approach 2: Struct tags (FromCSV → ToCSVStruct)")

	var processed atomic.Int64

	// Read CSV via struct tags — reflection builds field map once
	pipeline := gs.FromCSV[Employee](
		strings.NewReader(employeeCSV),
		gs.CSVConfig{Header: true},
	)

	// Same pipeline logic, identical result
	enriched := gs.PipeMap(
		pipeline.
			WithElementHook(gs.CountElements[Employee](&processed)).
			Filter(func(e Employee) bool { return e.Active }).
			Filter(func(e Employee) bool { return e.Rating >= 3.5 }),
		enrich,
	).Collect()

	fmt.Printf("  Read: %d employees\n", processed.Load())
	fmt.Printf("  After filter (active + rating >= 3.5): %d\n", len(enriched))

	// Write to CSV via struct tags — reflection builds field list once
	var out strings.Builder
	err := gs.ToCSVStruct[EnrichedEmployee](
		gs.FromSlice(enriched),
		&out,
		gs.CSVConfig{Header: true},
	)
	if err != nil {
		fmt.Printf("  Write error: %v\n", err)
	}

	fmt.Println("\n  Output CSV:")
	for _, line := range strings.Split(strings.TrimSpace(out.String()), "\n") {
		fmt.Printf("    %s\n", line)
	}

	return enriched
}

// ---------------------------------------------------------------------------
// Analytics on the enriched data
// ---------------------------------------------------------------------------

func analytics(enriched []EnrichedEmployee) {
	printSeparator("Analytics (on enriched data)")

	// Department breakdown
	byDept := gs.GroupBy(gs.FromSlice(enriched), func(e EnrichedEmployee) string {
		return e.Department
	})
	fmt.Println("\n  Department breakdown:")
	for dept, emps := range byDept {
		totalBonus := gs.SumBy(gs.FromSlice(emps), func(e EnrichedEmployee) float64 {
			return e.Bonus
		})
		avgSalary := gs.SumBy(gs.FromSlice(emps), func(e EnrichedEmployee) float64 {
			return e.Salary
		}) / float64(len(emps))
		fmt.Printf("    %-14s %d people, avg salary: %.0f, total bonus: %.2f\n",
			dept, len(emps), avgSalary, totalBonus)
	}

	// Top performer
	best, ok := gs.MaxBy(gs.FromSlice(enriched), func(e EnrichedEmployee) float64 {
		return e.Rating
	})
	if ok {
		fmt.Printf("\n  Top performer: %s (%.1f rating, %s, bonus: %.2f)\n",
			best.Name, best.Rating, best.Department, best.Bonus)
	}

	// Level distribution
	levels := gs.CountBy(gs.FromSlice(enriched), func(e EnrichedEmployee) string {
		return e.Level
	})
	fmt.Println("\n  Level distribution:")
	for level, count := range levels {
		fmt.Printf("    %-8s %d\n", level, count)
	}

	// Total bonus budget
	totalBudget := gs.SumBy(gs.FromSlice(enriched), func(e EnrichedEmployee) float64 {
		return e.Bonus
	})
	fmt.Printf("\n  Total bonus budget: %.2f\n", totalBudget)

	// CSV report to stdout
	fmt.Println("\n  Final report (stdout):")
	_ = gs.ToWriterString(gs.FromSlice(enriched), os.Stdout, func(e EnrichedEmployee) string {
		return fmt.Sprintf("    %-10s %-14s %6.0f  %.1f  %7.2f  %s\n",
			e.Name, e.Department, e.Salary, e.Rating, e.Bonus, e.Level)
	})
}

func main() {
	fmt.Println("CSV Pipeline Example")
	fmt.Println("====================")
	fmt.Println("Same data, same logic, two API styles.")

	result1 := approach1_functional()
	result2 := approach2_structTags()

	// Verify both approaches produce identical results
	printSeparator("Verification")
	match := len(result1) == len(result2)
	if match {
		for i := range result1 {
			if result1[i] != result2[i] {
				match = false
				break
			}
		}
	}
	fmt.Printf("  Both approaches identical: %v (%d records each)\n", match, len(result1))

	// Run analytics on the enriched data
	analytics(result1)
}
