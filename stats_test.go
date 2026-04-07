package gosplice

import (
	"math"
	"testing"
)

const eps = 1e-9

func approxEqual(a, b, epsilon float64) bool {
	return math.Abs(a-b) < epsilon
}

// ---------------------------------------------------------------------------
// Pipeline: MeanBy
// ---------------------------------------------------------------------------

func TestMeanBy_Basic(t *testing.T) {
	got := MeanBy(FromSlice([]int{10, 20, 30}), func(n int) float64 { return float64(n) })
	if !approxEqual(got, 20.0, eps) {
		t.Errorf("expected 20.0, got %f", got)
	}
}

func TestMeanBy_Empty(t *testing.T) {
	got := MeanBy(FromSlice([]int{}), func(n int) float64 { return float64(n) })
	if got != 0 {
		t.Errorf("expected 0, got %f", got)
	}
}

func TestMeanBy_Single(t *testing.T) {
	got := MeanBy(FromSlice([]float64{42.5}), func(f float64) float64 { return f })
	if !approxEqual(got, 42.5, eps) {
		t.Errorf("expected 42.5, got %f", got)
	}
}

func TestMeanBy_WithFilter(t *testing.T) {
	got := MeanBy(
		FromSlice([]int{1, 2, 3, 4, 5, 6}).Filter(func(n int) bool { return n%2 == 0 }),
		func(n int) float64 { return float64(n) },
	)
	if !approxEqual(got, 4.0, eps) {
		t.Errorf("expected 4.0 (mean of 2,4,6), got %f", got)
	}
}

// ---------------------------------------------------------------------------
// Pipeline: VarianceBy / StdDevBy
// ---------------------------------------------------------------------------

func TestVarianceBy_Basic(t *testing.T) {
	// [2, 4, 4, 4, 5, 5, 7, 9] → mean=5, variance=4
	data := []int{2, 4, 4, 4, 5, 5, 7, 9}
	got := VarianceBy(FromSlice(data), func(n int) float64 { return float64(n) })
	if !approxEqual(got, 4.0, eps) {
		t.Errorf("expected 4.0, got %f", got)
	}
}

func TestVarianceBy_Empty(t *testing.T) {
	got := VarianceBy(FromSlice([]int{}), func(n int) float64 { return float64(n) })
	if got != 0 {
		t.Errorf("expected 0, got %f", got)
	}
}

func TestVarianceBy_Constant(t *testing.T) {
	got := VarianceBy(FromSlice([]int{5, 5, 5, 5}), func(n int) float64 { return float64(n) })
	if !approxEqual(got, 0, eps) {
		t.Errorf("expected 0, got %f", got)
	}
}

func TestStdDevBy_Basic(t *testing.T) {
	data := []int{2, 4, 4, 4, 5, 5, 7, 9}
	got := StdDevBy(FromSlice(data), func(n int) float64 { return float64(n) })
	if !approxEqual(got, 2.0, eps) {
		t.Errorf("expected 2.0, got %f", got)
	}
}

// ---------------------------------------------------------------------------
// Pipeline: MedianBy
// ---------------------------------------------------------------------------

func TestMedianBy_Odd(t *testing.T) {
	got := MedianBy(FromSlice([]int{3, 1, 2}), func(n int) float64 { return float64(n) })
	if !approxEqual(got, 2.0, eps) {
		t.Errorf("expected 2.0, got %f", got)
	}
}

func TestMedianBy_Even(t *testing.T) {
	got := MedianBy(FromSlice([]int{3, 1, 2, 4}), func(n int) float64 { return float64(n) })
	if !approxEqual(got, 2.5, eps) {
		t.Errorf("expected 2.5, got %f", got)
	}
}

func TestMedianBy_Empty(t *testing.T) {
	got := MedianBy(FromSlice([]int{}), func(n int) float64 { return float64(n) })
	if got != 0 {
		t.Errorf("expected 0, got %f", got)
	}
}

func TestMedianBy_Single(t *testing.T) {
	got := MedianBy(FromSlice([]int{7}), func(n int) float64 { return float64(n) })
	if !approxEqual(got, 7.0, eps) {
		t.Errorf("expected 7.0, got %f", got)
	}
}

// ---------------------------------------------------------------------------
// Pipeline: PercentileBy
// ---------------------------------------------------------------------------

func TestPercentileBy_Median(t *testing.T) {
	got := PercentileBy(FromSlice([]int{1, 2, 3, 4, 5}), 50, func(n int) float64 { return float64(n) })
	if !approxEqual(got, 3.0, eps) {
		t.Errorf("expected 3.0, got %f", got)
	}
}

func TestPercentileBy_P0(t *testing.T) {
	got := PercentileBy(FromSlice([]int{10, 20, 30}), 0, func(n int) float64 { return float64(n) })
	if !approxEqual(got, 10.0, eps) {
		t.Errorf("expected 10.0, got %f", got)
	}
}

func TestPercentileBy_P100(t *testing.T) {
	got := PercentileBy(FromSlice([]int{10, 20, 30}), 100, func(n int) float64 { return float64(n) })
	if !approxEqual(got, 30.0, eps) {
		t.Errorf("expected 30.0, got %f", got)
	}
}

func TestPercentileBy_P25(t *testing.T) {
	data := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	got := PercentileBy(FromSlice(data), 25, func(n int) float64 { return float64(n) })
	// numpy: np.percentile([1..10], 25) = 3.25
	if !approxEqual(got, 3.25, eps) {
		t.Errorf("expected 3.25, got %f", got)
	}
}

func TestPercentileBy_Empty(t *testing.T) {
	got := PercentileBy(FromSlice([]int{}), 50, func(n int) float64 { return float64(n) })
	if got != 0 {
		t.Errorf("expected 0, got %f", got)
	}
}

// ---------------------------------------------------------------------------
// Pipeline: DescribeBy
// ---------------------------------------------------------------------------

func TestDescribeBy_Basic(t *testing.T) {
	data := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	s := DescribeBy(FromSlice(data), func(n int) float64 { return float64(n) })

	if s.Count != 10 {
		t.Errorf("count: expected 10, got %d", s.Count)
	}
	if !approxEqual(s.Mean, 5.5, eps) {
		t.Errorf("mean: expected 5.5, got %f", s.Mean)
	}
	if !approxEqual(s.Min, 1.0, eps) {
		t.Errorf("min: expected 1.0, got %f", s.Min)
	}
	if !approxEqual(s.Max, 10.0, eps) {
		t.Errorf("max: expected 10.0, got %f", s.Max)
	}
	if !approxEqual(s.Median, 5.5, eps) {
		t.Errorf("median: expected 5.5, got %f", s.Median)
	}
	if !approxEqual(s.Sum, 55.0, eps) {
		t.Errorf("sum: expected 55.0, got %f", s.Sum)
	}
	if s.StdDev <= 0 {
		t.Errorf("stddev should be positive, got %f", s.StdDev)
	}
	if s.IQR() <= 0 {
		t.Errorf("IQR should be positive, got %f", s.IQR())
	}
}

func TestDescribeBy_Empty(t *testing.T) {
	s := DescribeBy(FromSlice([]int{}), func(n int) float64 { return float64(n) })
	if s.Count != 0 || s.Mean != 0 || s.StdDev != 0 {
		t.Errorf("expected zero Stats, got %+v", s)
	}
}

func TestDescribeBy_Single(t *testing.T) {
	s := DescribeBy(FromSlice([]float64{42.0}), func(f float64) float64 { return f })
	if s.Count != 1 {
		t.Errorf("count: expected 1, got %d", s.Count)
	}
	if !approxEqual(s.Mean, 42.0, eps) {
		t.Errorf("mean: expected 42.0, got %f", s.Mean)
	}
	if !approxEqual(s.StdDev, 0, eps) {
		t.Errorf("stddev: expected 0, got %f", s.StdDev)
	}
	if !approxEqual(s.Min, 42.0, eps) || !approxEqual(s.Max, 42.0, eps) {
		t.Errorf("min/max: expected 42.0, got %f/%f", s.Min, s.Max)
	}
}

// ---------------------------------------------------------------------------
// Pipeline: CorrelationBy
// ---------------------------------------------------------------------------

func TestCorrelationBy_Perfect(t *testing.T) {
	type point struct{ x, y float64 }
	data := []point{{1, 2}, {2, 4}, {3, 6}, {4, 8}}
	r := CorrelationBy(FromSlice(data),
		func(p point) float64 { return p.x },
		func(p point) float64 { return p.y },
	)
	if !approxEqual(r, 1.0, eps) {
		t.Errorf("expected 1.0, got %f", r)
	}
}

func TestCorrelationBy_Negative(t *testing.T) {
	type point struct{ x, y float64 }
	data := []point{{1, 10}, {2, 8}, {3, 6}, {4, 4}}
	r := CorrelationBy(FromSlice(data),
		func(p point) float64 { return p.x },
		func(p point) float64 { return p.y },
	)
	if !approxEqual(r, -1.0, eps) {
		t.Errorf("expected -1.0, got %f", r)
	}
}

func TestCorrelationBy_Empty(t *testing.T) {
	type point struct{ x, y float64 }
	r := CorrelationBy(FromSlice([]point{}),
		func(p point) float64 { return p.x },
		func(p point) float64 { return p.y },
	)
	if r != 0 {
		t.Errorf("expected 0, got %f", r)
	}
}

func TestCorrelationBy_ConstantY(t *testing.T) {
	type point struct{ x, y float64 }
	data := []point{{1, 5}, {2, 5}, {3, 5}}
	r := CorrelationBy(FromSlice(data),
		func(p point) float64 { return p.x },
		func(p point) float64 { return p.y },
	)
	if r != 0 {
		t.Errorf("expected 0 (zero variance in y), got %f", r)
	}
}

// ---------------------------------------------------------------------------
// Standalone: Mean
// ---------------------------------------------------------------------------

func TestMean_Ints(t *testing.T) {
	if !approxEqual(Mean([]int{1, 2, 3, 4, 5}), 3.0, eps) {
		t.Error("mean of [1..5] should be 3")
	}
}

func TestMean_Floats(t *testing.T) {
	if !approxEqual(Mean([]float64{1.5, 2.5, 3.5}), 2.5, eps) {
		t.Error("mean of [1.5, 2.5, 3.5] should be 2.5")
	}
}

func TestMean_Empty(t *testing.T) {
	if Mean([]int{}) != 0 {
		t.Error("mean of empty should be 0")
	}
}

// ---------------------------------------------------------------------------
// Standalone: Variance / StdDev
// ---------------------------------------------------------------------------

func TestVariance_Basic(t *testing.T) {
	got := Variance([]int{2, 4, 4, 4, 5, 5, 7, 9})
	if !approxEqual(got, 4.0, eps) {
		t.Errorf("expected 4.0, got %f", got)
	}
}

func TestStdDev_Basic(t *testing.T) {
	got := StdDev([]int{2, 4, 4, 4, 5, 5, 7, 9})
	if !approxEqual(got, 2.0, eps) {
		t.Errorf("expected 2.0, got %f", got)
	}
}

func TestVariance_Empty(t *testing.T) {
	if Variance([]int{}) != 0 {
		t.Error("variance of empty should be 0")
	}
}

func TestVariance_Constant(t *testing.T) {
	if !approxEqual(Variance([]int{7, 7, 7}), 0, eps) {
		t.Error("variance of constant should be 0")
	}
}

// ---------------------------------------------------------------------------
// Standalone: Median
// ---------------------------------------------------------------------------

func TestMedian_Odd(t *testing.T) {
	if !approxEqual(Median([]int{3, 1, 2}), 2.0, eps) {
		t.Error("median of [3,1,2] should be 2")
	}
}

func TestMedian_Even(t *testing.T) {
	if !approxEqual(Median([]int{4, 1, 3, 2}), 2.5, eps) {
		t.Error("median of [4,1,3,2] should be 2.5")
	}
}

func TestMedian_Empty(t *testing.T) {
	if Median([]int{}) != 0 {
		t.Error("median of empty should be 0")
	}
}

func TestMedian_DoesNotModifyInput(t *testing.T) {
	data := []int{3, 1, 2}
	Median(data)
	assertSliceEqual(t, []int{3, 1, 2}, data)
}

// ---------------------------------------------------------------------------
// Standalone: Percentile
// ---------------------------------------------------------------------------

func TestPercentile_P50(t *testing.T) {
	if !approxEqual(Percentile([]int{1, 2, 3, 4, 5}, 50), 3.0, eps) {
		t.Error("p50 of [1..5] should be 3")
	}
}

func TestPercentile_P25(t *testing.T) {
	data := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	if !approxEqual(Percentile(data, 25), 3.25, eps) {
		t.Errorf("p25: expected 3.25, got %f", Percentile(data, 25))
	}
}

func TestPercentile_P75(t *testing.T) {
	data := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	if !approxEqual(Percentile(data, 75), 7.75, eps) {
		t.Errorf("p75: expected 7.75, got %f", Percentile(data, 75))
	}
}

func TestPercentile_Empty(t *testing.T) {
	if Percentile([]int{}, 50) != 0 {
		t.Error("percentile of empty should be 0")
	}
}

// ---------------------------------------------------------------------------
// Standalone: Describe
// ---------------------------------------------------------------------------

func TestDescribe_Basic(t *testing.T) {
	s := Describe([]int{1, 2, 3, 4, 5})
	if s.Count != 5 {
		t.Errorf("count: expected 5, got %d", s.Count)
	}
	if !approxEqual(s.Mean, 3.0, eps) {
		t.Errorf("mean: expected 3.0, got %f", s.Mean)
	}
	if !approxEqual(s.Min, 1.0, eps) {
		t.Errorf("min: expected 1.0, got %f", s.Min)
	}
	if !approxEqual(s.Max, 5.0, eps) {
		t.Errorf("max: expected 5.0, got %f", s.Max)
	}
}

func TestDescribe_Empty(t *testing.T) {
	s := Describe([]int{})
	if s.Count != 0 {
		t.Errorf("expected zero Stats")
	}
}

// ---------------------------------------------------------------------------
// Standalone: Correlation
// ---------------------------------------------------------------------------

func TestCorrelation_Perfect(t *testing.T) {
	xs := []int{1, 2, 3, 4, 5}
	ys := []int{2, 4, 6, 8, 10}
	if !approxEqual(Correlation(xs, ys), 1.0, eps) {
		t.Error("expected perfect positive correlation")
	}
}

func TestCorrelation_Negative(t *testing.T) {
	xs := []float64{1, 2, 3, 4}
	ys := []float64{10, 8, 6, 4}
	if !approxEqual(Correlation(xs, ys), -1.0, eps) {
		t.Error("expected perfect negative correlation")
	}
}

func TestCorrelation_UnequalLength(t *testing.T) {
	xs := []int{1, 2, 3, 4, 5}
	ys := []int{2, 4, 6} // shorter
	r := Correlation(xs, ys)
	if !approxEqual(r, 1.0, eps) {
		t.Errorf("expected 1.0 (uses shorter), got %f", r)
	}
}

func TestCorrelation_TooShort(t *testing.T) {
	if Correlation([]int{1}, []int{2}) != 0 {
		t.Error("expected 0 for n<2")
	}
}

// ---------------------------------------------------------------------------
// Standalone: Histogram
// ---------------------------------------------------------------------------

func TestHistogram_Basic(t *testing.T) {
	data := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	edges, counts := Histogram(data, 5)
	if len(edges) != 6 {
		t.Fatalf("expected 6 edges, got %d", len(edges))
	}
	if len(counts) != 5 {
		t.Fatalf("expected 5 counts, got %d", len(counts))
	}
	total := 0
	for _, c := range counts {
		total += c
	}
	if total != 10 {
		t.Errorf("expected total 10, got %d", total)
	}
}

func TestHistogram_Empty(t *testing.T) {
	edges, counts := Histogram([]int{}, 5)
	if edges != nil || counts != nil {
		t.Error("expected nil for empty input")
	}
}

func TestHistogram_AllSame(t *testing.T) {
	edges, counts := Histogram([]int{5, 5, 5}, 3)
	if len(counts) != 1 || counts[0] != 3 {
		t.Errorf("expected [3], got %v", counts)
	}
	if len(edges) != 2 || edges[0] != 5 {
		t.Errorf("unexpected edges: %v", edges)
	}
}

func TestHistogram_ZeroBins(t *testing.T) {
	edges, counts := Histogram([]int{1, 2, 3}, 0)
	if edges != nil || counts != nil {
		t.Error("expected nil for 0 bins")
	}
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

func BenchmarkMeanBy_10k(b *testing.B) {
	data := makeIterData(10_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MeanBy(FromSlice(data), func(n int) float64 { return float64(n) })
	}
}

func BenchmarkVarianceBy_10k(b *testing.B) {
	data := makeIterData(10_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		VarianceBy(FromSlice(data), func(n int) float64 { return float64(n) })
	}
}

func BenchmarkMedianBy_10k(b *testing.B) {
	data := makeIterData(10_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MedianBy(FromSlice(data), func(n int) float64 { return float64(n) })
	}
}

func BenchmarkDescribeBy_10k(b *testing.B) {
	data := makeIterData(10_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		DescribeBy(FromSlice(data), func(n int) float64 { return float64(n) })
	}
}

func BenchmarkCorrelationBy_10k(b *testing.B) {
	type pair struct{ x, y float64 }
	data := make([]pair, 10_000)
	for i := range data {
		data[i] = pair{float64(i), float64(i) * 2.5}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CorrelationBy(FromSlice(data),
			func(p pair) float64 { return p.x },
			func(p pair) float64 { return p.y },
		)
	}
}

func BenchmarkMean_Slice_10k(b *testing.B) {
	data := makeIterData(10_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Mean(data)
	}
}

func BenchmarkDescribe_Slice_10k(b *testing.B) {
	data := makeIterData(10_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Describe(data)
	}
}

func BenchmarkHistogram_10k(b *testing.B) {
	data := makeIterData(10_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Histogram(data, 20)
	}
}
