package gosplice

import (
	"math"
	"sort"
)

type Numeric interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 |
		~float32 | ~float64
}

type meanAcc struct {
	sum   float64
	count int
}

type welfordAcc struct {
	count int
	mean  float64
	m2    float64
}

type correlationAcc struct {
	n                 int
	meanX, meanY      float64
	m2X, m2Y, covarXY float64
}

// --- Pipeline aggregations ---

func MeanBy[T any](p *Pipeline[T], fn func(T) float64) float64 {
	defer p.finalize()
	r := fold(p, meanAcc{}, func(a meanAcc, v T) meanAcc {
		a.sum += fn(v)
		a.count++
		return a
	})
	if r.count == 0 {
		return 0
	}
	return r.sum / float64(r.count)
}

// VarianceBy uses Welford's online algorithm — single-pass, numerically stable.
func VarianceBy[T any](p *Pipeline[T], fn func(T) float64) float64 {
	defer p.finalize()
	r := fold(p, welfordAcc{}, func(w welfordAcc, v T) welfordAcc {
		w.count++
		x := fn(v)
		delta := x - w.mean
		w.mean += delta / float64(w.count)
		delta2 := x - w.mean
		w.m2 += delta * delta2
		return w
	})
	if r.count < 1 {
		return 0
	}
	return r.m2 / float64(r.count)
}

func StdDevBy[T any](p *Pipeline[T], fn func(T) float64) float64 {
	return math.Sqrt(VarianceBy(p, fn))
}

// MedianBy collects all values into memory and sorts — O(n log n).
func MedianBy[T any](p *Pipeline[T], fn func(T) float64) float64 {
	defer p.finalize()
	var vals []float64
	drain(p, func(v T) { vals = append(vals, fn(v)) })
	return medianSorted(vals)
}

func PercentileBy[T any](p *Pipeline[T], pct float64, fn func(T) float64) float64 {
	defer p.finalize()
	var vals []float64
	drain(p, func(v T) { vals = append(vals, fn(v)) })
	return percentileSorted(vals, pct)
}

// DescribeBy collects values once, then computes everything in one pass + sort.
func DescribeBy[T any](p *Pipeline[T], fn func(T) float64) Stats {
	defer p.finalize()
	var vals []float64
	drain(p, func(v T) { vals = append(vals, fn(v)) })

	if len(vals) == 0 {
		return Stats{}
	}

	var count int
	var mean, m2 float64
	minVal := math.Inf(1)
	maxVal := math.Inf(-1)
	for _, x := range vals {
		count++
		delta := x - mean
		mean += delta / float64(count)
		delta2 := x - mean
		m2 += delta * delta2
		if x < minVal {
			minVal = x
		}
		if x > maxVal {
			maxVal = x
		}
	}

	sort.Float64s(vals)

	return Stats{
		Count:  count,
		Mean:   mean,
		StdDev: math.Sqrt(m2 / float64(count)),
		Min:    minVal,
		Q1:     percentileFromSorted(vals, 25),
		Median: percentileFromSorted(vals, 50),
		Q3:     percentileFromSorted(vals, 75),
		Max:    maxVal,
		Sum:    mean * float64(count),
	}
}

type Stats struct {
	Count  int
	Sum    float64
	Mean   float64
	StdDev float64
	Min    float64
	Q1     float64
	Median float64
	Q3     float64
	Max    float64
}

func (s Stats) IQR() float64 { return s.Q3 - s.Q1 }

// CorrelationBy uses online covariance — single-pass, O(1) memory.
// Returns 0 for fewer than 2 elements or zero variance.
func CorrelationBy[T any](p *Pipeline[T], fnX, fnY func(T) float64) float64 {
	defer p.finalize()
	r := fold(p, correlationAcc{}, func(s correlationAcc, v T) correlationAcc {
		s.n++
		x, y := fnX(v), fnY(v)
		dx := x - s.meanX
		s.meanX += dx / float64(s.n)
		dx2 := x - s.meanX
		s.m2X += dx * dx2

		dy := y - s.meanY
		s.meanY += dy / float64(s.n)
		dy2 := y - s.meanY
		s.m2Y += dy * dy2

		s.covarXY += dx * (y - s.meanY)
		return s
	})
	if r.n < 2 || r.m2X == 0 || r.m2Y == 0 {
		return 0
	}
	return r.covarXY / math.Sqrt(r.m2X*r.m2Y)
}

// --- Standalone slice functions ---

func Mean[N Numeric](data []N) float64 {
	if len(data) == 0 {
		return 0
	}
	var sum float64
	for _, v := range data {
		sum += float64(v)
	}
	return sum / float64(len(data))
}

func Variance[N Numeric](data []N) float64 {
	if len(data) == 0 {
		return 0
	}
	var mean, m2 float64
	for i, v := range data {
		x := float64(v)
		delta := x - mean
		mean += delta / float64(i+1)
		delta2 := x - mean
		m2 += delta * delta2
	}
	return m2 / float64(len(data))
}

func StdDev[N Numeric](data []N) float64 {
	return math.Sqrt(Variance(data))
}

func Median[N Numeric](data []N) float64 {
	if len(data) == 0 {
		return 0
	}
	vals := make([]float64, len(data))
	for i, v := range data {
		vals[i] = float64(v)
	}
	return medianSorted(vals)
}

func Percentile[N Numeric](data []N, pct float64) float64 {
	if len(data) == 0 {
		return 0
	}
	vals := make([]float64, len(data))
	for i, v := range data {
		vals[i] = float64(v)
	}
	return percentileSorted(vals, pct)
}

func Describe[N Numeric](data []N) Stats {
	if len(data) == 0 {
		return Stats{}
	}
	vals := make([]float64, len(data))
	for i, v := range data {
		vals[i] = float64(v)
	}

	var mean, m2 float64
	minVal := vals[0]
	maxVal := vals[0]
	for i, x := range vals {
		delta := x - mean
		mean += delta / float64(i+1)
		delta2 := x - mean
		m2 += delta * delta2
		if x < minVal {
			minVal = x
		}
		if x > maxVal {
			maxVal = x
		}
	}

	sort.Float64s(vals)

	return Stats{
		Count:  len(vals),
		Sum:    mean * float64(len(vals)),
		Mean:   mean,
		StdDev: math.Sqrt(m2 / float64(len(vals))),
		Min:    minVal,
		Q1:     percentileFromSorted(vals, 25),
		Median: percentileFromSorted(vals, 50),
		Q3:     percentileFromSorted(vals, 75),
		Max:    maxVal,
	}
}

func Correlation[N Numeric, M Numeric](xs []N, ys []M) float64 {
	n := len(xs)
	if len(ys) < n {
		n = len(ys)
	}
	if n < 2 {
		return 0
	}
	var meanX, meanY, m2X, m2Y, covarXY float64
	for i := 0; i < n; i++ {
		x, y := float64(xs[i]), float64(ys[i])
		dx := x - meanX
		meanX += dx / float64(i+1)
		dx2 := x - meanX
		m2X += dx * dx2

		dy := y - meanY
		meanY += dy / float64(i+1)
		dy2 := y - meanY
		m2Y += dy * dy2

		covarXY += dx * (y - meanY)
	}
	if m2X == 0 || m2Y == 0 {
		return 0
	}
	return covarXY / math.Sqrt(m2X*m2Y)
}

// Histogram bins values into n equal-width buckets.
// Returns edges (n+1 boundaries) and counts (n buckets).
func Histogram[N Numeric](data []N, bins int) (edges []float64, counts []int) {
	if len(data) == 0 || bins < 1 {
		return nil, nil
	}
	minVal := float64(data[0])
	maxVal := float64(data[0])
	for _, v := range data[1:] {
		x := float64(v)
		if x < minVal {
			minVal = x
		}
		if x > maxVal {
			maxVal = x
		}
	}

	if minVal == maxVal {
		edges = []float64{minVal, minVal}
		counts = []int{len(data)}
		return
	}

	width := (maxVal - minVal) / float64(bins)
	edges = make([]float64, bins+1)
	for i := range edges {
		edges[i] = minVal + float64(i)*width
	}
	edges[bins] = maxVal

	counts = make([]int, bins)
	for _, v := range data {
		x := float64(v)
		idx := int((x - minVal) / width)
		if idx >= bins {
			idx = bins - 1
		}
		counts[idx]++
	}
	return
}

// --- Internal helpers ---

func medianSorted(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	sort.Float64s(vals)
	n := len(vals)
	if n%2 == 1 {
		return vals[n/2]
	}
	return (vals[n/2-1] + vals[n/2]) / 2
}

func percentileSorted(vals []float64, pct float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	sort.Float64s(vals)
	return percentileFromSorted(vals, pct)
}

// percentileFromSorted uses linear interpolation (numpy default method).
func percentileFromSorted(sorted []float64, pct float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	if pct <= 0 {
		return sorted[0]
	}
	if pct >= 100 {
		return sorted[len(sorted)-1]
	}
	n := float64(len(sorted))
	idx := (pct / 100) * (n - 1)
	lo := int(idx)
	hi := lo + 1
	if hi >= len(sorted) {
		return sorted[lo]
	}
	frac := idx - float64(lo)
	return sorted[lo]*(1-frac) + sorted[hi]*frac
}
