package main

import (
	"fmt"
	gs "github.com/lacolle87/gosplice"
	"math"
	"math/rand"
	"os"
	"sync/atomic"
	"time"
)

type Tick struct {
	Time   time.Time
	Price  float64
	Volume int
}

type Candle struct {
	Start  time.Time
	Open   float64
	High   float64
	Low    float64
	Close  float64
	Volume int
	Count  int
}

type SessionStats struct {
	Candles    int
	HighPrice  float64
	LowPrice   float64
	TotalVol   int
	StrongUp   int
	StrongDown int
}

func generateTicks(n int) <-chan Tick {
	ch := make(chan Tick)

	go func() {
		defer close(ch)

		price := 100.0
		trend := 0.0

		for i := 0; i < n; i++ {
			if rand.Float64() < 0.05 {
				trend = rand.Float64()*0.4 - 0.2
			}

			volatility := rand.Float64()*0.8 + 0.2

			jump := 0.0
			if rand.Float64() < 0.03 {
				jump = rand.Float64()*6 - 3
			}

			change := trend + rand.NormFloat64()*volatility + jump
			price = math.Max(1, price*(1+change/100))

			baseVol := rand.Intn(50) + 10
			moveFactor := int(math.Abs(change) * 20)

			ch <- Tick{
				Time:   time.Now().Add(time.Duration(i) * time.Second),
				Price:  math.Round(price*100) / 100,
				Volume: baseVol + moveFactor,
			}
		}
	}()

	return ch
}

func buildCandle(ticks []Tick) Candle {
	return gs.Reduce(ticks, Candle{
		Start: ticks[0].Time.Truncate(time.Minute),
		Open:  ticks[0].Price,
		High:  ticks[0].Price,
		Low:   ticks[0].Price,
	}, func(c Candle, t Tick) Candle {
		if t.Price > c.High {
			c.High = t.Price
		}
		if t.Price < c.Low {
			c.Low = t.Price
		}
		c.Close = t.Price
		c.Volume += t.Volume
		c.Count++
		return c
	})
}

func (c Candle) Delta() float64    { return c.Close - c.Open }
func (c Candle) DeltaPct() float64 { return c.Delta() / c.Open * 100 }
func (c Candle) IsBullish() bool   { return c.Close > c.Open }
func (c Candle) IsStrong() bool    { return math.Abs(c.DeltaPct()) > 0.5 }

func main() {
	fmt.Println("GoSplice streaming candles")
	fmt.Println("==========================")

	var tickCount atomic.Int64
	start := time.Now()

	// Stream ticks → batch into candles → collect
	candles := gs.PipeMap(
		gs.PipeBatch(
			gs.FromChannel(generateTicks(600)).
				WithElementHook(gs.CountElements[Tick](&tickCount)),
			gs.BatchConfig{Size: 60},
		),
		buildCandle,
	).
		WithElementHook(func(c Candle) {
			arrow := "—"
			if c.IsBullish() {
				arrow = "▲"
			} else if c.Delta() < 0 {
				arrow = "▼"
			}
			fmt.Printf("  %s %s O:%.2f H:%.2f L:%.2f C:%.2f V:%d (%+.2f%%)\n",
				c.Start.Format("15:04:05"), arrow,
				c.Open, c.High, c.Low, c.Close, c.Volume, c.DeltaPct())
		}).
		WithCompletionHook(func() {
			fmt.Printf("\nProcessed %d ticks in %v\n", tickCount.Load(), time.Since(start).Round(time.Millisecond))
		}).
		Collect()

	if len(candles) == 0 {
		fmt.Println("no candles generated")
		return
	}

	// Session stats: single-pass Reduce
	stats := gs.Reduce(candles, SessionStats{LowPrice: math.MaxFloat64},
		func(s SessionStats, c Candle) SessionStats {
			s.Candles++
			s.TotalVol += c.Volume
			if c.High > s.HighPrice {
				s.HighPrice = c.High
			}
			if c.Low < s.LowPrice {
				s.LowPrice = c.Low
			}
			if c.IsStrong() && c.IsBullish() {
				s.StrongUp++
			}
			if c.IsStrong() && !c.IsBullish() {
				s.StrongDown++
			}
			return s
		})

	// Extremes
	best, _ := gs.MaxBy(gs.FromSlice(candles), func(c Candle) float64 { return c.DeltaPct() })
	worst, _ := gs.MinBy(gs.FromSlice(candles), func(c Candle) float64 { return c.DeltaPct() })
	busiest, _ := gs.MaxBy(gs.FromSlice(candles), func(c Candle) int { return c.Volume })

	// Direction distribution
	dirDist := gs.CountBy(gs.FromSlice(candles), func(c Candle) string {
		switch {
		case c.DeltaPct() > 0.5:
			return "strong up"
		case c.DeltaPct() > 0:
			return "up"
		case c.DeltaPct() > -0.5:
			return "down"
		default:
			return "strong down"
		}
	})

	// Volume by direction
	bullVol := gs.SumBy(
		gs.FromSlice(candles).Filter(Candle.IsBullish),
		func(c Candle) int { return c.Volume },
	)
	bearVol := gs.SumBy(
		gs.FromSlice(candles).Filter(func(c Candle) bool { return !c.IsBullish() }),
		func(c Candle) int { return c.Volume },
	)

	// Consecutive candle patterns via PipeWindow
	windows := gs.PipeWindow(gs.FromSlice(candles), 3, 1).Collect()
	streaks := gs.Filter(windows, func(w []Candle) bool {
		return gs.Every(w, Candle.IsBullish) || gs.Every(w, func(c Candle) bool { return !c.IsBullish() })
	})

	// Output
	fmt.Println("\nSession summary")
	fmt.Println("===============")
	fmt.Printf("Candles: %d | Ticks: %d\n", stats.Candles, tickCount.Load())
	fmt.Printf("Price range: %.2f — %.2f\n", stats.LowPrice, stats.HighPrice)
	fmt.Printf("Total volume: %d (bull: %d, bear: %d)\n", stats.TotalVol, bullVol, bearVol)
	fmt.Printf("Strong moves: %d up, %d down\n", stats.StrongUp, stats.StrongDown)
	fmt.Printf("Best candle: %s (%+.2f%%)\n", best.Start.Format("15:04:05"), best.DeltaPct())
	fmt.Printf("Worst candle: %s (%+.2f%%)\n", worst.Start.Format("15:04:05"), worst.DeltaPct())
	fmt.Printf("Busiest candle: %s (vol: %d)\n", busiest.Start.Format("15:04:05"), busiest.Volume)
	fmt.Printf("3-candle streaks: %d\n", len(streaks))

	fmt.Println("\nDirection distribution:")
	for dir, count := range dirDist {
		pct := float64(count) / float64(stats.Candles) * 100
		fmt.Printf("  %-12s %d (%.0f%%)\n", dir, count, pct)
	}

	// Strong moves log via ToWriterString
	strong := gs.FromSlice(candles).Filter(Candle.IsStrong)
	strongCount := gs.FromSlice(candles).Filter(Candle.IsStrong).Count()

	if strongCount > 0 {
		fmt.Printf("\nStrong moves (%d):\n", strongCount)
		_ = gs.ToWriterString(strong, os.Stdout, func(c Candle) string {
			dir := "▲"
			if !c.IsBullish() {
				dir = "▼"
			}
			return fmt.Sprintf("  %s %s %+.2f%% (vol: %d)\n",
				c.Start.Format("15:04:05"), dir, c.DeltaPct(), c.Volume)
		})
	}
}
