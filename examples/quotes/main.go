package main

import (
	"fmt"
	gs "github.com/lacolle87/gosplice"
	"math"
	"math/rand"
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
}

func generateTicks() <-chan Tick {
	ch := make(chan Tick)

	go func() {
		defer close(ch)

		price := 100.0
		trend := 0.0

		for {
			time.Sleep(1 * time.Second)

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
			volume := baseVol + moveFactor

			ch <- Tick{
				Time:   time.Now(),
				Price:  price,
				Volume: volume,
			}
		}
	}()

	return ch
}

func buildCandle(ticks []Tick) Candle {
	open := ticks[0].Price
	high := open
	low := open
	totalVol := 0

	for _, t := range ticks {
		if t.Price > high {
			high = t.Price
		}
		if t.Price < low {
			low = t.Price
		}
		totalVol += t.Volume
	}

	return Candle{
		Start:  ticks[0].Time.Truncate(time.Minute),
		Open:   open,
		High:   high,
		Low:    low,
		Close:  ticks[len(ticks)-1].Price,
		Volume: totalVol,
	}
}

func main() {
	fmt.Println("GoSplice streaming candles with hooks")
	fmt.Println("====================================")

	// SOURCE
	source := gs.FromChannel(generateTicks()).
		WithElementHook(func(t Tick) {
			if rand.Intn(20) == 0 {
				fmt.Printf("tick: %.2f vol=%d\n", t.Price, t.Volume)
			}
		})

	// PIPELINE
	candlesPipeline := gs.PipeMap(
		gs.PipeBatch(source, gs.BatchConfig{Size: 60}),
		func(batch []Tick) Candle {
			return buildCandle(batch)
		},
	).
		WithElementHook(func(c Candle) {
			fmt.Printf(
				"\n🕯 %s | O: %.2f H: %.2f L: %.2f C: %.2f V: %d\n",
				c.Start.Format("15:04"),
				c.Open,
				c.High,
				c.Low,
				c.Close,
				c.Volume,
			)
		}).
		WithCompletionHook(func() {
			fmt.Println("stream finished")
		})

	// EXTRA: фильтр сильных свечей
	strongMoves := candlesPipeline.Filter(func(c Candle) bool {
		return math.Abs(c.Close-c.Open) > 1.5
	})

	strongMoves = strongMoves.WithElementHook(func(c Candle) {
		fmt.Printf("🔥 strong move: %s Δ=%.2f\n",
			c.Start.Format("15:04"),
			c.Close-c.Open,
		)
	})

	// SINK → канал
	out := make(chan Candle, 10)
	go strongMoves.ToChannel(out)

	// CONSUMER
	for c := range out {
		_ = c
	}
}
