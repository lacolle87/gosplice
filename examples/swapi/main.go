package main

import (
	"context"
	"encoding/json"
	"fmt"
	gs "github.com/lacolle87/gosplice"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

type Person struct {
	Name      string   `json:"name"`
	Height    string   `json:"height"`
	Mass      string   `json:"mass"`
	HairColor string   `json:"hair_color"`
	EyeColor  string   `json:"eye_color"`
	Gender    string   `json:"gender"`
	Films     []string `json:"films"`
}

func (p Person) GetHeight() int {
	if p.Height == "" || p.Height == "unknown" {
		return 0
	}
	v, _ := strconv.Atoi(p.Height)
	return v
}

func (p Person) GetMass() float64 {
	if p.Mass == "" || p.Mass == "unknown" {
		return 0
	}
	v, _ := strconv.ParseFloat(strings.ReplaceAll(p.Mass, ",", ""), 64)
	return v
}

type Stats struct {
	Total       int
	HeightSum   int
	HeightCount int
	MassSum     float64
	MassCount   int
}

func streamPages(startURL string, client *http.Client) <-chan Person {
	ch := make(chan Person)

	go func() {
		defer close(ch)
		next := startURL

		for next != "" {
			var result struct {
				Next    string   `json:"next"`
				Results []Person `json:"results"`
			}

			var lastErr error
			for attempt := 0; attempt < 5; attempt++ {
				if attempt > 0 {
					wait := time.Duration(attempt) * 3 * time.Second
					log.Printf("retry %d for %s (waiting %v)", attempt, next, wait)
					time.Sleep(wait)
				}

				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				req, _ := http.NewRequestWithContext(ctx, "GET", next, nil)
				resp, err := client.Do(req)

				if err != nil {
					cancel()
					lastErr = err
					continue
				}

				err = json.NewDecoder(resp.Body).Decode(&result)
				resp.Body.Close()
				cancel()

				if err != nil {
					lastErr = err
					continue
				}

				lastErr = nil
				break
			}

			if lastErr != nil {
				log.Printf("giving up on %s after 5 attempts: %v", next, lastErr)
				return
			}

			for _, p := range result.Results {
				ch <- p
			}

			next = result.Next
			time.Sleep(500 * time.Millisecond)
		}
	}()

	return ch
}

func heightBucket(p Person) string {
	h := p.GetHeight()
	switch {
	case h == 0:
		return "unknown"
	case h < 150:
		return "<150cm"
	case h < 170:
		return "150-170cm"
	case h < 190:
		return "170-190cm"
	default:
		return ">190cm"
	}
}

func normalizeGender(p Person) string {
	if p.Gender == "n/a" {
		return "other"
	}
	return p.Gender
}

func normalizeHair(p Person) string {
	if p.HairColor == "n/a" || p.HairColor == "none" {
		return "bald"
	}
	return p.HairColor
}

func printDist(name string, dist map[string]int) {
	keys := make([]string, 0, len(dist))
	for k := range dist {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return dist[keys[i]] > dist[keys[j]] })

	fmt.Printf("\n%s:\n", name)
	for _, k := range keys {
		fmt.Printf("  %-12s %d\n", k, dist[k])
	}
}

func main() {
	log.SetFlags(log.Lmicroseconds)

	var fetched atomic.Int64
	start := time.Now()

	fmt.Println("Streaming ETL pipeline — swapi.dev")
	fmt.Println("===================================")

	// Phase 1: stream from API, collect all characters
	people := gs.FromChannel(streamPages("https://swapi.dev/api/people/", &http.Client{})).
		WithElementHook(gs.CountElements[Person](&fetched)).
		WithElementHook(func(p Person) {
			if fetched.Load()%10 == 0 {
				log.Printf("streamed: %d characters", fetched.Load())
			}
		}).
		WithCompletionHook(func() {
			log.Printf("stream complete: %d characters in %v", fetched.Load(), time.Since(start))
		}).
		Collect()

	// Phase 2: compute stats using Reduce (single pass)
	stats := gs.Reduce(people, Stats{}, func(s Stats, p Person) Stats {
		s.Total++
		if h := p.GetHeight(); h > 0 {
			s.HeightSum += h
			s.HeightCount++
		}
		if m := p.GetMass(); m > 0 {
			s.MassSum += m
			s.MassCount++
		}
		return s
	})

	// Phase 3: distributions via CountBy
	heightDist := gs.CountBy(gs.FromSlice(people), heightBucket)
	genderDist := gs.CountBy(gs.FromSlice(people), normalizeGender)
	hairDist := gs.CountBy(gs.FromSlice(people), normalizeHair)
	eyeDist := gs.CountBy(gs.FromSlice(people), func(p Person) string { return p.EyeColor })

	// Phase 4: find extremes
	tallest, _ := gs.MaxBy(
		gs.FromSlice(people).Filter(func(p Person) bool { return p.GetHeight() > 0 }),
		func(p Person) int { return p.GetHeight() },
	)
	heaviest, _ := gs.MaxBy(
		gs.FromSlice(people).Filter(func(p Person) bool { return p.GetMass() > 0 }),
		func(p Person) float64 { return p.GetMass() },
	)

	// Phase 5: most appearances
	type NameFilms struct {
		Name  string
		Count int
	}
	appearances := gs.PipeMap(gs.FromSlice(people), func(p Person) NameFilms {
		return NameFilms{p.Name, len(p.Films)}
	})
	mostFilms, _ := gs.MaxBy(appearances, func(nf NameFilms) int { return nf.Count })

	// Phase 6: character names by height category using GroupBy + Map
	byHeight := gs.GroupBy(gs.FromSlice(people), heightBucket)
	fmt.Println("\nCharacters by height group:")
	for _, bucket := range []string{"<150cm", "150-170cm", "170-190cm", ">190cm", "unknown"} {
		group, ok := byHeight[bucket]
		if !ok {
			continue
		}
		names := gs.Map(group, func(p Person) string { return p.Name })
		sort.Strings(names)
		fmt.Printf("  %s: %s\n", bucket, strings.Join(names, ", "))
	}

	// Output
	fmt.Println("\nRESULTS")
	fmt.Println("=======")
	fmt.Printf("Total characters: %d\n", stats.Total)

	if stats.HeightCount > 0 {
		fmt.Printf("Avg height: %.1f cm (%d known)\n",
			float64(stats.HeightSum)/float64(stats.HeightCount), stats.HeightCount)
	}
	if stats.MassCount > 0 {
		fmt.Printf("Avg mass: %.1f kg (%d known)\n",
			stats.MassSum/float64(stats.MassCount), stats.MassCount)
	}

	fmt.Printf("\nTallest: %s (%d cm)\n", tallest.Name, tallest.GetHeight())
	fmt.Printf("Heaviest: %s (%.0f kg)\n", heaviest.Name, heaviest.GetMass())
	fmt.Printf("Most films: %s (%d)\n", mostFilms.Name, mostFilms.Count)

	printDist("Height distribution", heightDist)
	printDist("Gender distribution", genderDist)
	printDist("Hair color", hairDist)
	printDist("Eye color", eyeDist)

	// Bonus: tall characters sorted by height
	tall := gs.FromSlice(people).
		Filter(func(p Person) bool { return p.GetHeight() > 180 }).
		Collect()
	sort.Slice(tall, func(i, j int) bool { return tall[i].GetHeight() > tall[j].GetHeight() })

	fmt.Printf("\nTall characters (>180cm, %d):\n", len(tall))
	_ = gs.ToWriterString(gs.FromSlice(tall), os.Stdout,
		func(p Person) string {
			return fmt.Sprintf("  %-20s %d cm\n", p.Name, p.GetHeight())
		})
}
