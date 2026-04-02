package main

import (
	"errors"
	"fmt"
	gs "github.com/lacolle87/gosplice"
	"math"
	"math/rand"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

type Property struct {
	ID       int
	Address  string
	District string
	Type     string
	Rooms    int
	Area     float64
	Price    float64
	Floor    int
	Floors   int
	Year     int
	Parking  bool
	Balcony  bool
	ListedAt time.Time
	SoldAt   *time.Time
	AgentID  int
}

type Label struct {
	Tag      string
	Priority int
}

type LabeledProperty struct {
	Property Property
	Labels   []Label
	PriceM2  float64
	DaysOn   int
	Score    int
}

type AgentReport struct {
	AgentID  int
	Sold     int
	Revenue  float64
	AvgDays  float64
	AvgPrice float64
}

type DistrictSummary struct {
	District   string
	Count      int
	AvgPriceM2 float64
	MinPrice   float64
	MaxPrice   float64
	AvgArea    float64
}

func generateListings(n int) []Property {
	districts := []string{"Center", "Westend", "Nordend", "Sachsenhausen", "Bornheim", "Bockenheim", "Ostend", "Gallus"}
	types := []string{"apartment", "studio", "penthouse", "loft"}
	streets := []string{"Hauptstr.", "Berliner Str.", "Schillerstr.", "Goethestr.", "Friedberger Landstr.", "Hanauer Landstr."}

	now := time.Now()
	props := make([]Property, n)

	for i := range props {
		district := districts[rand.Intn(len(districts))]
		typ := types[rand.Intn(len(types))]
		rooms := rand.Intn(4) + 1
		if typ == "studio" {
			rooms = 1
		}
		if typ == "penthouse" {
			rooms = rand.Intn(3) + 3
		}

		area := float64(rooms)*18 + rand.Float64()*30 + 20
		basePrice := area * (3500 + rand.Float64()*4000)
		if district == "Center" || district == "Westend" {
			basePrice *= 1.3
		}
		if typ == "penthouse" {
			basePrice *= 1.5
		}

		floor := rand.Intn(8) + 1
		floors := floor + rand.Intn(4)
		year := 1960 + rand.Intn(64)
		listed := now.AddDate(0, 0, -rand.Intn(180))

		var sold *time.Time
		if rand.Float64() < 0.6 {
			s := listed.AddDate(0, 0, rand.Intn(90)+5)
			if s.Before(now) {
				sold = &s
			}
		}

		a := math.Round(area*10) / 10
		if rand.Float64() < 0.03 {
			a = 0
		}

		props[i] = Property{
			ID:       1000 + i,
			Address:  fmt.Sprintf("%s %d", streets[rand.Intn(len(streets))], rand.Intn(120)+1),
			District: district,
			Type:     typ,
			Rooms:    rooms,
			Area:     a,
			Price:    math.Round(basePrice/100) * 100,
			Floor:    floor,
			Floors:   floors,
			Year:     year,
			Parking:  rand.Float64() < 0.4,
			Balcony:  rand.Float64() < 0.6,
			ListedAt: listed,
			SoldAt:   sold,
			AgentID:  rand.Intn(8) + 1,
		}
	}
	return props
}

func labelProperty(p Property, avgPriceM2 float64) (LabeledProperty, error) {
	if p.Area <= 0 {
		return LabeledProperty{}, errors.New("invalid area")
	}
	if p.Price <= 0 {
		return LabeledProperty{}, errors.New("invalid price")
	}

	priceM2 := p.Price / p.Area
	now := time.Now()

	daysOn := int(now.Sub(p.ListedAt).Hours() / 24)
	if p.SoldAt != nil {
		daysOn = int(p.SoldAt.Sub(p.ListedAt).Hours() / 24)
	}

	var labels []Label
	score := 50

	ratio := priceM2 / avgPriceM2
	switch {
	case ratio < 0.8:
		labels = append(labels, Label{"underpriced", 1})
		score += 20
	case ratio > 1.2:
		labels = append(labels, Label{"premium", 2})
		score -= 10
	}

	if p.Area/float64(p.Rooms) > 28 {
		labels = append(labels, Label{"spacious", 3})
		score += 10
	}
	if p.Floor == p.Floors {
		labels = append(labels, Label{"top-floor", 3})
		score += 5
	}
	if p.Year >= 2015 {
		labels = append(labels, Label{"new-build", 2})
		score += 10
	} else if p.Year < 1980 {
		labels = append(labels, Label{"needs-renovation", 3})
		score -= 10
	}
	if p.Parking && p.Balcony {
		labels = append(labels, Label{"full-package", 2})
		score += 10
	}
	if daysOn > 90 {
		labels = append(labels, Label{"stale", 1})
		score -= 15
	} else if daysOn < 14 {
		labels = append(labels, Label{"fresh", 2})
		score += 5
	}
	if p.SoldAt != nil && daysOn < 30 {
		labels = append(labels, Label{"fast-sale", 1})
	}

	if score > 100 {
		score = 100
	}
	if score < 0 {
		score = 0
	}

	return LabeledProperty{
		Property: p,
		Labels:   labels,
		PriceM2:  math.Round(priceM2*100) / 100,
		DaysOn:   daysOn,
		Score:    score,
	}, nil
}

func hasLabel(lp LabeledProperty, tag string) bool {
	return gs.Some(lp.Labels, func(l Label) bool { return l.Tag == tag })
}

func labelTags(lp LabeledProperty) string {
	return strings.Join(gs.Map(lp.Labels, func(l Label) string { return l.Tag }), ", ")
}

func isSold(lp LabeledProperty) bool { return lp.Property.SoldAt != nil }

func main() {
	fmt.Println("Real estate market analytics")
	fmt.Println("============================\n")

	listings := generateListings(500)

	// Market baseline via pipeline terminal
	validListings := gs.FromSlice(listings).
		Filter(func(p Property) bool { return p.Area > 0 })

	validCount := gs.FromSlice(listings).
		Filter(func(p Property) bool { return p.Area > 0 }).
		Count()

	avgPriceM2 := gs.SumBy(validListings, func(p Property) float64 {
		return p.Price / p.Area
	}) / float64(validCount)

	fmt.Printf("Market baseline: %.0f EUR/m2 (%d listings, %d valid)\n",
		avgPriceM2, len(listings), validCount)

	// Main pipeline: validate → label → collect
	// PipeMapErr skips bad data (area=0), error hooks track drops
	var processed atomic.Int64
	var badData []error

	labeled := gs.PipeMapErr(
		gs.FromSlice(listings).
			WithElementHook(gs.CountElements[Property](&processed)).
			WithErrorHook(gs.CollectErrors[Property](&badData)),
		func(p Property) (LabeledProperty, error) {
			return labelProperty(p, avgPriceM2)
		},
	).Collect()

	fmt.Printf("Processed: %d | Labeled: %d | Skipped: %d bad records\n",
		processed.Load(), len(labeled), len(badData))

	// Label frequency: FlatMap labels → CountBy
	allLabels := gs.FlatMap(labeled, func(lp LabeledProperty) []string {
		return gs.Map(lp.Labels, func(l Label) string { return l.Tag })
	})

	fmt.Println("\nLabel distribution:")
	for tag, count := range gs.CountBy(gs.FromSlice(allLabels), func(s string) string { return s }) {
		fmt.Printf("  %-20s %3d (%4.1f%%)\n", tag, count, float64(count)/float64(len(labeled))*100)
	}

	// District summary: GroupBy → Reduce per group
	byDistrict := gs.GroupBy(gs.FromSlice(labeled), func(lp LabeledProperty) string {
		return lp.Property.District
	})

	fmt.Println("\nDistrict overview:")
	fmt.Printf("  %-16s %5s %10s %10s %10s %8s\n",
		"District", "Count", "Avg EUR/m2", "Min price", "Max price", "Avg m2")

	for district, group := range byDistrict {
		s := gs.Reduce(group, DistrictSummary{
			District: district, MinPrice: math.MaxFloat64,
		}, func(s DistrictSummary, lp LabeledProperty) DistrictSummary {
			s.Count++
			s.AvgPriceM2 += lp.PriceM2
			s.AvgArea += lp.Property.Area
			if lp.Property.Price < s.MinPrice {
				s.MinPrice = lp.Property.Price
			}
			if lp.Property.Price > s.MaxPrice {
				s.MaxPrice = lp.Property.Price
			}
			return s
		})
		fmt.Printf("  %-16s %5d %10.0f %10.0f %10.0f %8.1f\n",
			s.District, s.Count, s.AvgPriceM2/float64(s.Count),
			s.MinPrice, s.MaxPrice, s.AvgArea/float64(s.Count))
	}

	// Type breakdown: CountBy + SumBy with pipeline Filter
	fmt.Println("\nBy property type:")
	for typ, count := range gs.CountBy(gs.FromSlice(labeled), func(lp LabeledProperty) string { return lp.Property.Type }) {
		avgScore := gs.SumBy(
			gs.FromSlice(labeled).Filter(func(lp LabeledProperty) bool { return lp.Property.Type == typ }),
			func(lp LabeledProperty) int { return lp.Score },
		) / count
		fmt.Printf("  %-12s %3d listings, avg score: %d\n", typ, count, avgScore)
	}

	// Partition sold/active
	sold, active := gs.Partition(gs.FromSlice(labeled), isSold)
	fmt.Printf("\nSold: %d | Active: %d (%.0f%% conversion)\n",
		len(sold), len(active), float64(len(sold))/float64(len(labeled))*100)

	// Sold analytics via pipeline terminals
	if len(sold) > 0 {
		avgDays := gs.SumBy(gs.FromSlice(sold), func(lp LabeledProperty) int { return lp.DaysOn }) / len(sold)
		fastest, _ := gs.MinBy(gs.FromSlice(sold), func(lp LabeledProperty) int { return lp.DaysOn })
		priciest, _ := gs.MaxBy(gs.FromSlice(sold), func(lp LabeledProperty) float64 { return lp.Property.Price })

		fmt.Printf("Avg days to sell: %d\n", avgDays)
		fmt.Printf("Fastest sale: %s (%d days, %.0f EUR)\n",
			fastest.Property.Address, fastest.DaysOn, fastest.Property.Price)
		fmt.Printf("Highest sale: %s (%.0f EUR, %s)\n",
			priciest.Property.Address, priciest.Property.Price, priciest.Property.District)

		// Fast sales pipeline
		fastCount := gs.FromSlice(sold).
			Filter(func(lp LabeledProperty) bool { return lp.DaysOn < 30 }).
			Count()
		fmt.Printf("Fast sales (<30 days): %d (%.0f%% of sold)\n",
			fastCount, float64(fastCount)/float64(len(sold))*100)
	}

	// Agent leaderboard: GroupBy → Map → MaxBy
	fmt.Println("\nAgent leaderboard:")
	agentSold := gs.GroupBy(gs.FromSlice(sold), func(lp LabeledProperty) int { return lp.Property.AgentID })

	reports := gs.Map(
		gs.Unique(gs.Map(sold, func(lp LabeledProperty) int { return lp.Property.AgentID })),
		func(id int) AgentReport {
			group := agentSold[id]
			rev := gs.SumBy(gs.FromSlice(group), func(lp LabeledProperty) float64 { return lp.Property.Price })
			days := gs.SumBy(gs.FromSlice(group), func(lp LabeledProperty) int { return lp.DaysOn })
			return AgentReport{
				AgentID: id, Sold: len(group), Revenue: rev,
				AvgDays: float64(days) / float64(len(group)), AvgPrice: rev / float64(len(group)),
			}
		},
	)

	topSeller, _ := gs.MaxBy(gs.FromSlice(reports), func(r AgentReport) int { return r.Sold })
	topRev, _ := gs.MaxBy(gs.FromSlice(reports), func(r AgentReport) float64 { return r.Revenue })
	fastestAg, _ := gs.MinBy(gs.FromSlice(reports), func(r AgentReport) float64 { return r.AvgDays })

	fmt.Printf("  Top seller:   Agent #%d (%d sold, avg %.0f days)\n", topSeller.AgentID, topSeller.Sold, topSeller.AvgDays)
	fmt.Printf("  Top revenue:  Agent #%d (%.0f EUR, %d deals)\n", topRev.AgentID, topRev.Revenue, topRev.Sold)
	fmt.Printf("  Fastest avg:  Agent #%d (%.0f days avg, %d deals)\n", fastestAg.AgentID, fastestAg.AvgDays, fastestAg.Sold)

	// Buyer opportunities: pipeline Filter chain → ToWriterString
	oppCount := gs.FromSlice(active).
		Filter(func(lp LabeledProperty) bool { return hasLabel(lp, "underpriced") }).
		Filter(func(lp LabeledProperty) bool { return lp.Score >= 60 }).
		Count()

	if oppCount > 0 {
		fmt.Printf("\nBuyer opportunities (%d underpriced, score >= 60):\n", oppCount)
		_ = gs.ToWriterString(
			gs.FromSlice(active).
				Filter(func(lp LabeledProperty) bool { return hasLabel(lp, "underpriced") }).
				Filter(func(lp LabeledProperty) bool { return lp.Score >= 60 }),
			os.Stdout,
			func(lp LabeledProperty) string {
				return fmt.Sprintf("  #%d %-28s %-14s %dr %.0fm2  %6.0f EUR (%4.0f/m2) score:%d [%s]\n",
					lp.Property.ID, lp.Property.Address, lp.Property.District,
					lp.Property.Rooms, lp.Property.Area, lp.Property.Price,
					lp.PriceM2, lp.Score, labelTags(lp))
			},
		)
	}

	// Stale listings: pipeline Filter → ForEach
	now := time.Now()
	staleCount := gs.FromSlice(active).
		Filter(func(lp LabeledProperty) bool { return hasLabel(lp, "stale") }).
		Count()

	if staleCount > 0 {
		fmt.Printf("\nStale listings needing price cut (%d):\n", staleCount)
		gs.FromSlice(active).
			Filter(func(lp LabeledProperty) bool { return hasLabel(lp, "stale") }).
			ForEach(func(lp LabeledProperty) {
				over := int(now.Sub(lp.Property.ListedAt).Hours()/24) - 90
				newPrice := lp.Property.Price * 0.95
				fmt.Printf("  #%d %s — %d days overdue, %.0f EUR → %.0f EUR (-5%%)\n",
					lp.Property.ID, lp.Property.Address, over,
					lp.Property.Price, newPrice)
			})
	}

	// Score distribution
	fmt.Println("\nScore distribution:")
	for bucket, count := range gs.CountBy(gs.FromSlice(labeled), func(lp LabeledProperty) string {
		switch {
		case lp.Score >= 80:
			return "80-100 excellent"
		case lp.Score >= 60:
			return "60-79  good"
		case lp.Score >= 40:
			return "40-59  average"
		default:
			return "0-39   poor"
		}
	}) {
		bar := strings.Repeat("█", count*40/len(labeled))
		fmt.Printf("  %s %3d %s\n", bucket, count, bar)
	}
}
