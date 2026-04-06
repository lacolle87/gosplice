package gosplice

import (
	"context"
	"errors"
	"runtime"
	"testing"
)

func BenchmarkPipelineFilter(b *testing.B) {
	data := makeRange(10000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		FromSlice(data).
			Filter(func(n int) bool { return n%2 == 0 }).
			Collect()
	}
}

func BenchmarkPipelineFilterTake(b *testing.B) {
	data := makeRange(10000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		FromSlice(data).
			Filter(func(n int) bool { return n%2 == 0 }).
			Take(100).
			Collect()
	}
}

func BenchmarkPipeMap(b *testing.B) {
	data := makeRange(10000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		PipeMap(FromSlice(data), func(n int) int { return n * 2 }).Collect()
	}
}

func BenchmarkPipeMapParallel4(b *testing.B) {
	data := makeRange(10000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		PipeMapParallel(FromSlice(data), 4, func(n int) int { return n * 2 }).Collect()
	}
}

func BenchmarkPipeMapParallel8(b *testing.B) {
	data := makeRange(10000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		PipeMapParallel(FromSlice(data), 8, func(n int) int { return n * 2 }).Collect()
	}
}

func BenchmarkPipeFilterParallel4(b *testing.B) {
	data := makeRange(10000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		PipeFilterParallel(FromSlice(data), 4, func(n int) bool { return n%2 == 0 }).Collect()
	}
}

func BenchmarkPipeFlatMap(b *testing.B) {
	data := makeRange(1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		PipeFlatMap(FromSlice(data), func(n int) []int { return []int{n, n + 1} }).Collect()
	}
}

func BenchmarkPipeDistinct(b *testing.B) {
	data := make([]int, 10000)
	for i := range data {
		data[i] = i % 100
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		PipeDistinct(FromSlice(data)).Collect()
	}
}

func BenchmarkPipeChunk(b *testing.B) {
	data := makeRange(10000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		PipeChunk(FromSlice(data), 100).Collect()
	}
}

func BenchmarkPipeWindow(b *testing.B) {
	data := makeRange(10000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		PipeWindow(FromSlice(data), 5, 1).Collect()
	}
}

func BenchmarkPipelineReduce(b *testing.B) {
	data := makeRange(10000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		FromSlice(data).Reduce(0, func(a, b int) int { return a + b })
	}
}

func BenchmarkGroupBy(b *testing.B) {
	data := makeRange(10000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		GroupBy(FromSlice(data), func(n int) int { return n % 10 })
	}
}

func BenchmarkCountBy(b *testing.B) {
	data := makeRange(10000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CountBy(FromSlice(data), func(n int) int { return n % 10 })
	}
}

func BenchmarkSumBy(b *testing.B) {
	data := makeRange(10000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		SumBy(FromSlice(data), func(n int) int { return n })
	}
}

func BenchmarkPipeBatch(b *testing.B) {
	data := makeRange(10000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		PipeBatch(FromSlice(data), BatchConfig{Size: 100}).Collect()
	}
}

func BenchmarkPipelineChained(b *testing.B) {
	data := makeRange(10000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		PipeMap(
			FromSlice(data).
				Filter(func(n int) bool { return n%2 == 0 }).
				Take(1000),
			func(n int) int { return n * 3 },
		).Collect()
	}
}

func BenchmarkFromChannel(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ch := make(chan int, 1000)
		go func() {
			for j := 0; j < 1000; j++ {
				ch <- j
			}
			close(ch)
		}()
		FromChannel(ch).Collect()
	}
}

func BenchmarkPipelineVsStandalone_Map(b *testing.B) {
	data := makeRange(10000)

	b.Run("standalone", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			Map(data, func(n int) int { return n * 2 })
		}
	})

	b.Run("pipeline", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			PipeMap(FromSlice(data), func(n int) int { return n * 2 }).Collect()
		}
	})
}

func BenchmarkPipelineVsStandalone_Filter(b *testing.B) {
	data := makeRange(10000)

	b.Run("standalone", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			Filter(data, func(n int) bool { return n%2 == 0 })
		}
	})

	b.Run("pipeline", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			FromSlice(data).
				Filter(func(n int) bool { return n%2 == 0 }).
				Collect()
		}
	})
}

func BenchmarkPipelineVsStandalone_Reduce(b *testing.B) {
	data := makeRange(10000)

	b.Run("standalone", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			Reduce(data, 0, func(a, b int) int { return a + b })
		}
	})

	b.Run("pipeline", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			FromSlice(data).Reduce(0, func(a, b int) int { return a + b })
		}
	})
}

func BenchmarkPipelineCollect(b *testing.B) {
	data := makeRange(10000)

	b.Run("direct_slice", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			FromSlice(data).Collect()
		}
	})

	b.Run("after_filter", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			FromSlice(data).
				Filter(func(n int) bool { return n%2 == 0 }).
				Collect()
		}
	})

	b.Run("after_map", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			PipeMap(FromSlice(data), func(n int) int { return n * 2 }).Collect()
		}
	})
}

func BenchmarkPipeMapParallelHeavy(b *testing.B) {
	data := makeRange(10000)
	heavy := func(n int) int {
		x := n
		for j := 0; j < 100; j++ {
			x = x*31 + 17
		}
		return x
	}

	b.Run("sequential", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			PipeMap(FromSlice(data), heavy).Collect()
		}
	})

	b.Run("parallel_4", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			PipeMapParallel(FromSlice(data), 4, heavy).Collect()
		}
	})

	b.Run("parallel_8", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			PipeMapParallel(FromSlice(data), 8, heavy).Collect()
		}
	})
}

// ---------------------------------------------------------------------------
// Parallel stream, error handling, finalize, foldWhile edge cases
// ---------------------------------------------------------------------------

func BenchmarkParallelStreamNoCtx(b *testing.B) {
	data := makeRange(100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		PipeMapParallelStream(FromSlice(data), 4, 16,
			func(n int) int { return n * 2 }).Collect()
	}
}

func BenchmarkParallelStreamWithCtx(b *testing.B) {
	data := makeRange(100)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		PipeMapParallelStream(FromSlice(data).WithContext(ctx), 4, 16,
			func(n int) int { return n * 2 }).Collect()
	}
}

func BenchmarkMapErrRetries(b *testing.B) {
	data := makeRange(5)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		PipeMapErr(
			FromSlice(data).WithErrorHandler(RetryHandler[int](3, 0)).WithMaxRetries(5),
			func(n int) (int, error) {
				if n == 3 {
					return 0, errors.New("fail")
				}
				return n, nil
			}).Collect()
	}
}

func BenchmarkFinalize(b *testing.B) {
	for i := 0; i < b.N; i++ {
		FromSlice([]int{1, 2, 3}).WithCompletionHook(func() {}).Collect()
	}
}

func BenchmarkFoldWhileAnyEarlyExit(b *testing.B) {
	data := makeRange(10000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		FromSlice(data).Any(func(n int) bool { return n == 9999 })
	}
}

func BenchmarkFoldWhileAnyNoMatch(b *testing.B) {
	data := makeRange(10000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		FromSlice(data).Any(func(n int) bool { return n < 0 })
	}
}

func makeRange(n int) []int {
	s := make([]int, n)
	for i := range s {
		s[i] = i
	}
	return s
}

func BenchmarkPipeMapParallel_CheapFn_10k(b *testing.B) {
	data := makeIterData(10_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		PipeMapParallel(FromSlice(data), runtime.NumCPU(), func(n int) int { return n * 2 }).Collect()
	}
}

func BenchmarkPipeMapSequential_CheapFn_10k(b *testing.B) {
	data := makeIterData(10_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		PipeMap(FromSlice(data), func(n int) int { return n * 2 }).Collect()
	}
}

func BenchmarkPipeBatch_NoTimeout_10k(b *testing.B) {
	data := makeIterData(10_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		PipeBatch(FromSlice(data), BatchConfig{Size: 100}).Collect()
	}
}

func BenchmarkPipeMapErr_AllSuccess_10k(b *testing.B) {
	data := makeIterData(10_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		PipeMapErr(FromSlice(data), func(n int) (int, error) { return n * 2, nil }).Collect()
	}
}

func BenchmarkPartition_10k(b *testing.B) {
	data := makeIterData(10_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Partition(FromSlice(data), func(n int) bool { return n%2 == 0 })
	}
}

func BenchmarkGroupBy_HighCard_10k(b *testing.B) {
	data := makeIterData(10_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		GroupBy(FromSlice(data), func(n int) int { return n })
	}
}

func BenchmarkPipeWindow_Size10_Step1_10k(b *testing.B) {
	data := makeIterData(10_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		PipeWindow(FromSlice(data), 10, 1).Collect()
	}
}

func BenchmarkCollect_FilterChain_10k(b *testing.B) {
	data := makeIterData(10_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		FromSlice(data).
			Filter(func(n int) bool { return n%2 == 0 }).
			Filter(func(n int) bool { return n > 1000 }).
			Collect()
	}
}
