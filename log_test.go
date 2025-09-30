package golog

import (
	"crypto/rand"
	"sort"
	"testing"
	"time"
)

func BenchmarkWrite(b *testing.B) {
	log := NewLog[any]("data", 64<<20)

	log.Load()

	const pool = 200_000
	data := make([][]byte, pool)

	for i := 0; i < pool; i++ {
		buf := make([]byte, 64)

		_, err := rand.Read(buf)

		if err != nil {
			b.Fatalf("failed to generate random bytes: %v", err)
		}

		data[i] = buf
	}

	latencies := make([]time.Duration, b.N)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		start := time.Now()

		_, err := log.Write([]byte(data[i%pool]))

		if err != nil {
			panic(err)
		}

		if err := log.Commit(); err != nil {
			panic(err)
		}

		latencies[i] = time.Since(start)
	}

	b.StopTimer()

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

	p50 := latencies[len(latencies)*50/100]
	p95 := latencies[len(latencies)*95/100]
	p99 := latencies[len(latencies)*99/100]

	b.ReportMetric(float64(p50.Milliseconds()), "p50_ms")
	b.ReportMetric(float64(p95.Milliseconds()), "p95_ms")
	b.ReportMetric(float64(p99.Milliseconds()), "p99_ms")

	log.Commit()
}
