package golog

import (
	"crypto/rand"
	"testing"
)

func BenchmarkWrite(b *testing.B) {
	log := NewLog("data")

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

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := log.Write([]byte(data[i%pool]))

		if err != nil {
			panic(err)
		}
	}

	log.Commit()
}
