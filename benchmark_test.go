package golog

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"testing"
)

// benchData generates a pool of random byte slices of the given size.
func benchData(b *testing.B, size int, count int) [][]byte {
	b.Helper()
	data := make([][]byte, count)
	for i := range data {
		buf := make([]byte, size)
		if _, err := rand.Read(buf); err != nil {
			b.Fatal(err)
		}
		data[i] = buf
	}
	return data
}

// benchLog creates a Log backed by a temporary directory that is automatically
// cleaned up when the benchmark finishes.
func benchLog(b *testing.B, maxSegmentSize uint64) *Log {
	b.Helper()
	return NewLog(b.TempDir(), maxSegmentSize)
}

// ---------------------------------------------------------------------------
// Write-only (buffered, no fsync)
// ---------------------------------------------------------------------------

func BenchmarkWriteOnly(b *testing.B) {
	for _, size := range []int{64, 256, 1024, 4096} {
		b.Run(fmt.Sprintf("payload=%dB", size), func(b *testing.B) {
			const pool = 10_000
			data := benchData(b, size, pool)
			log := benchLog(b, 64<<20)
			defer log.Close()

			b.SetBytes(int64(size))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if _, err := log.Write(data[i%pool]); err != nil {
					b.Fatal(err)
				}
			}

			b.StopTimer()
		})
	}
}

// ---------------------------------------------------------------------------
// Write + Commit (durable write, fsync after every entry)
// ---------------------------------------------------------------------------

func BenchmarkWriteAndCommit(b *testing.B) {
	for _, size := range []int{64, 256, 1024, 4096} {
		b.Run(fmt.Sprintf("payload=%dB", size), func(b *testing.B) {
			const pool = 10_000
			data := benchData(b, size, pool)
			log := benchLog(b, 64<<20)
			defer log.Close()

			b.SetBytes(int64(size))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if _, err := log.Write(data[i%pool]); err != nil {
					b.Fatal(err)
				}
				if err := log.Commit(); err != nil {
					b.Fatal(err)
				}
			}

			b.StopTimer()
		})
	}
}

// ---------------------------------------------------------------------------
// Batched writes then single commit
// ---------------------------------------------------------------------------

func BenchmarkWriteBatchCommit(b *testing.B) {
	for _, batchSize := range []int{1, 10, 100, 1000} {
		b.Run(fmt.Sprintf("batch=%d", batchSize), func(b *testing.B) {
			const pool = 10_000
			data := benchData(b, 64, pool)
			log := benchLog(b, 64<<20)
			defer log.Close()

			b.SetBytes(int64(64 * batchSize))
			b.ResetTimer()

			idx := 0
			for i := 0; i < b.N; i++ {
				for j := 0; j < batchSize; j++ {
					if _, err := log.Write(data[idx%pool]); err != nil {
						b.Fatal(err)
					}
					idx++
				}
				if err := log.Commit(); err != nil {
					b.Fatal(err)
				}
			}

			b.StopTimer()
		})
	}
}

// ---------------------------------------------------------------------------
// Sequential Read
// ---------------------------------------------------------------------------

func BenchmarkReadSequential(b *testing.B) {
	for _, size := range []int{64, 256, 1024, 4096} {
		b.Run(fmt.Sprintf("payload=%dB", size), func(b *testing.B) {
			const entries = 10_000
			data := benchData(b, size, entries)
			log := benchLog(b, 64<<20)
			defer log.Close()

			for i := 0; i < entries; i++ {
				if _, err := log.Write(data[i]); err != nil {
					b.Fatal(err)
				}
			}
			if err := log.Commit(); err != nil {
				b.Fatal(err)
			}

			b.SetBytes(int64(size))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				idx := uint64(i%entries) + 1
				if _, err := log.Read(idx); err != nil {
					b.Fatal(err)
				}
			}

			b.StopTimer()
		})
	}
}

// ---------------------------------------------------------------------------
// Random Read
// ---------------------------------------------------------------------------

func BenchmarkReadRandom(b *testing.B) {
	for _, size := range []int{64, 256, 1024, 4096} {
		b.Run(fmt.Sprintf("payload=%dB", size), func(b *testing.B) {
			const entries = 10_000
			data := benchData(b, size, entries)
			log := benchLog(b, 64<<20)
			defer log.Close()

			for i := 0; i < entries; i++ {
				if _, err := log.Write(data[i]); err != nil {
					b.Fatal(err)
				}
			}
			if err := log.Commit(); err != nil {
				b.Fatal(err)
			}

			// Pre-generate random indices to avoid rand overhead in the hot loop.
			indices := make([]uint64, b.N)
			for i := range indices {
				n, err := rand.Int(rand.Reader, big.NewInt(int64(entries)))
				if err != nil {
					b.Fatal(err)
				}
				indices[i] = n.Uint64() + 1
			}

			b.SetBytes(int64(size))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if _, err := log.Read(indices[i]); err != nil {
					b.Fatal(err)
				}
			}

			b.StopTimer()
		})
	}
}

// ---------------------------------------------------------------------------
// Load (recovery from disk)
// ---------------------------------------------------------------------------

func BenchmarkLoad(b *testing.B) {
	for _, entryCount := range []int{1_000, 10_000, 50_000} {
		b.Run(fmt.Sprintf("entries=%d", entryCount), func(b *testing.B) {
			dir := b.TempDir()
			data := benchData(b, 64, 1)

			// Populate the log once.
			setup := NewLog(dir, 64<<20)
			for i := 0; i < entryCount; i++ {
				if _, err := setup.Write(data[0]); err != nil {
					b.Fatal(err)
				}
			}
			if err := setup.Commit(); err != nil {
				b.Fatal(err)
			}
			if err := setup.Close(); err != nil {
				b.Fatal(err)
			}

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				log := NewLog(dir, 64<<20)
				if err := log.Load(); err != nil {
					b.Fatal(err)
				}
				log.Close()
			}

			b.StopTimer()
		})
	}
}

// ---------------------------------------------------------------------------
// Write with segment rotation (small segments)
// ---------------------------------------------------------------------------

func BenchmarkWriteWithSegmentRotation(b *testing.B) {
	// Each entry is 20 (header) + 64 (data) = 84 bytes. A segment size of
	// 1024 bytes forces rotation roughly every 12 entries.
	const pool = 10_000
	data := benchData(b, 64, pool)
	log := benchLog(b, 1024)
	defer log.Close()

	b.SetBytes(64)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := log.Write(data[i%pool]); err != nil {
			b.Fatal(err)
		}
	}

	b.StopTimer()
}

// ---------------------------------------------------------------------------
// WriteCommit (background flusher path)
// ---------------------------------------------------------------------------

func BenchmarkWriteCommitDurable(b *testing.B) {
	const pool = 10_000
	data := benchData(b, 64, pool)
	log := benchLog(b, 64<<20)
	defer log.Close()

	b.SetBytes(64)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := log.WriteCommit(data[i%pool]); err != nil {
			b.Fatal(err)
		}
	}

	b.StopTimer()
}

// ---------------------------------------------------------------------------
// TruncateFrom (Raft rollback)
// ---------------------------------------------------------------------------

func BenchmarkTruncateFrom(b *testing.B) {
	for _, entryCount := range []int{100, 1_000, 10_000} {
		b.Run(fmt.Sprintf("entries=%d", entryCount), func(b *testing.B) {
			const payloadSize = 64
			data := benchData(b, payloadSize, 1)

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				b.StopTimer()

				log := benchLog(b, 64<<20)
				for j := 0; j < entryCount; j++ {
					if _, err := log.Write(data[0]); err != nil {
						b.Fatal(err)
					}
				}
				if err := log.Commit(); err != nil {
					b.Fatal(err)
				}

				// Truncate the last half.
				truncateAt := uint64(entryCount/2) + 1

				b.StartTimer()

				if err := log.TruncateFrom(truncateAt); err != nil {
					b.Fatal(err)
				}

				b.StopTimer()
				log.Close()
			}
		})
	}
}

// ---------------------------------------------------------------------------
// TruncateTo (log compaction)
// ---------------------------------------------------------------------------

func BenchmarkTruncateTo(b *testing.B) {
	for _, entryCount := range []int{100, 1_000, 10_000} {
		b.Run(fmt.Sprintf("entries=%d", entryCount), func(b *testing.B) {
			const payloadSize = 64
			data := benchData(b, payloadSize, 1)

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				b.StopTimer()

				log := benchLog(b, 64<<20)
				for j := 0; j < entryCount; j++ {
					if _, err := log.Write(data[0]); err != nil {
						b.Fatal(err)
					}
				}
				if err := log.Commit(); err != nil {
					b.Fatal(err)
				}

				// Compact the first half.
				compactTo := uint64(entryCount / 2)

				b.StartTimer()

				if err := log.TruncateTo(compactTo); err != nil {
					b.Fatal(err)
				}

				b.StopTimer()
				log.Close()
			}
		})
	}
}

// ---------------------------------------------------------------------------
// GetLastIndex / GetCommittedIndex
// ---------------------------------------------------------------------------

func BenchmarkGetLastIndex(b *testing.B) {
	log := benchLog(b, 64<<20)
	defer log.Close()

	data := benchData(b, 64, 1)
	for i := 0; i < 1_000; i++ {
		if _, err := log.Write(data[0]); err != nil {
			b.Fatal(err)
		}
	}
	if err := log.Commit(); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := log.GetLastIndex(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGetCommittedIndex(b *testing.B) {
	log := benchLog(b, 64<<20)
	defer log.Close()

	data := benchData(b, 64, 1)
	for i := 0; i < 1_000; i++ {
		if _, err := log.Write(data[0]); err != nil {
			b.Fatal(err)
		}
	}
	if err := log.Commit(); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := log.GetCommittedIndex(); err != nil {
			b.Fatal(err)
		}
	}
}

// ---------------------------------------------------------------------------
// Entry serialization
// ---------------------------------------------------------------------------

func BenchmarkNewEntry(b *testing.B) {
	for _, size := range []int{64, 256, 1024, 4096} {
		b.Run(fmt.Sprintf("payload=%dB", size), func(b *testing.B) {
			data := make([]byte, size)
			if _, err := rand.Read(data); err != nil {
				b.Fatal(err)
			}

			b.SetBytes(int64(size))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				_ = NewEntry(data, uint64(i)+1)
			}
		})
	}
}

func BenchmarkEntryToBytes(b *testing.B) {
	for _, size := range []int{64, 256, 1024, 4096} {
		b.Run(fmt.Sprintf("payload=%dB", size), func(b *testing.B) {
			data := make([]byte, size)
			if _, err := rand.Read(data); err != nil {
				b.Fatal(err)
			}
			entry := NewEntry(data, 1)

			b.SetBytes(int64(EntryHeaderSize + size))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				_ = entry.ToBytes()
			}
		})
	}
}

// ---------------------------------------------------------------------------
// EntryHeader serialization
// ---------------------------------------------------------------------------

func BenchmarkEntryHeaderToBytes(b *testing.B) {
	h := EntryHeader{Length: 64, Checksum: 0xDEADBEEF, Index: 42}

	b.SetBytes(EntryHeaderSize)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = h.ToBytes()
	}
}

func BenchmarkNewEntryHeaderFromBytes(b *testing.B) {
	h := EntryHeader{Length: 64, Checksum: 0xDEADBEEF, Index: 42}
	data := h.ToBytes()

	b.SetBytes(EntryHeaderSize)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = NewEntryHeaderFromBytes(data)
	}
}
