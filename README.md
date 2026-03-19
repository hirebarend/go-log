# go-log

A lightweight, append-only log in Go. Designed for durability and high-throughput sequential writes with automatic segmentation, making it suitable as a building block for databases, message queues, or distributed systems.

## Features

- **Append-only writes** with buffered I/O (64 KB write buffer)
- **Automatic segmentation** — splits into new files when a segment exceeds the configured size
- **CRC32 checksums** on every entry for data integrity
- **Durable commits** via explicit `fsync` (`Commit`) or background flushing (`WriteCommit`)
- **Random-access reads** with in-memory offset caching for O(1) lookups
- **Truncation** — forward (`TruncateFrom`) and backward (`TruncateTo`) for rollback and compaction
- **Thread-safe** — concurrent reads and writes protected by `sync.RWMutex`
- **Zero external dependencies** — uses only the Go standard library

## Installation

```bash
go get github.com/hirebarend/go-log
```

## Usage

```go
package main

import (
    "fmt"

    golog "github.com/hirebarend/go-log"
)

func main() {
    // Create a log with 64 MB segment files stored in ./data
    log := golog.NewLog("data", 64<<20)

    // Load existing segments from disk (makes the log durable across restarts)
    if err := log.Load(); err != nil {
        panic(err)
    }

    // Append entries
    for i := 0; i < 1_000_000; i++ {
        if _, err := log.Write([]byte("hello world")); err != nil {
            panic(err)
        }
    }

    // Flush and fsync to disk
    if err := log.Commit(); err != nil {
        panic(err)
    }

    // Random-access read
    data, err := log.Read(500_000)
    if err != nil {
        panic(err)
    }
    fmt.Println(string(data))

    // Truncate entries from index 900,000 onward (rollback)
    if err := log.TruncateFrom(900_000); err != nil {
        panic(err)
    }

    // Truncate entries up to index 100,000 (compaction)
    if err := log.TruncateTo(100_000); err != nil {
        panic(err)
    }

    // Close all segment files
    if err := log.Close(); err != nil {
        panic(err)
    }
}
```

## API

| Method | Description |
|---|---|
| `NewLog(dir, maxSegmentSize)` | Create a new log instance |
| `Load()` | Load existing segments from disk |
| `Write(data)` | Append an entry (buffered) |
| `WriteCommit(data)` | Append and wait for the next background flush |
| `Commit()` | Flush the buffer and fsync to disk |
| `Read(index)` | Read an entry by index |
| `GetLastIndex()` | Return the last written index |
| `GetCommittedIndex()` | Return the last committed (fsynced) index |
| `TruncateFrom(index)` | Remove all entries from `index` onward |
| `TruncateTo(index)` | Remove all entries up to and including `index` |
| `Close()` | Flush, fsync, and close all segments |

## On-Disk Format

Each segment file is named by its starting index (e.g. `00000000000000000001.seg`) and contains a sequence of entries:

```
Entry = Header (20 bytes) + Data (variable)

Header layout (little-endian):
  [0:8]   Length    uint64    Size of data in bytes
  [8:12]  Checksum  uint32    CRC32-IEEE of data
  [12:20] Index     uint64    Logical position in the log
```

```
data/
  ├── 00000000000000000001.seg
  ├── 00000000000001875001.seg
  └── ...
```

## License

MIT License. See [LICENSE](LICENSE) for details.