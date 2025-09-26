# go-log

`go-log` is a lightweight, append-only log implementation in Go. It is designed for durability and efficient sequential writes, with automatic segmentation and support for truncation. This makes it useful as a building block for databases, message queues, or distributed systems that need a simple, persistent log.

## Installation

```bash
go get github.com/hirebarend/go-log
```

## Usage

```go
package main

import (
    "fmt"
    "log"

    "github.com/<your-username>/golog"
)

func main() {
    // Initialize a new log with a max segment size of 1MB
    l := golog.NewLog("./data", 1<<20)

    // Load segments from disk
    if err := l.Load(); err != nil {
        log.Fatal(err)
    }

    // Write some data
    index, err := l.Write([]byte("hello world"))
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println("Wrote entry at index:", index)

    // Commit the latest segment to disk
    if err := l.Commit(); err != nil {
        log.Fatal(err)
    }

    // Truncate all entries starting from index 10
    if err := l.TruncateFrom(10); err != nil {
        log.Fatal(err)
    }
}
```

## Log Structure

Each log is stored in a directory as a set of segment files:

```
data/
  ├── 00000000000000000001.seg
  ├── 00000000000001875001.seg
  └── ...
```

- Segments are named by their starting index.
- Entries inside segments are sequential and contain a header and data.

## License

MIT License. See LICENSE for details.