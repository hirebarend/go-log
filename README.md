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

    golog "github.com/hirebarend/go-log"
)

func main() {
    // Create a new segmented log instance.
	// - "data" is the directory where segment files will be stored.
	// - 64<<20 sets the maximum size of each segment file to 64 MB.
	//   Once a segment reaches this limit, a new segment will be created automatically.
	log := golog.NewLog[any]("data", 64<<20)

	// Load any existing segments from disk.
	// This step makes the log durable across restarts: previously written data can be read again.
	err := log.Load()
	if err != nil {
		panic(err)
	}

	// Append 10 million entries to the log.
	// Each entry is just a simple byte slice in this example, but it could be JSON,
	// protobuf, or any serialized data in a real system.
	for i := 0; i < 10_000_000; i++ {
		index, err := log.Write([]byte("Lorem ipsum dolor sit amet, consectetur adipiscing elit."))

		if err != nil {
			panic(err)
		}
	}

	// Commit flushes any buffered entries to disk and forces an fsync.
	// This guarantees that all written entries are durable, even if the process crashes afterward.
	err = log.Commit()
	if err != nil {
		panic(err)
	}

	// Read the entry at a specific index.
	// In this case, we read back the entry at position 5,750,000 to demonstrate random access.
	data, err := log.Read(5_750_000)
	if err != nil {
		panic(err)
	}
	fmt.Printf("data: %v", string(data))

	// TruncateFrom removes all entries starting from the given index onward.
	// Here we truncate from index 6,000,000, so everything after that is discarded.
	// This is useful for rolling back uncommitted or invalid entries.
	err = log.TruncateFrom(6_000_000)
	if err != nil {
		panic(err)
	}

	// TruncateTo removes all entries up to (and including) the given index.
	// Here we truncate up to index 3,000,000, effectively compacting the log
	// by dropping older entries no longer needed for recovery.
	err = log.TruncateTo(3_000_000)
	if err != nil {
		panic(err)
	}

	// Close gracefully shuts down the log by closing all segment files.
	// After this call, no further reads or writes can be performed until reopened.
	err = log.Close()
	if err != nil {
		panic(err)
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

MIT License. See [LICENSE](/LICENSE) for details.