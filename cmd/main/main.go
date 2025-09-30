package main

import (
	"fmt"

	golog "github.com/hirebarend/go-log"
)

func main() {
	// Create a new segmented log instance.
	// - "data" is the directory where segment files will be stored.
	// - 64<<20 sets the maximum size of each segment file to 64 MB.
	//   Once a segment reaches this limit, a new segment will be created automatically.
	log := golog.NewLog("data", 64<<20)

	// Load any existing log segments from disk.
	// This step makes the log durable across restarts: previously written data can be read again.
	err := log.Load()
	if err != nil {
		panic(err)
	}

	// Append 10 million entries to the log.
	// Each entry is just a simple byte slice in this example, but it could be JSON,
	// protobuf, or any serialized data in a real system.
	for i := 0; i < 10_000_000; i++ {
		_, err := log.Write([]byte("Lorem ipsum dolor sit amet, consectetur adipiscing elit."))
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

	// Read the entry at a specific log index.
	// In this case, we read back the entry at position 5,750,000 to demonstrate random access.
	data, err := log.Read(5_750_000)
	if err != nil {
		panic(err)
	}
	fmt.Printf("data: %v", string(data))

	// TruncateFrom removes all log entries starting from the given index onward.
	// Here we truncate from index 6,000,000, so everything after that is discarded.
	// This is useful for rolling back uncommitted or invalid entries.
	err = log.TruncateFrom(6_000_000)
	if err != nil {
		panic(err)
	}

	// TruncateTo removes all log entries up to (and including) the given index.
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
