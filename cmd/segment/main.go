package main

import (
	"fmt"
	"os"

	golog "github.com/hirebarend/go-log"
)

func main() {
	segment, err := golog.NewSegment("data/00000000000000625000.seg")

	if err != nil {
		panic(err)
	}

	fmt.Printf("%d, %d, %d\n", segment.IndexStart, segment.IndexEnd, segment.Size)

	segment.Write([]byte("Here is a string....")) // 625000
	segment.Write([]byte("Here is a string....")) // 625001
	segment.Write([]byte("Here is a string....")) // 625002
	segment.Write([]byte("Here is a string....")) // 625003

	segment.Commit()

	fmt.Printf("%d, %d, %d\n", segment.IndexStart, segment.IndexEnd, segment.Size)

	entries := segment.ReadAll()

	if len(entries) != 4 {
		panic(fmt.Errorf("expected 4, got %d", len(entries)))
	}

	if entries[len(entries)-1].Header.Index != 625003 {
		panic(fmt.Errorf("expected 625003, got %d", entries[len(entries)-1].Header.Index))
	}

	err = segment.Truncate(625002)

	if err != nil {
		panic(err)
	}

	entries = segment.ReadAll()

	if len(entries) != 3 {
		panic(fmt.Errorf("expected 3, got %d", len(entries)))
	}

	if err := os.Remove(segment.File.Name()); err != nil {
		panic(err)
	}
}
