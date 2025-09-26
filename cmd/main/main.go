package main

import (
	"fmt"

	golog "github.com/hirebarend/go-log"
)

func main() {
	log := golog.NewLog("data", 64<<20)

	log.Load()

	for i := 0; i < 10_000_000; i++ {
		_, err := log.Write([]byte("Lorem ipsum dolor sit amet, consectetur adipiscing elit."))

		if err != nil {
			panic(err)
		}
	}

	committedIndex, err := log.GetCommittedIndex()

	if err != nil {
		panic(err)
	}

	fmt.Printf("IndexCommited: %d\n", committedIndex)

	log.Commit()

	committedIndex, err = log.GetCommittedIndex()

	if err != nil {
		panic(err)
	}

	fmt.Printf("IndexCommited: %d\n", committedIndex)
}
