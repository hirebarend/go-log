package main

import (
	"fmt"

	golog "github.com/hirebarend/go-log"
)

func main() {
	log := golog.NewLog("data")

	log.Load()

	index, err := log.Write([]byte("Here is a string...."))

	log.Commit()

	log.Truncate(4166686)

	fmt.Printf("%v | %v", index, err)
}
