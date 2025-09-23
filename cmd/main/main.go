package main

import (
	"fmt"

	golog "github.com/hirebarend/go-log"
)

func main() {
	log := golog.NewLog("data")

	log.Load()

	index, err := log.Write([]byte("Here is a string...."))

	fmt.Printf("%v | %v", index, err)
}
