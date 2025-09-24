package main

import (
	golog "github.com/hirebarend/go-log"
)

func main() {
	log := golog.NewLog("data")

	log.Load()

	// for i := 0; i < 5_000_000; i++ {
	// 	_, err := log.Write([]byte("Here is a string...."))

	// 	if err != nil {
	// 		panic(err)
	// 	}
	// }

	// log.Commit()

	// segment := log.Segments[4]

	// fmt.Printf("%d -> %d\n", segment.IndexStart, segment.IndexEnd)

	// index, _ := log.Write([]byte("Here is a string...."))

	// log.Commit()

	// fmt.Printf("%d\n", index)

	log.TruncateTo(2500101)
}
