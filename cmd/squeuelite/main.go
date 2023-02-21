package main

import (
	"log"
	"time"

	"github.com/nfoerster/squeuelite"
)

// func updateFunc(mode int, database, table string, rowid int64) {
// 	log.Printf("%v %v %v %v updated", mode, database, table, rowid)
// }

func main() {
	queue, err := squeuelite.NewSQueueLite("test", true, 100)
	if err != nil {
		log.Fatal(err)
	}

	go func(queue *squeuelite.SQueueLite) {
		time.Sleep(10 * time.Second)

		_, err := queue.Put("blahhh")
		if err != nil {
			log.Fatal(err)
		}
	}(queue)

	msg, err := queue.PeekBlock()
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("%+v", msg)

}
