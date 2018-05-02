package tempo

import (
	"fmt"
	"time"
)

var (
	Q      = make(chan *msg)
	doWork = make(chan bool)
)

type msg struct {
	Data string `json:"d"`
}

func push(s string) {
	Q <- &msg{s}
}

func tick() {
	time.AfterFunc(time.Duration(3)*time.Second, func() {
		doWork <- true
	})
}

// work should be a blocking call.
func work(q <-chan *msg) {
	fmt.Println("===")
	for m := range q {
		fmt.Printf("\t msg: %s\n", m.Data)
	}
	fmt.Println("===")
}

func schedule() {
	tick()
	q := make(chan *msg, 100)

	for {
		select {
		case m := <-Q:
			q <- m
		case do := <-doWork:
			if do {
				if len(q) <= 0 {
					tick()
					continue
				}
				close(q)
				work(q)
				q = make(chan *msg, 100)
				tick()
			}
		}
	}
}
