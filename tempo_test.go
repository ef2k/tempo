package tempo

import (
	"fmt"
	"log"
	"testing"
	"time"
)

type msg struct {
	s string
}

func TestLog(t *testing.T) {
	done := make(chan bool)

	// init a dispatcher
	d := NewDispatcher(&Config{
		Interval:      time.Duration(10) * time.Second,
		MaxBatchItems: 50,
	})

	// start it up in its own goroutine.
	go d.Start()

	// produce lots of messages...

	go func() {
		for i := 0; i < 50; i++ {
			m := msg{fmt.Sprintf("producer1/ message #%d", i)}
			d.Q <- m
			time.Sleep(time.Duration(100) * time.Millisecond)
		}
	}()
	go func() {
		for i := 0; i < 50; i++ {
			m := msg{fmt.Sprintf("producer2/ message #%d", i)}
			d.Q <- m
			time.Sleep(time.Duration(100) * time.Millisecond)
		}
	}()

	// get the batch and print them out.
	for {
		select {
		case b := <-d.BatchCh:
			log.Println(b)
		}
	}

	// 	d := NewDispatcher(&Config{
	// 		Interval:      time.Duration(5000) * time.Millisecond,
	// 		MaxBatchItems: 10,
	// 	})
	// 	go d.Start()

	// 	// Make lots of concurrent messages.
	// 	go func() {
	// 		for i := 0; i < 100; i++ {
	// 			m := fmt.Sprintf("prod1 #%d", i)
	// 			d.Q <- m
	// 			time.Sleep(time.Duration(100) * time.Millisecond)
	// 		}
	// 	}()

	// 	for {
	// 		select {
	// 		case b := <-d.BatchCh:
	// 			log.Println(b)
	// 		}
	// 	}

	<-done
}

// go func() {
// 	for i := 0; i < 50; i++ {
// 		m := fmt.Sprintf("prod2 #%d", i)
// 		d.Q <- m
// 		time.Sleep(time.Duration(100) * time.Millisecond)
// 	}
// }()

// time.AfterFunc(time.Duration(30)*time.Second, func() {
// 	for i := 0; i < 50; i++ {
// 		m := fmt.Sprintf("prod3 #%d", i)
// 		d.Q <- m
// 		time.Sleep(time.Duration(100) * time.Millisecond)
// 	}
// })
