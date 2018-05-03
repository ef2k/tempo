package tempo

import (
	"fmt"
	"log"
	"testing"
	"time"
)

type msg struct {
	S string
}

func TestLog(t *testing.T) {

	log.SetFlags(log.Lmicroseconds)

	// init a dispatcher
	d := NewDispatcher(&Config{
		Interval:      time.Duration(10) * time.Second,
		MaxBatchItems: 50,
	})

	// stop everything when we're done.
	defer d.Stop()

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
	go func() {
		for {
			select {
			case b := <-d.BatchCh:
				m := b.(msg)
				log.Println(m.S)
			}
		}
	}()

	done := make(chan bool)
	<-done

}
