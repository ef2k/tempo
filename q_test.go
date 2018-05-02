package tempo

import (
	"fmt"
	"testing"
	"time"
)

func TestLog(t *testing.T) {
	done := make(chan bool)

	go schedule()

	// Make lots of concurrent messages.
	go func() {
		for i := 0; i < 50; i++ {
			push(fmt.Sprintf("prod1 #%d", i))
			time.Sleep(time.Duration(100) * time.Millisecond)
		}
	}()

	go func() {
		for i := 0; i < 50; i++ {
			push(fmt.Sprintf("prod2 #%d", i))
			time.Sleep(time.Duration(100) * time.Millisecond)
		}
	}()

	time.AfterFunc(time.Duration(30)*time.Second, func() {
		for i := 0; i < 50; i++ {
			push(fmt.Sprintf("prod3 #%d", i))
			time.Sleep(time.Duration(100) * time.Millisecond)
		}
	})

	<-done
}
