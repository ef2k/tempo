package tempo

import (
	"time"
)

type item interface{}

// Config configure time interval and set a batch limit.
type Config struct {
	Interval      time.Duration
	MaxBatchItems int
}

// NewDispatcher returns an initialized instance of Dispatcher.
func NewDispatcher(c *Config) *Dispatcher {
	return &Dispatcher{
		doWork:          make(chan bool),
		stop:            make(chan bool),
		Q:               make(chan item),
		Batch:           make(chan []item),
		Interval:        c.Interval,
		MaxBatchItems:   c.MaxBatchItems,
		DispatchedCount: 0,
	}
}

// Dispatcher coordinates dispatching of queue items by time intervals
// or immediately after the batching limit is met.
type Dispatcher struct {
	doWork          chan bool
	stop            chan bool
	timer           *time.Timer
	Q               chan item
	Batch           chan []item
	Interval        time.Duration
	MaxBatchItems   int
	DispatchedCount int
}

func (d *Dispatcher) tick() {
	if d.timer != nil {
		d.timer.Reset(d.Interval)
		return
	}
	d.timer = time.AfterFunc(d.Interval, func() {
		d.doWork <- true
	})
}

func (d *Dispatcher) dispatch(batch chan item) {
	var items []item
	for b := range batch {
		items = append(items, b)
	}
	d.DispatchedCount += len(items)
	d.Batch <- items
}

// Start begins item dispatching.
func (d *Dispatcher) Start() {
	d.tick()
	batch := make(chan item, d.MaxBatchItems)

	for {
		select {
		case m := <-d.Q:
			if len(batch) < cap(batch) {
				batch <- m
			} else {
				// NOTE at this point, there's no space in the
				// batch and we have an item pending enqueue.
				d.timer.Stop()
				go func() {
					d.doWork <- true
					// enqueue into the next batch.
					d.Q <- m
				}()
			}
		case doWork := <-d.doWork:
			if !doWork {
				continue
			}
			if len(batch) <= 0 {
				d.tick()
				continue
			}
			close(batch)
			d.dispatch(batch)
			batch = make(chan item, d.MaxBatchItems)
			d.tick()
		case <-d.stop:
			d.timer.Stop()
			d.DispatchedCount = 0
			return
		}
	}
}

// Stop stops the internal dispatch scheduler.
func (d *Dispatcher) Stop() {
	d.timer.Stop()
	d.stop <- true
}
