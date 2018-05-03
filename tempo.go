package tempo

import (
	"time"
)

type item interface{}

type Config struct {
	Interval      time.Duration
	MaxBatchItems int
}

func NewDispatcher(c *Config) *Dispatcher {
	return &Dispatcher{
		doWork:          make(chan bool),
		stop:            make(chan bool),
		Q:               make(chan item),
		MaxBatchItems:   c.MaxBatchItems,
		BatchCh:         make(chan []item),
		Interval:        c.Interval,
		DispatchedCount: 0,
	}
}

type Dispatcher struct {
	doWork          chan bool
	stop            chan bool
	timer           *time.Timer
	Q               chan item
	BatchCh         chan []item
	Interval        time.Duration
	MaxBatchItems   int
	DispatchedCount int
}

func (d *Dispatcher) tick() {
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
	d.BatchCh <- items
}

func (d *Dispatcher) Start() {
	d.tick()
	batch := make(chan item, d.MaxBatchItems)

	for {
		select {
		case m := <-d.Q:
			if len(batch) < cap(batch) {
				batch <- m
			} else {
				//  we have a ghost item we'll call Casper.
				d.timer.Stop()
				go func() {
					d.doWork <- true
					// send Casper to the next batch.
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
			d.dispatch(batch)
			return
		}
	}
}
func (d *Dispatcher) Stop() {
	d.stop <- true
}
