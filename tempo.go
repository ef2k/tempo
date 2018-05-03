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
		Q:      make(chan item),
		doWork: make(chan bool),

		MaxBatchItems: c.MaxBatchItems,
		BatchCh:       make(chan item, c.MaxBatchItems),
		Interval:      c.Interval,
	}
}

type Dispatcher struct {
	Q             chan item
	doWork        chan bool
	BatchCh       chan item
	Interval      time.Duration
	MaxBatchItems int
	timer         *time.Timer
}

func (d *Dispatcher) tick() {
	d.timer = time.AfterFunc(d.Interval, func() {
		d.doWork <- true
	})
}

func (d *Dispatcher) dispatch(batch chan item) {
	for b := range batch {
		d.BatchCh <- b
	}
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
				d.timer.Stop()
				go func() {
					d.doWork <- true
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
		}

	}
}
