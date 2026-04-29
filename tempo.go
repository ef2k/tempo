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
		stop:            make(chan struct{}, 1),
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
	stop            chan struct{}
	Q               chan item
	Batch           chan []item
	Interval        time.Duration
	MaxBatchItems   int
	DispatchedCount int
}

// Start begins item dispatching.
func (d *Dispatcher) Start() {
	batch := make([]item, 0, d.MaxBatchItems)
	ready := make([][]item, 0)

	timer := time.NewTimer(d.Interval)
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	var timerCh <-chan time.Time

	stopTimer := func() {
		if timerCh == nil {
			return
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timerCh = nil
	}

	startTimer := func() {
		if d.Interval <= 0 {
			return
		}
		if timerCh != nil {
			stopTimer()
		}
		timer.Reset(d.Interval)
		timerCh = timer.C
	}

	queueBatch := func() {
		if len(batch) == 0 {
			return
		}
		ready = append(ready, append([]item(nil), batch...))
		batch = make([]item, 0, d.MaxBatchItems)
		stopTimer()
	}

	for {
		var out chan []item
		var next []item
		if len(ready) > 0 {
			out = d.Batch
			next = ready[0]
		}

		select {
		case <-d.stop:
			stopTimer()
			return
		case m := <-d.Q:
			if len(batch) == 0 {
				startTimer()
			}
			batch = append(batch, m)
			if len(batch) >= d.MaxBatchItems {
				queueBatch()
			}
		case <-timerCh:
			queueBatch()
		case out <- next:
			d.DispatchedCount += len(next)
			ready = ready[1:]
		}
	}
}

// Stop stops the internal dispatch scheduler.
func (d *Dispatcher) Stop() {
	select {
	case d.stop <- struct{}{}:
	default:
	}
}
