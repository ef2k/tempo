package tempo

import (
	"context"
	"time"
)

type item interface{}

type shutdownRequest struct {
	ctx  context.Context
	done chan error
}

// Config configure time interval and set a batch limit.
type Config struct {
	Interval      time.Duration
	MaxBatchItems int
}

// NewDispatcher returns an initialized instance of Dispatcher.
func NewDispatcher(c *Config) *Dispatcher {
	return &Dispatcher{
		stop:            make(chan struct{}, 1),
		shutdown:        make(chan shutdownRequest, 1),
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
	shutdown        chan shutdownRequest
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
	var shutdown *shutdownRequest

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
		if shutdown != nil && len(batch) > 0 {
			queueBatch()
		}

		var out chan []item
		var next []item
		if len(ready) > 0 {
			out = d.Batch
			next = ready[0]
		}

		var in chan item
		var timerTick <-chan time.Time
		if shutdown == nil {
			in = d.Q
			timerTick = timerCh
		}

		var shutdownDone <-chan struct{}
		if shutdown != nil {
			shutdownDone = shutdown.ctx.Done()
		}

		select {
		case <-d.stop:
			stopTimer()
			return
		case req := <-d.shutdown:
			if shutdown != nil {
				continue
			}
			stopTimer()
			shutdown = &req
			if len(batch) > 0 {
				queueBatch()
			}
			if len(ready) == 0 {
				shutdown.done <- nil
				return
			}
		case <-shutdownDone:
			shutdown.done <- shutdown.ctx.Err()
			return
		case m := <-in:
			if len(batch) == 0 {
				startTimer()
			}
			batch = append(batch, m)
			if len(batch) >= d.MaxBatchItems {
				queueBatch()
			}
		case <-timerTick:
			queueBatch()
		case out <- next:
			d.DispatchedCount += len(next)
			ready = ready[1:]
			if shutdown != nil && len(ready) == 0 {
				shutdown.done <- nil
				return
			}
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

// Shutdown gracefully drains buffered items or returns when the context expires.
func (d *Dispatcher) Shutdown(ctx context.Context) error {
	req := shutdownRequest{
		ctx:  ctx,
		done: make(chan error, 1),
	}

	select {
	case d.shutdown <- req:
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case err := <-req.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
