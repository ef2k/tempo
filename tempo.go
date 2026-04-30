package tempo

import (
	"context"
	"errors"
	"sync"
	"time"
)

type item interface{}

var (
	ErrStopped       = errors.New("tempo: dispatcher stopped")
	ErrShuttingDown  = errors.New("tempo: dispatcher shutting down")
	ErrQueueFull     = errors.New("tempo: dispatcher queue full")
	ErrNilConfig     = errors.New("tempo: nil config")
	ErrBadInterval   = errors.New("tempo: interval must be greater than zero")
	ErrBadMaxBatch   = errors.New("tempo: max batch items must be greater than zero")
	ErrBadMaxPending = errors.New("tempo: max pending items must not be negative")
)

type state int

const (
	stateRunning state = iota
	stateShuttingDown
	stateStopped
)

type shutdownRequest struct {
	ctx  context.Context
	done chan error
}

// Config configure time interval and set a batch limit.
type Config struct {
	Interval        time.Duration
	MaxBatchItems   int
	MaxPendingItems int
}

// NewDispatcher returns an initialized instance of Dispatcher.
func NewDispatcher(c *Config) (*Dispatcher, error) {
	if c == nil {
		return nil, ErrNilConfig
	}
	if c.Interval <= 0 {
		return nil, ErrBadInterval
	}
	if c.MaxBatchItems <= 0 {
		return nil, ErrBadMaxBatch
	}
	if c.MaxPendingItems < 0 {
		return nil, ErrBadMaxPending
	}

	var pendingSlots chan struct{}
	if c.MaxPendingItems > 0 {
		pendingSlots = make(chan struct{}, c.MaxPendingItems)
	}

	return &Dispatcher{
		stop:            make(chan struct{}, 1),
		shutdown:        make(chan shutdownRequest, 1),
		closing:         make(chan struct{}),
		pendingSlots:    pendingSlots,
		Q:               make(chan item),
		Batch:           make(chan []item),
		Interval:        c.Interval,
		MaxBatchItems:   c.MaxBatchItems,
		MaxPendingItems: c.MaxPendingItems,
	}, nil
}

// Dispatcher coordinates dispatching of queue items by time intervals
// or immediately after the batching limit is met.
type Dispatcher struct {
	mu              sync.RWMutex
	state           state
	stop            chan struct{}
	shutdown        chan shutdownRequest
	closing         chan struct{}
	closeOnce       sync.Once
	pendingSlots    chan struct{}
	Q               chan item
	Batch           chan []item
	Interval        time.Duration
	MaxBatchItems   int
	MaxPendingItems int
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
			d.releasePendingSlots(len(batch) + pendingItemCount(ready))
			stopTimer()
			return
		case req := <-d.shutdown:
			if shutdown != nil {
				continue
			}
			stopTimer()
			shutdown = &req
			d.setState(stateShuttingDown)
			d.closeClosing()
			if len(batch) > 0 {
				queueBatch()
			}
			if len(ready) == 0 {
				d.setState(stateStopped)
				shutdown.done <- nil
				return
			}
		case <-shutdownDone:
			d.releasePendingSlots(len(batch) + pendingItemCount(ready))
			d.setState(stateStopped)
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
			ready = ready[1:]
			d.releasePendingSlots(len(next))
			if shutdown != nil && len(ready) == 0 {
				d.setState(stateStopped)
				shutdown.done <- nil
				return
			}
		}
	}
}

// Stop stops the internal dispatch scheduler.
func (d *Dispatcher) Stop() {
	d.setState(stateStopped)
	d.closeClosing()
	select {
	case d.stop <- struct{}{}:
	default:
	}
}

// Shutdown gracefully drains buffered items or returns when the context expires.
func (d *Dispatcher) Shutdown(ctx context.Context) error {
	if err := d.enqueueStateError(); err != nil {
		return nil
	}

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

// Enqueue submits an item while the dispatcher is running.
func (d *Dispatcher) Enqueue(v any) error {
	if err := d.enqueueStateError(); err != nil {
		return err
	}
	if d.pendingSlots == nil {
		select {
		case d.Q <- v:
			return nil
		default:
		}

		if err := d.enqueueStateError(); err != nil {
			return err
		}

		d.Q <- v
		return nil
	}

	if err := d.reservePendingSlot(); err != nil {
		return err
	}

	select {
	case d.Q <- v:
		return nil
	default:
	}

	if err := d.enqueueStateError(); err != nil {
		d.releasePendingSlots(1)
		return err
	}

	select {
	case d.Q <- v:
		return nil
	case <-d.closing:
		d.releasePendingSlots(1)
		if err := d.enqueueStateError(); err != nil {
			return err
		}
		return ErrStopped
	}
}

// Batches exposes the batch output stream as a read-only channel.
func (d *Dispatcher) Batches() <-chan []item {
	return d.Batch
}

func (d *Dispatcher) enqueueStateError() error {
	d.mu.RLock()
	defer d.mu.RUnlock()

	switch d.state {
	case stateShuttingDown:
		return ErrShuttingDown
	case stateStopped:
		return ErrStopped
	default:
		return nil
	}
}

func (d *Dispatcher) setState(s state) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.state = s
}

func (d *Dispatcher) closeClosing() {
	d.closeOnce.Do(func() {
		close(d.closing)
	})
}

func (d *Dispatcher) reservePendingSlot() error {
	if d.pendingSlots == nil {
		return nil
	}
	select {
	case d.pendingSlots <- struct{}{}:
		return nil
	default:
		return ErrQueueFull
	}
}

func (d *Dispatcher) releasePendingSlots(n int) {
	if d.pendingSlots == nil {
		return
	}
	for i := 0; i < n; i++ {
		select {
		case <-d.pendingSlots:
		default:
			return
		}
	}
}

func pendingItemCount(ready [][]item) int {
	total := 0
	for _, batch := range ready {
		total += len(batch)
	}
	return total
}
