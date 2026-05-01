package tempo

import (
	"context"
	"errors"
	"sync"
	"time"
)

var (
	ErrStopped          = errors.New("tempo: dispatcher stopped")
	ErrShuttingDown     = errors.New("tempo: dispatcher shutting down")
	ErrQueueFull        = errors.New("tempo: dispatcher queue full")
	ErrNilConfig        = errors.New("tempo: nil config")
	ErrBadInterval      = errors.New("tempo: interval must be greater than zero")
	ErrBadMaxBatchBytes = errors.New("tempo: max batch bytes must be greater than zero")
	ErrBadMaxPending    = errors.New("tempo: max pending bytes must be greater than zero")
	ErrPayloadTooLarge  = errors.New("tempo: payload exceeds configured byte limit")
)

const (
	KiB int64 = 1024
	MiB       = 1024 * KiB
	GiB       = 1024 * MiB
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

type queuedBatch struct {
	items [][]byte
	size  int64
}

// Config configures interval-based flushing and byte-oriented queue limits.
type Config struct {
	Interval        time.Duration
	MaxBatchBytes   int64
	MaxPendingBytes int64
}

// NewDispatcher returns an initialized instance of Dispatcher.
func NewDispatcher(c *Config) (*Dispatcher, error) {
	if c == nil {
		return nil, ErrNilConfig
	}
	if c.Interval <= 0 {
		return nil, ErrBadInterval
	}
	if c.MaxBatchBytes <= 0 {
		return nil, ErrBadMaxBatchBytes
	}
	if c.MaxPendingBytes <= 0 {
		return nil, ErrBadMaxPending
	}

	return &Dispatcher{
		stop:            make(chan struct{}, 1),
		shutdown:        make(chan shutdownRequest, 1),
		closing:         make(chan struct{}),
		Q:               make(chan []byte),
		Batch:           make(chan [][]byte),
		Interval:        c.Interval,
		MaxBatchBytes:   c.MaxBatchBytes,
		MaxPendingBytes: c.MaxPendingBytes,
	}, nil
}

// Dispatcher coordinates dispatching of queued payloads by time interval
// or when the batch byte limit is met.
type Dispatcher struct {
	mu              sync.RWMutex
	admitMu         sync.Mutex
	state           state
	stop            chan struct{}
	shutdown        chan shutdownRequest
	closing         chan struct{}
	closeOnce       sync.Once
	pendingBytes    int64
	Q               chan []byte
	Batch           chan [][]byte
	Interval        time.Duration
	MaxBatchBytes   int64
	MaxPendingBytes int64
}

// Start begins payload dispatching.
func (d *Dispatcher) Start() {
	batch := make([][]byte, 0)
	var batchBytes int64
	ready := make([]queuedBatch, 0)
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
		ready = append(ready, queuedBatch{
			items: append([][]byte(nil), batch...),
			size:  batchBytes,
		})
		batch = make([][]byte, 0)
		batchBytes = 0
		stopTimer()
	}

	releaseQueuedBytes := func() {
		total := batchBytes + pendingBatchBytes(ready)
		d.releasePendingBytes(total)
	}

	for {
		if shutdown != nil && len(batch) > 0 {
			queueBatch()
		}

		var out chan [][]byte
		var next queuedBatch
		if len(ready) > 0 {
			out = d.Batch
			next = ready[0]
		}

		var in chan []byte
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
			releaseQueuedBytes()
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
			releaseQueuedBytes()
			d.setState(stateStopped)
			shutdown.done <- shutdown.ctx.Err()
			return
		case m := <-in:
			size := int64(len(m))
			if len(batch) > 0 && batchBytes+size > d.MaxBatchBytes {
				queueBatch()
			}
			if len(batch) == 0 {
				startTimer()
			}
			batch = append(batch, m)
			batchBytes += size
			if batchBytes >= d.MaxBatchBytes {
				queueBatch()
			}
		case <-timerTick:
			queueBatch()
		case out <- next.items:
			ready = ready[1:]
			d.releasePendingBytes(next.size)
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

// Enqueue submits a payload while the dispatcher is running.
func (d *Dispatcher) Enqueue(v []byte) error {
	if err := d.enqueueStateError(); err != nil {
		return err
	}

	size := int64(len(v))
	if size > d.MaxBatchBytes || size > d.MaxPendingBytes {
		return ErrPayloadTooLarge
	}

	if err := d.reservePendingBytes(size); err != nil {
		return err
	}

	payload := append([]byte(nil), v...)

	select {
	case d.Q <- payload:
		return nil
	default:
	}

	if err := d.enqueueStateError(); err != nil {
		d.releasePendingBytes(size)
		return err
	}

	select {
	case d.Q <- payload:
		return nil
	case <-d.closing:
		d.releasePendingBytes(size)
		if err := d.enqueueStateError(); err != nil {
			return err
		}
		return ErrStopped
	}
}

// Batches exposes the batch output stream as a read-only channel.
func (d *Dispatcher) Batches() <-chan [][]byte {
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

func (d *Dispatcher) reservePendingBytes(n int64) error {
	d.admitMu.Lock()
	defer d.admitMu.Unlock()

	if d.pendingBytes+n > d.MaxPendingBytes {
		return ErrQueueFull
	}
	d.pendingBytes += n
	return nil
}

func (d *Dispatcher) releasePendingBytes(n int64) {
	if n <= 0 {
		return
	}

	d.admitMu.Lock()
	defer d.admitMu.Unlock()

	d.pendingBytes -= n
	if d.pendingBytes < 0 {
		d.pendingBytes = 0
	}
}

func pendingBatchBytes(ready []queuedBatch) int64 {
	var total int64
	for _, batch := range ready {
		total += batch.size
	}
	return total
}
