# tempo

Tempo batches high-volume events in memory so downstream systems can handle them in manageable batches instead of one at a time.

It runs in-process as part of an application, covering the cases where a
dispatcher is enough and a heavier system like Kafka or RabbitMQ would be more
infrastructure than the job really needs.

**Useful for**

Tempo is useful anywhere events arrive faster than downstream work should be
performed.

- telemetry and analytics pipelines that want to batch writes before handing
  them to storage
- API clients that benefit from sending grouped work instead of one request per
  event
- logging, tracing, and agent-style workloads that produce bursts inside a
  single process
- applications that need backpressure and bounded memory without introducing a
  separate queueing system

**Narrow by design**

I built this package for my own telemetry needs, and that shapes its scope.
Tempo is not a broker or a distributed queue, and it is not meant to grow into
one. It is a narrow dispatcher for batching events well.

**Features**

- bounded in-memory buffering via `MaxBufferedBytes`
- timed flushes via `Interval`
- optional batch-size shaping via `MaxBatchBytes`
- graceful draining with `Shutdown`
- explicit failures with `ErrQueueFull` and `ErrPayloadTooLarge`

**Reliability**

Tempo is a bounded in-memory dispatcher. When the queue is full, `Enqueue`
returns `ErrQueueFull` and the item is not accepted. Tempo guarantees clean
drain for accepted items, but it does not provide durable storage or automatic
replay for rejected ones. If your workload cannot tolerate loss under
overload, handle `ErrQueueFull` in the caller and route those records through a
durable fallback.

**Performance**

Tempo is meant to stay in-process and close to application code. As a realistic
lower bound, I ran Tempo on an 8GB Raspberry Pi 4 Model B for a 5-minute soak
with `~10KiB` payloads and intentional backpressure. It sustained about
`244k delivered events/sec`, delivered `73.1M` events with zero rejections,
peaked at roughly `3.5 MiB` of heap allocation, and drained in `118µs`.

Of course, this doesn't prove an entire telemetry pipeline can run on a
Raspberry Pi, but it does show that the queueing and batching layer is unlikely to be a bottleneck in real deployments.

The repo also includes a correctness-focused test suite and machine-relative performance suite for tuning, soak testing, and benchmarking.

You can learn more about included soaks and benchmarks in [performance/README.md](./performance/README.md).

## Install

```sh
go get github.com/ef2k/tempo
```

## Documentation

[pkg.go.dev/github.com/ef2k/tempo](https://pkg.go.dev/github.com/ef2k/tempo)
