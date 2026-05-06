# Performance

This directory is for Tempo's performance suite.

The results here are machine-relative. A faster machine will produce higher
throughput than a slower one. What matters most is:

- what this machine can do
- what settings are sane for this machine
- whether correctness and recovery hold under pressure

The main commands are:

- `make tune`
  recommends calibrated settings for the current machine
- `make soak`
  ensures correctness and recovery under heavy backpressure
- `make bench`
  reports local throughput and allocation numbers

## `make tune`

Use this first on a new machine.

`make tune` asks for an approximate payload size, probes the machine, and
recommends sane starting values for:

- `MaxBufferedBytes`
- optional `MaxBatchBytes`

These bounds are the main controls under uneven load:

- `MaxBufferedBytes` is the hard safety boundary
- `Interval` is the flush guarantee
- `MaxBatchBytes` is optional shaping for emitted work

It also gives rough capacity information, such as observed throughput and
buffer headroom for that payload size.

The generated [settings.json](/Users/e/work/tempo/performance/settings.json:1)
stores the tuned defaults for this machine.

Those settings are then used by the soak test to run with the recommended byte
bounds while creating intentional backpressure. The goal is to show how much
load can be sustained for payloads of roughly that size on your machine and to
leave you with a practical starting point.

## `make soak`

`make soak` is the pressure and recovery test.

Its job is to create sustained backpressure (`P > C`) and prove that it:

- stays live under pressure
- delivers all accepted items
- drains and shuts down cleanly
- keeps memory behavior understandable while under load

This is not the tuner. It is the behavioral proof that the chosen settings
still make sense when the system is under stress.

The soak uses a telemetry-shaped payload mix so the pressure is closer to a
real event stream than a tiny synthetic message loop.

## `make bench`

`make bench` is the raw throughput snapshot.

Its job is to show how fast Tempo can go on the current machine under stable
synthetic benchmark conditions.

Right now it is mainly useful as local engineering information:

- rough items/sec
- rough bytes/sec
- allocations
