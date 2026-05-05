# Performance

This directory is for Tempo's performance harness.

The results here are machine-relative. A faster machine will produce higher
throughput than a slower one. What matters most is:

- what this machine can do
- what settings are sane for this machine
- whether Tempo still behaves correctly under pressure

The main commands are:

- `make tune`
- `make soak`
- `make bench`

## `make tune`

Use this first on a new machine.

`make tune` asks for an approximate payload size, probes the machine, and
recommends sane starting values for:

- `MaxBufferedBytes`
- optional `MaxBatchBytes`

It also gives rough capacity information, such as observed throughput and
buffer headroom for that payload size.

The generated [settings.json](/Users/e/work/tempo/performance/settings.json:1)
is meant to be local machine calibration state.

## `make soak`

`make soak` is the pressure and recovery test.

Its job is to create sustained backpressure (`P > C`) and prove that Tempo:

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

It is not yet a historical regression system by itself, because it is not
currently tied to saved benchmark baselines.

## How To Think About Them

- `tune` answers: what should I configure on this machine?
- `soak` answers: does Tempo still behave correctly under sustained pressure?
- `bench` answers: what kind of throughput numbers are we seeing on this machine?
