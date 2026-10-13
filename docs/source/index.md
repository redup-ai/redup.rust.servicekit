# redup-servicekit

[Rust](https://www.rust-lang.org/) toolkit for building production-ready
microservices with centralized YAML configuration, structured JSON logging,
Prometheus metrics and gRPC utilities.

The metrics contract is compatible with `redup.python.servicekit` — the same
metric names, labels, histogram buckets and EMA semantics, so dashboards,
PromQL expressions and HPA `PodMetric` queries work without changes.

## Topics

- [Getting started](getting_started.md)
- [Configuration](config.md)
- [Metrics](metrics.md)
- [gRPC layer](grpc.md)
- [HPA integration](hpa.md)