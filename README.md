# redup-servicekit

[![Rust Test status](https://github.com/redup-ai/redup.rust.servicekit/actions/workflows/rust-ci.yml/badge.svg?branch=master)](https://github.com/redup-ai/redup.rust.servicekit/actions/workflows/rust-ci.yml)
[![crates.io version](https://img.shields.io/crates/v/redup-servicekit)](https://crates.io/crates/redup-servicekit)

Rust toolkit for building production-ready microservices with:

- centralized YAML configuration and environment overrides,
- structured JSON logging,
- Prometheus metrics and monitoring helpers,
- gRPC health checks and a metrics layer for tonic services.

The metrics contract (names, labels, buckets, EMA semantics) is compatible
with `redup.python.servicekit`, so existing dashboards, PromQL expressions and
HPA `PodMetric` queries keep working unchanged.

## Features

- **Configuration**: `Config` for loading and merging YAML configs, with
  environment variable overrides (`___` nesting, `__` for spaces in keys).
- **Logging**: JSON console logging via `tracing-subscriber`.
- **Monitoring & Metrics**: Prometheus integration with counters, gauges and
  histograms for tasks and service stats (lazy refresh on `/metrics`).
- **gRPC Utilities**: health checks and a `tower` layer for collecting
  per-method metrics on tonic servers.

## Installation

```bash
cargo add redup-servicekit
```

## Quick start

```rust,ignore
use redup_servicekit::{Config, MonitorServerConfig, ServiceKit};
use std::path::Path;

let mut cfg = Config::load(Path::new("/config/config.yaml"))?;
cfg.inject_os_envs();
let monitor: MonitorServerConfig = cfg.get("MonitorServer")?.unwrap_or_default();
let kit = ServiceKit::new(
    monitor,
    cfg.get_path::<u32>("service.max_workers")?.unwrap_or(1),
    cfg.get_path::<u32>("service.hpa_max_workers")?,
);
```

Serving `/metrics`, `/livez`, `/readyz`:

```rust,ignore
use redup_servicekit::http::router;

let listener = tokio::net::TcpListener::bind("0.0.0.0:9999").await?;
axum::serve(listener, router(kit).into_make_service()).await?;
```

See `examples/basic_server.rs` and `examples/basic_client.rs`.

## License

Licensed under the MIT License. See the `LICENSE` file for details.