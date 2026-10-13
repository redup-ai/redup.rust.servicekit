# Getting started

Add the dependency to your service:

```bash
cargo add redup-servicekit
```

Create a `ServiceKit` from config and serve the metrics endpoints:

```rust,ignore
use redup_servicekit::config::MonitorServerConfig;
use redup_servicekit::http::router;
use redup_servicekit::{Config, ServiceKit};

let mut cfg = Config::load("/config/config.yaml")?;
cfg.inject_os_envs();
let monitor: MonitorServerConfig = cfg.get("MonitorServer")?.unwrap_or_default();
let kit = ServiceKit::new(
    monitor,
    cfg.get_path::<u32>("service.max_workers")?.unwrap_or(1),
    cfg.get_path::<u32>("service.hpa_max_workers")?,
);
let ready = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
kit.set_ready_probe(ready);

let listener = tokio::net::TcpListener::bind("0.0.0.0:9999").await?;
axum::serve(listener, router(kit).into_make_service()).await?;
```

The router serves `/metrics`, `/livez` (200 OK) and `/readyz` (200 OK when the
probe is ready, 503 otherwise).