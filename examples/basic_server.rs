//! Basic server example: how to wire ServiceKit in a tonic gRPC service.
//!
//! Run with `cargo run --example basic_server` (requires a config.yaml).

use redup_servicekit::config::MonitorServerConfig;
use redup_servicekit::http::router as metrics_router;
use redup_servicekit::logging::init_json_log;
use redup_servicekit::{Config, ServiceKit};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::net::TcpListener;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(run(PathBuf::from("config.yaml")))
}

async fn run(config_path: PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let mut cfg = Config::load(config_path)?;
    cfg.inject_os_envs();
    let monitor: MonitorServerConfig = cfg.get("MonitorServer")?.unwrap_or_default();
    init_json_log("info");

    let kit = ServiceKit::new(
        monitor,
        cfg.get_path::<u32>("service.max_workers")?.unwrap_or(1),
        cfg.get_path::<u32>("service.hpa_max_workers")?,
    );
    let ready = Arc::new(std::sync::atomic::AtomicBool::new(true));
    kit.set_ready_probe(ready.clone());

    let http_addr: SocketAddr = "0.0.0.0:9999".parse()?;
    let listener = TcpListener::bind(http_addr).await?;
    tracing::info!(%http_addr, "metrics server starting");
    axum::serve(listener, metrics_router(kit.clone()).into_make_service()).await?;
    Ok(())
}
