//! Basic client usage example for the servicekit utilities.
//!
//! Demonstrates loading config, initializing logging and using a ServiceKit
//! with custom stats. There is no dedicated gRPC client wrapper in the Rust
//! crate (services use generated tonic clients directly).

use redup_servicekit::config::MonitorServerConfig;
use redup_servicekit::logging::init_json_log;
use redup_servicekit::{Config, ServiceKit};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_json_log("info");
    let mut cfg = Config::new();
    cfg.inject_os_envs();
    let monitor: MonitorServerConfig = cfg.get("MonitorServer")?.unwrap_or_default();

    let kit = ServiceKit::new(monitor, 4, Some(2));
    let tracker = kit.task_tracker();
    let id = tracker.start();
    std::thread::sleep(std::time::Duration::from_millis(10));
    tracker.finish(&id);
    kit.inc_stat("request___method__ExampleCall", 1.0);
    kit.observe_stat("time full___method__ExampleCall", 0.01);

    let bytes = kit.metrics_text();
    let text = String::from_utf8_lossy(&bytes);
    println!("{text}");
    Ok(())
}
