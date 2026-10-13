//! Toolkit for Rust microservices: YAML configuration, JSON logging,
//! Prometheus metrics and gRPC utilities.
//!
//! The metrics contract is semantically compatible with
//! `redup.python.servicekit` so dashboards, PromQL expressions and HPA
//! PodMetric queries stay unchanged.
//!
//! Main entry point is [`monitor::ServiceKit`]:
//!
//! ```no_run
//! use redup_servicekit::{Config, MonitorServerConfig, ServiceKit};
//! use std::path::Path;
//!
//! let mut cfg = Config::load(Path::new("/config/config.yaml")).unwrap();
//! cfg.inject_os_envs();
//! let monitor: MonitorServerConfig =
//!     cfg.get("MonitorServer").ok().flatten().unwrap_or_default();
//! let kit = ServiceKit::new(
//!     monitor,
//!     cfg.get_path::<u32>("service.max_workers").ok().flatten().unwrap_or(1),
//!     cfg.get_path::<u32>("service.hpa_max_workers").ok().flatten(),
//! );
//! ```

pub mod config;
pub mod grpc;
pub mod health;
pub mod http;
pub mod logging;
pub mod metrics;
pub mod monitor;
pub mod reflection;

pub use config::{Config, MonitorServerConfig, ServiceInfoEntry};
pub use monitor::{ReadyProbe, ServiceKit, TaskStatus, TaskTracker};

/// Current crate version (synced with Cargo.toml / VERSION).
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
