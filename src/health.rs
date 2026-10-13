//! gRPC health checking integration.
//!
//! Analog of `redup_servicekit.health`: helpers around `tonic-health`.
//! `health_reporter()` creates the `(HealthReporter, HealthServer)` pair
//! exactly as used by the doaid services (see `doaid.users/src/main.rs`).

pub use tonic_health::server::health_reporter;
use tonic_health::server::HealthReporter;
pub use tonic_health::ServingStatus;

/// Mark a service as SERVING (call after a successful start).
pub async fn set_serving(reporter: &mut HealthReporter, service: impl AsRef<str>) {
    reporter
        .set_service_status(service.as_ref(), ServingStatus::Serving)
        .await;
}

/// Normalize a service name (trim leading `/`).
#[allow(unused)]
pub fn normalize_service_name(name: &str) -> String {
    name.trim_start_matches('/').to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn reporter_serve_status() {
        let (mut reporter, _server) = health_reporter();
        set_serving(&mut reporter, "doaid.users.v1.UsersService").await;
        // No public getter on HealthReporter; verifying set did not panic.
        assert_eq!(normalize_service_name("/abc"), "abc");
    }
}
