//! Axum router with `/metrics`, `/livez`, `/readyz` endpoints.
//!
//! `/metrics` refreshes the registry from storage (lazy refresh, like Python
//! `MetricServer`), then exports Prometheus text. `/livez` returns 200 OK;
//! `/readyz` returns 200 OK when the service is ready (probe), else 503.

use crate::monitor::ServiceKit;
use axum::{extract::State, http::StatusCode, response::IntoResponse, routing::get, Router};
use std::sync::Arc;

/// Axum state for the metrics router.
#[derive(Clone)]
pub struct HttpState {
    kit: Arc<ServiceKit>,
}

impl HttpState {
    pub fn new(kit: Arc<ServiceKit>) -> Self {
        Self { kit }
    }
}

/// Build the axum router serving `/metrics`, `/livez`, `/readyz`.
/// The readiness probe must be configured via [`ServiceKit::set_ready_probe`].
pub fn router(kit: Arc<ServiceKit>) -> Router {
    Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/livez", get(livez_handler))
        .route("/readyz", get(readyz_handler))
        .with_state(HttpState::new(kit))
}

async fn metrics_handler(State(state): State<HttpState>) -> impl IntoResponse {
    let body = state.kit.metrics_text();
    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4")],
        String::from_utf8_lossy(&body).to_string(),
    )
}

async fn livez_handler() -> impl IntoResponse {
    (StatusCode::OK, "OK")
}

async fn readyz_handler(State(state): State<HttpState>) -> impl IntoResponse {
    if state.kit.is_ready() {
        (StatusCode::OK, "OK")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "NOT_READY")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MonitorServerConfig, ServiceKit, TaskStatus};
    use axum::body::Body;
    use axum::http::Request;
    use http_body_util::BodyExt;
    use std::sync::atomic::AtomicBool;
    use tower::ServiceExt;

    #[tokio::test]
    async fn metrics_router_works() {
        let kit = ServiceKit::new(MonitorServerConfig::default(), 4, Some(2));
        let ready = Arc::new(AtomicBool::new(true));
        kit.set_ready_probe(ready);
        let app = router(kit.clone());

        // /livez
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/livez")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        // /readyz
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/readyz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        // not ready case
        let not_ready = Arc::new(AtomicBool::new(false));
        kit.set_ready_probe(not_ready);
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/readyz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);

        // /metrics after a task
        let tracker = kit.task_tracker();
        let id = tracker.start();
        std::thread::sleep(std::time::Duration::from_millis(10));
        tracker.finish(&id);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let text = String::from_utf8_lossy(&body);
        assert!(text.contains("started_total"));
        assert!(text.contains("completed_total"));
        assert!(text.contains("stats_tasks_with_current_time_spend_normalised_total"));
        assert!(text.contains("tasks_time_spent_quantile_bucket"));
        assert!(text.contains("service_threads"));
    }

    #[test]
    fn kit_ready_default_false() {
        let kit = ServiceKit::new(MonitorServerConfig::default(), 4, Some(2));
        assert!(!kit.is_ready());
        kit.set_task_status(TaskStatus::Ok, "default");
        assert_eq!(kit.get_statuses().len(), 1);
    }
}
