//! Axum router with `/metrics`, `/livez`, `/readyz` endpoints.
//!
//! `/metrics` refreshes the registry from storage (lazy refresh, like Python
//! `MetricServer`), then exports Prometheus text. `/livez` returns 200 OK;
//! `/readyz` returns 200 OK when the service is ready (probe), else 503.

use crate::monitor::ServiceKit;
use axum::{extract::State, http::StatusCode, response::IntoResponse, routing::get, Router};
use http::{Request, Response};
use http_body::Body as HttpBody;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tower::{Layer, Service};

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

/// Tonic-gRPC compatible metrics layer for plain HTTP services (axum/axum).
///
/// Analog of [`crate::grpc::GrpcMetricsLayer`] for REST handlers
/// (`redup.python.servicekit` `request_guard`): per request it increments
/// `request___method__<m>`, registers a task, measures duration and
/// request/response sizes, and on response increments
/// `processed___method__<m>___status__success|failure`, on failure also
/// `errors___method__<m>___type__other`, observes `time full`,
/// `request_size`, `response_size` (EMA + `stats_hist_*`).
///
/// Method name is the last path segment (`/api/v1/billing/summary` ->
/// `summary`); the full route is available via
/// [`http_method_name`]. Sizes come from `size_hint().exact()`.
#[derive(Clone)]
pub struct HttpMetricsLayer {
    kit: Arc<ServiceKit>,
}

impl HttpMetricsLayer {
    pub fn new(kit: Arc<ServiceKit>) -> Self {
        Self { kit }
    }
}

impl<S> Layer<S> for HttpMetricsLayer {
    type Service = HttpMetricsService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        HttpMetricsService {
            inner,
            kit: self.kit.clone(),
        }
    }
}

/// Service wrapper produced by [`HttpMetricsLayer`].
#[derive(Clone)]
pub struct HttpMetricsService<S> {
    inner: S,
    kit: Arc<ServiceKit>,
}

impl<S, B> Service<Request<B>> for HttpMetricsService<S>
where
    S: Service<Request<B>, Response = Response<B>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    B: HttpBody + Send + 'static,
{
    type Response = Response<B>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<B>) -> Self::Future {
        let method = http_method_name(req.uri().path());
        let kit = self.kit.clone();
        let request_size = req.body().size_hint().exact().unwrap_or(0) as f64;
        let mut inner = self.inner.clone();

        let start = std::time::Instant::now();
        kit.init_method(&method);
        kit.inc_stat(&format!("request___method__{method}"), 1.0);
        let task_id = kit.task_tracker().start();

        let future = async move {
            let result = inner.call(req).await;
            let elapsed = start.elapsed().as_secs_f64();
            kit.task_tracker().finish(&task_id);

            match result {
                Ok(response) => {
                    let failed = response.status().as_u16() >= 400;
                    let response_size = response.body().size_hint().exact().unwrap_or(0) as f64;
                    let status = if failed { "failure" } else { "success" };
                    kit.inc_stat(
                        &format!("processed___method__{method}___status__{status}"),
                        1.0,
                    );
                    if failed {
                        kit.inc_stat(&format!("errors___method__{method}___type__other"), 1.0);
                    }
                    kit.observe_stat(&format!("time full___method__{method}"), elapsed);
                    kit.observe_stat(&format!("request_size___method__{method}"), request_size);
                    kit.observe_stat(&format!("response_size___method__{method}"), response_size);
                    kit.set_task_status(
                        if failed {
                            crate::monitor::TaskStatus::OtherException
                        } else {
                            crate::monitor::TaskStatus::Ok
                        },
                        &method,
                    );
                    Ok(response)
                }
                Err(error) => {
                    kit.inc_stat(
                        &format!("processed___method__{method}___status__failure"),
                        1.0,
                    );
                    kit.inc_stat(&format!("errors___method__{method}___type__other"), 1.0);
                    kit.observe_stat(&format!("time full___method__{method}"), elapsed);
                    kit.observe_stat(&format!("request_size___method__{method}"), request_size);
                    kit.set_task_status(crate::monitor::TaskStatus::OtherException, &method);
                    Err(error)
                }
            }
        };
        Box::pin(future)
    }
}

/// Derive HTTP method name from the URI path (last path segment).
/// `/api/v1/billing/summary` -> `summary`, `/health` -> `health`.
pub fn http_method_name(path: &str) -> String {
    let trimmed = path.trim_end_matches('/');
    trimmed
        .rsplit('/')
        .find(|s| !s.is_empty())
        .unwrap_or("root")
        .to_string()
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

    #[test]
    fn http_method_name_cases() {
        assert_eq!(http_method_name("/api/v1/billing/summary"), "summary");
        assert_eq!(http_method_name("/health"), "health");
        assert_eq!(http_method_name("/"), "root");
        assert_eq!(http_method_name("/ready/"), "ready");
    }

    #[tokio::test]
    async fn http_metrics_layer_records_success_and_failure() {
        let kit = ServiceKit::new(MonitorServerConfig::default(), 4, Some(2));
        let ready = Arc::new(AtomicBool::new(true));
        kit.set_ready_probe(ready);
        let app = router(kit.clone()).layer(HttpMetricsLayer::new(kit.clone()));

        // success: /livez
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

        // failure: unmatched route -> 404
        let resp = app
            .clone()
            .oneshot(Request::builder().uri("/boom").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);

        let text = String::from_utf8_lossy(&kit.metrics_text()).to_string();
        assert!(text.contains("stats_request"));
        assert!(text.contains("method=\"livez\""));
        assert!(text.contains("stats_processed{"));
        assert!(text.contains("method=\"boom\""));
        assert!(text.contains("status=\"failure\""));
        assert!(text.contains("stats_errors{"));
        assert!(text.contains("stats_hist_time_full_bucket"));
        assert!(text.contains("stats_hist_request_size_bucket"));
        assert!(text.contains("stats_hist_response_size_bucket"));
        assert!(text.contains("tasks_count"));
    }
}
