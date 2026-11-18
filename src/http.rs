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

/// RAII guard for the in-flight HTTP task metric.
///
/// `HttpMetricsService::call` must finish the tracked task exactly once per
/// request. A regular completion calls [`finish`](Self::finish) (records the
/// duration); if the handler future is dropped mid-flight — client gone,
/// proxy reset, timeout at the edge — the guard's `Drop` discards the task
/// so `tasks_count` / `current_tasks_max_time_spend` cannot grow forever.
struct TaskGuard {
    kit: Arc<ServiceKit>,
    id: Option<String>,
}

impl TaskGuard {
    fn start(kit: &Arc<ServiceKit>) -> Self {
        let id = kit.task_tracker().start();
        Self {
            kit: kit.clone(),
            id: Some(id),
        }
    }

    fn finish(&mut self) {
        if let Some(id) = self.id.take() {
            self.kit.task_tracker().finish(&id);
        }
    }
}

impl Drop for TaskGuard {
    fn drop(&mut self) {
        if let Some(id) = self.id.take() {
            self.kit.task_tracker().discard(&id);
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
        // Prefer the matched route template (bounded cardinality: one series
        // per registered route), so dynamic segments like `/tasks/{uuid}`
        // never become their own metric series. Fall back to the URI-path
        // heuristic only for requests that did not match a route (404/probe).
        let method = req
            .extensions()
            .get::<axum::extract::MatchedPath>()
            .map(|mp| mp.as_str().to_string())
            .unwrap_or_else(|| http_method_name(req.uri().path()));
        let kit = self.kit.clone();
        let request_size = req.body().size_hint().exact().unwrap_or(0) as f64;
        let mut inner = self.inner.clone();

        let start = std::time::Instant::now();
        kit.init_method(&method);
        kit.inc_stat(&format!("request___method__{method}"), 1.0);
        let mut guard = TaskGuard::start(&kit);

        let future = async move {
            let result = inner.call(req).await;
            let elapsed = start.elapsed().as_secs_f64();
            guard.finish();

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

/// Derive a metric method name from a raw URI path.
///
/// This is only a **fallback** for requests that did not match a registered
/// route, so `MatchedPath` is absent (e.g. a 404 / unknown-path probe).
/// The canonical source is [`MatchedPath`] (the registered route template),
/// which has bounded cardinality. Here we keep the last path segment but
/// collapse clearly dynamic segments (uuid-like, long hex, bare number) to
/// `id` so an unknown dynamic path still cannot explode the metric series.
pub fn http_method_name(path: &str) -> String {
    let trimmed = path.trim_end_matches('/');
    let last = trimmed
        .rsplit('/')
        .find(|s| !s.is_empty())
        .unwrap_or("root");
    if is_dynamic_segment(last) {
        "id".to_string()
    } else {
        last.to_string()
    }
}

/// Heuristic only for the fallback path: collapse a segment that is clearly
/// dynamic (a generated id) to a single bounded label `id`.
fn is_dynamic_segment(seg: &str) -> bool {
    if seg.is_empty() {
        return true;
    }
    // Bare integer.
    if seg.chars().all(|c| c.is_ascii_digit()) {
        return true;
    }
    let hex_count = seg.chars().filter(|c| c.is_ascii_hexdigit()).count();
    // Hex-id: entirely hex chars, optionally separated by '-'. This covers
    // short uuid-like ids ("deadbeef-1234", "abcd1234") as well as long
    // hashes, while leaving static segments like "summary"/"billing" (which
    // contain non-hex letters) untouched. Only the last path segment reaches
    // here, so an aggressive match cannot misfire on the api/v1 prefixes.
    let all_hex = seg.chars().all(|c| c.is_ascii_hexdigit() || c == '-');
    // UUID shape (dashes optional) or a long hex token.
    let uuidish = (seg.len() >= 32 && hex_count >= 32) || (seg.contains('-') && seg.len() >= 36);
    // Long hex token without separators (hash ids).
    let long_hex = seg.len() >= 24 && hex_count == seg.len();
    (all_hex && hex_count >= 8) || uuidish || long_hex
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

    /// Parse a scalar Prometheus metric (the first `{name{...} value}` line for
    /// the given metric name) into a f64. Helpers only for tests.
    fn parse_metric(text: &str, name: &str) -> f64 {
        text.lines()
            .find(|l| {
                l.starts_with(name)
                    && !l.starts_with(&format!("{name}_bucket"))
                    && !l.starts_with(&format!("{name}_sum"))
                    && !l.starts_with(&format!("{name}_count"))
            })
            .and_then(|l| l.split_whitespace().last())
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or_else(|| panic!("metric {name} not found in:\n{text}"))
    }

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

    /// A service whose handler never resolves (client gone / proxy reset
    /// mid-request). Dropping the request future must not leave the task in
    /// the tracker: previously `tasks_count` grew forever and
    /// `current_tasks_max_time_spend` accumulated (`HttpMetricsService`
    /// called `finish` only after `inner.call(req).await`).
    #[derive(Clone)]
    struct NeverService;

    impl Service<Request<axum::body::Body>> for NeverService {
        type Response = Response<axum::body::Body>;
        type Error = std::convert::Infallible;
        type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

        fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _: Request<axum::body::Body>) -> Self::Future {
            Box::pin(std::future::pending())
        }
    }

    #[tokio::test]
    async fn dropped_request_does_not_leak_task() {
        let kit = ServiceKit::new(MonitorServerConfig::default(), 4, Some(2));
        let mut svc = HttpMetricsLayer::new(kit.clone()).layer(NeverService);

        let req = Request::builder()
            .uri("/api/v1/chat/tasks/:id/transcript")
            .body(axum::body::Body::empty())
            .unwrap();
        let fut = svc.call(req);
        assert_eq!(kit.task_tracker().in_flight(), 1);
        drop(fut);
        assert_eq!(kit.task_tracker().in_flight(), 0);

        // A normal completion must not double-count or under-count.
        let ready = Arc::new(AtomicBool::new(true));
        kit.set_ready_probe(ready);
        let app = router(kit.clone()).layer(HttpMetricsLayer::new(kit.clone()));
        app.oneshot(
            Request::builder()
                .uri("/livez")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(kit.task_tracker().in_flight(), 0);
    }

    /// Regression: `sum + spent` must be monotonic even when a task is
    /// cancelled via `TaskGuard::drop -> discard()`. Before the fix, `discard`
    /// removed the task from `tasks` (the `sum` term) without crediting the
    /// elapsed time to `stats_tasks_time_spend_total` (the `spent` term), so
    /// `normalised` (and thus `rate()`) could drop, producing bogus spikes.
    #[tokio::test]
    async fn discarding_task_keeps_sum_plus_spent_monotonic() {
        let kit = ServiceKit::new(MonitorServerConfig::default(), 4, Some(2));
        let ready = Arc::new(AtomicBool::new(true));
        kit.set_ready_probe(ready);

        let metrics_after_start = {
            let id = kit.task_tracker().start();
            std::thread::sleep(std::time::Duration::from_millis(30));
            // "Cancel" the task while it is still in flight: the guard would
            // drop here, charging the elapsed time to `spent`.
            let text = kit.metrics_text();
            let sum_after_start = parse_metric(
                std::str::from_utf8(&text).unwrap(),
                "current_tasks_time_spend",
            );
            let spent_after_start = parse_metric(
                std::str::from_utf8(&text).unwrap(),
                "stats_tasks_time_spend_total",
            );
            (id, sum_after_start, spent_after_start)
        };
        let (id, sum_after_start, spent_after_start) = metrics_after_start;

        // Simulate the guard's Drop -> discard().
        std::thread::sleep(std::time::Duration::from_millis(30));
        kit.task_tracker().discard(&id);

        let text = kit.metrics_text();
        let sum_after_discard = parse_metric(
            std::str::from_utf8(&text).unwrap(),
            "current_tasks_time_spend",
        );
        let spent_after_discard = parse_metric(
            std::str::from_utf8(&text).unwrap(),
            "stats_tasks_time_spend_total",
        );

        // The task left `tasks` (sum dropped) ...
        assert!(
            sum_after_discard < sum_after_start,
            "sum should drop after discard: {sum_after_start} -> {sum_after_discard}"
        );
        // ... but its elapsed time was credited to `spent`, so the total
        // (sum + spent) must not decrease.
        assert!(
            spent_after_discard > spent_after_start,
            "spent should grow after discard: {spent_after_start} -> {spent_after_discard}"
        );
        assert!(
            sum_after_discard + spent_after_discard >= sum_after_start + spent_after_start,
            "sum+spent must be monotonic: {} + {} = {} vs {} + {} = {}",
            sum_after_discard,
            spent_after_discard,
            sum_after_discard + spent_after_discard,
            sum_after_start,
            spent_after_start,
            sum_after_start + spent_after_start,
        );
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
        assert_eq!(http_method_name("/health"), "health");
        assert_eq!(http_method_name("/ready"), "ready");
        assert_eq!(http_method_name("/"), "root");
        assert_eq!(http_method_name("/ready/"), "ready");
        // Fallback keeps the last path segment for static routes.
        assert_eq!(http_method_name("/metrics"), "metrics");
        assert_eq!(http_method_name("/livez"), "livez");
        assert_eq!(http_method_name("/api/v1/billing/summary"), "summary");
        // Fallback collapses clearly dynamic segments to a bounded label.
        assert_eq!(http_method_name("/api/v1/chat/tasks/1234"), "id");
        assert_eq!(
            http_method_name("/api/v1/chat/tasks/deadbeef-1234-1234-1234-1234567890ab"),
            "id"
        );
        // Short hex/uuid ids (previously leaked as unique method labels).
        assert_eq!(http_method_name("/api/v1/chat/tasks/deadbeef-1234"), "id");
        assert_eq!(http_method_name("/api/v1/chat/tasks/abcd1234"), "id");
        assert_eq!(http_method_name("/api/v1/chat/tasks/0a1b-2c3d"), "id");
    }

    #[tokio::test]
    async fn matched_path_is_used_as_method() {
        let kit = ServiceKit::new(MonitorServerConfig::default(), 4, Some(2));
        let ready = Arc::new(AtomicBool::new(true));
        kit.set_ready_probe(ready.clone());
        // A minimal router with a dynamic segment; the metrics layer must be a
        // route_layer so MatchedPath is present in req.extensions().
        let app = Router::new()
            .route("/api/v1/chat/tasks/:task_id", get(|| async {}))
            .route_layer(HttpMetricsLayer::new(kit.clone()))
            .with_state(());
        app.oneshot(
            Request::builder()
                .uri("/api/v1/chat/tasks/deadbeef-1234")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

        let text = String::from_utf8_lossy(&kit.metrics_text()).to_string();
        // The dynamic uuid must NOT be the method label; the route template is.
        assert!(
            !text.contains("method=\"deadbeef-1234\""),
            "dynamic id leaked into method label: {text}"
        );
        assert!(
            text.contains("method=\"/api/v1/chat/tasks/:task_id\""),
            "MatchedPath template expected as method, got: {text}"
        );
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
        // Registered route -> MatchedPath template (leading slash); unmatched
        // -> fallback `http_method_name` (no leading slash).
        assert!(text.contains("method=\"/livez\""));
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
