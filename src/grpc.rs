//! Tonic gRPC layer collecting per-method metrics.
//!
//! Analog of `aio_grpc_method_wrapper` from `redup.python.servicekit`:
//! per call it increments `stats_request{method}`, registers a task
//! (`started_total`), measures duration/response size, on success/failure
//! increments `stats_processed{method,status}`, on error increments
//! `stats_errors{method,type}` and observes `time full___method__<m>`
//! (EMA + `stats_hist_time_full`), `request_size___method__<m>` and
//! `response_size___method__<m>`.
//!
//! The layer is compatible with `tonic::transport::Server::layer`:
//! it wraps the service and maps HTTP requests to gRPC service/method names.

use crate::monitor::ServiceKit;
use http::{Request, Response};
use http_body::Body as HttpBody;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tonic::body::BoxBody;
use tower::{Layer, Service};

/// Tonic layer that records metrics around each gRPC request.
#[derive(Clone)]
pub struct GrpcMetricsLayer {
    kit: Arc<ServiceKit>,
}

impl GrpcMetricsLayer {
    pub fn new(kit: Arc<ServiceKit>) -> Self {
        Self { kit }
    }
}

impl<S> Layer<S> for GrpcMetricsLayer {
    type Service = GrpcMetricsService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        GrpcMetricsService {
            inner,
            kit: self.kit.clone(),
        }
    }
}

/// RAII guard for the in-flight gRPC task metric.
///
/// `GrpcMetricsService::call` must finish the tracked task exactly once per
/// request. A regular completion calls [`finish`](Self::finish) (records the
/// duration); if the handler future is dropped mid-flight — client gone,
/// proxy reset, deadline at the edge — the guard's `Drop` discards the task
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

/// Service wrapper produced by [`GrpcMetricsLayer`].
#[derive(Clone)]
pub struct GrpcMetricsService<S> {
    inner: S,
    kit: Arc<ServiceKit>,
}

impl<S> Service<Request<BoxBody>> for GrpcMetricsService<S>
where
    S: Service<Request<BoxBody>, Response = Response<BoxBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = Response<BoxBody>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<BoxBody>) -> Self::Future {
        let method = grpc_method_name(req.uri().path());
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
                    let response_size = response.body().size_hint().exact().unwrap_or(0) as f64;
                    kit.inc_stat(
                        &format!("processed___method__{method}___status__success"),
                        1.0,
                    );
                    kit.observe_stat(&format!("time full___method__{method}"), elapsed);
                    kit.observe_stat(&format!("request_size___method__{method}"), request_size);
                    kit.observe_stat(&format!("response_size___method__{method}"), response_size);
                    kit.set_task_status(crate::monitor::TaskStatus::Ok, &method);
                    Ok(response)
                }
                Err(error) => {
                    kit.inc_stat(
                        &format!("processed___method__{method}___status__failure"),
                        1.0,
                    );
                    let error_type = "other";
                    kit.inc_stat(
                        &format!("errors___method__{method}___type__{error_type}"),
                        1.0,
                    );
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

/// Derive gRPC method name from the URI path.
/// Tonic maps `/service.Method/Call` to `service.Method`, so the method
/// component (last path segment) is the called handler name, e.g.
/// `/doaid.users.v1.UsersService/CreateUser` -> `CreateUser`.
///
/// Empty or root paths (`/`, `//`) have no method; they fall back to
/// `unknown` so per-method stat keys never contain an empty label value
/// (empty values make `parse_stat_key` produce an empty label name, which
/// panics `GaugeVec::new` and poisons the whole metrics registry).
pub fn grpc_method_name(path: &str) -> String {
    let trimmed = path.trim_start_matches('/');
    let method = trimmed
        .rsplit_once('/')
        .map(|(_, method)| method)
        .unwrap_or(trimmed);
    if method.is_empty() {
        "unknown".to_string()
    } else {
        method.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MonitorServerConfig, ServiceKit};
    use http::{Method, Request as HttpRequest, Response as HttpResponse, Uri};
    use tonic::body::empty_body;

    #[test]
    fn method_name_from_path() {
        assert_eq!(
            grpc_method_name("/doaid.users.v1.UsersService/CreateUser"),
            "CreateUser"
        );
        assert_eq!(grpc_method_name("/abc.Def/H"), "H");
        assert_eq!(grpc_method_name("/x/y/z"), "z");
        assert_eq!(grpc_method_name("/"), "unknown");
        assert_eq!(grpc_method_name("//"), "unknown");
    }

    #[tokio::test]
    async fn layer_counts_map_request() {
        let kit = ServiceKit::new(MonitorServerConfig::default(), 4, Some(2));
        let layer = GrpcMetricsLayer::new(kit.clone());
        // Build a trivial service that echoes the request body.
        let svc = EchoService;
        let mut svc = layer.layer(svc);
        let request = HttpRequest::builder()
            .method(Method::POST)
            .uri(Uri::from_static("/doaid.users.v1.UsersService/CreateUser"))
            .body(empty_body())
            .unwrap();
        let _ = svc.call(request).await;
        // metrics should include started/completed totals and per-method stats
        let bytes = kit.metrics_text();
        let text = String::from_utf8_lossy(&bytes);
        assert!(text.contains("stats_request"));
        assert!(text.contains("method=\"CreateUser\""));
        assert!(text.contains("stats_processed{"));
        assert!(text.contains("started_total"));
        assert!(text.contains("completed_total"));
        // Counter must grow on each call, not be reset by init_method.
        let bytes = kit.metrics_text();
        let text = String::from_utf8_lossy(&bytes);
        let line = text
            .lines()
            .find(|l| l.starts_with("stats_request") && l.contains("method=\"CreateUser\""))
            .expect("stats_request for CreateUser");
        assert!(
            line.ends_with(" 1"),
            "stats_request must be 1 after one call, got: {line}"
        );
    }

    #[tokio::test]
    async fn root_path_does_not_poison_metrics() {
        // Regression: a request to "/" (e.g. ingress health check / plain
        // HTTP GET against the gRPC port) must not panic inside the metrics
        // layer; it is labeled `method="unknown"` and never empties the
        // `method` label name.
        let kit = ServiceKit::new(MonitorServerConfig::default(), 4, Some(2));
        let layer = GrpcMetricsLayer::new(kit.clone());
        let mut svc = layer.layer(EchoService);
        let request = HttpRequest::builder()
            .method(Method::POST)
            .uri(Uri::from_static("/"))
            .body(empty_body())
            .unwrap();
        let _ = svc.call(request).await;
        let bytes = kit.metrics_text();
        let text = String::from_utf8_lossy(&bytes);
        assert!(
            text.contains("method=\"unknown\""),
            "root request must be labeled unknown, got: {text}"
        );
        assert!(
            text.contains("stats_request") && text.contains("stats_processed{"),
            "metrics must be exported after root request"
        );
    }

    #[tokio::test]
    async fn layer_request_counter_accumulates() {
        let kit = ServiceKit::new(MonitorServerConfig::default(), 4, Some(2));
        let layer = GrpcMetricsLayer::new(kit.clone());
        for _ in 0..3 {
            let mut svc = layer.layer(EchoService);
            let request = HttpRequest::builder()
                .method(Method::POST)
                .uri(Uri::from_static("/doaid.users.v1.UsersService/CreateUser"))
                .body(empty_body())
                .unwrap();
            let _ = svc.call(request).await;
        }
        let bytes = kit.metrics_text();
        let text = String::from_utf8_lossy(&bytes);
        let line = text
            .lines()
            .find(|l| l.starts_with("stats_request") && l.contains("method=\"CreateUser\""))
            .expect("stats_request for CreateUser");
        assert!(
            line.ends_with(" 3"),
            "stats_request must accumulate to 3, got: {line}"
        );
    }

    #[tokio::test]
    async fn dropped_request_does_not_leak_task() {
        // Regression: client cancels the RPC before the handler future
        // completes; the in-flight task must be discarded on drop instead of
        // staying in `tasks` forever (tasks_count/current_tasks_max_time_spend
        // would otherwise grow unboundedly).
        let kit = ServiceKit::new(MonitorServerConfig::default(), 4, Some(2));
        let mut svc = GrpcMetricsLayer::new(kit.clone()).layer(NeverService);

        let request = HttpRequest::builder()
            .method(Method::POST)
            .uri(Uri::from_static("/doaid.users.v1.UsersService/CreateUser"))
            .body(empty_body())
            .unwrap();
        let fut = svc.call(request);
        assert_eq!(kit.task_tracker().in_flight(), 1);
        drop(fut);
        assert_eq!(kit.task_tracker().in_flight(), 0);

        // A normal completion must not double-count or under-count.
        let mut svc = GrpcMetricsLayer::new(kit.clone()).layer(EchoService);
        let request = HttpRequest::builder()
            .method(Method::POST)
            .uri(Uri::from_static("/doaid.users.v1.UsersService/CreateUser"))
            .body(empty_body())
            .unwrap();
        let _ = svc.call(request).await;
        assert_eq!(kit.task_tracker().in_flight(), 0);
    }

    #[derive(Clone)]
    struct EchoService;

    impl Service<HttpRequest<BoxBody>> for EchoService {
        type Response = HttpResponse<BoxBody>;
        type Error = tonic::Status;
        type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: HttpRequest<BoxBody>) -> Self::Future {
            let _ = _req;
            Box::pin(async move { Ok(HttpResponse::new(empty_body())) })
        }
    }

    /// A service whose future never resolves; dropping that future simulates
    /// a client cancel / reset mid-request.
    #[derive(Clone)]
    struct NeverService;

    impl Service<HttpRequest<BoxBody>> for NeverService {
        type Response = HttpResponse<BoxBody>;
        type Error = tonic::Status;
        type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: HttpRequest<BoxBody>) -> Self::Future {
            let _ = _req;
            Box::pin(std::future::pending())
        }
    }
}
