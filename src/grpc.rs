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
use axum::body::Bytes;
use http::{Request, Response};
use http_body::Body as HttpBody;
use http_body::{Frame, SizeHint};
use http_body_util::BodyExt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::Semaphore;
use tonic::body::BoxBody;
use tonic::Status;
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
    S::Error: 'static + std::fmt::Debug,
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

        // gRPC request/response bodies are streamed lazily, so
        // `size_hint().exact()` is always `None` — which made
        // `request_size`/`response_size` observe `0` to the histograms.
        // Instead, wrap each body in a counter that records the real wire
        // byte size as its frames are polled, and observe the metric in the
        // body's `on_end` hook, which fires exactly once once the stream is
        // fully drained (request: consumed by the handler; response: written
        // over the wire). This yields real, non-zero sizes.
        let request_metric = kit.clone();
        let request_method = method.clone();
        let (req_parts, req_body) = req.into_parts();
        let req_body = CountingBody::new(
            req_body,
            Some(Box::new(move |bytes: usize| {
                request_metric.observe_stat(
                    &format!("request_size___method__{request_method}"),
                    bytes as f64,
                );
            })),
        )
        .boxed_unsync();
        let req = Request::from_parts(req_parts, req_body);
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
                    // tonic turns a handler `Err(Status)` into an
                    // `Ok(http::Response)` whose headers carry the real
                    // `grpc-status` (non-zero) — the generated service has
                    // `Error = Infallible`, so a backend failure never
                    // reaches this layer as `Err`. Without this check a
                    // failed request is recorded as a success and never
                    // lands in `stats_errors` / `processed___status__failure`.
                    let grpc_code = response
                        .headers()
                        .get(Status::GRPC_STATUS)
                        .and_then(|v| v.to_str().ok())
                        .and_then(|v| v.parse::<i32>().ok());
                    let is_error = grpc_code.map(|c| c != 0).unwrap_or(false);

                    let response_metric = kit.clone();
                    let response_method = method.clone();
                    let (resp_parts, resp_body) = response.into_parts();
                    let resp_body = CountingBody::new(
                        resp_body,
                        Some(Box::new(move |bytes: usize| {
                            response_metric.observe_stat(
                                &format!("response_size___method__{response_method}"),
                                bytes as f64,
                            );
                        })),
                    )
                    .boxed_unsync();
                    let response = Response::from_parts(resp_parts, resp_body);

                    if is_error {
                        kit.inc_stat(
                            &format!("processed___method__{method}___status__failure"),
                            1.0,
                        );
                        // Classify into `stats_errors{type}` from the backend
                        // message (Python `ErrorParser.parse`): the configured
                        // "service_errors" words (textprocessor/translation/...)
                        // in the grpc-message decide the label, defaulting to
                        // "other". This lets a backend failure surface as its
                        // own error type instead of always collapsing into
                        // `type="other"`.
                        let error_message = response
                            .headers()
                            .get(Status::GRPC_MESSAGE)
                            .and_then(|v| v.to_str().ok())
                            .unwrap_or("");
                        let error_type = kit.classify_error(error_message);
                        kit.inc_stat(
                            &format!("errors___method__{method}___type__{error_type}"),
                            1.0,
                        );
                        kit.observe_stat(&format!("time full___method__{method}"), elapsed);
                        kit.set_task_status(crate::monitor::TaskStatus::OtherException, &method);
                        return Ok(response);
                    }

                    kit.inc_stat(
                        &format!("processed___method__{method}___status__success"),
                        1.0,
                    );
                    kit.observe_stat(&format!("time full___method__{method}"), elapsed);
                    kit.set_task_status(crate::monitor::TaskStatus::Ok, &method);
                    Ok(response)
                }
                Err(error) => {
                    kit.inc_stat(
                        &format!("processed___method__{method}___status__failure"),
                        1.0,
                    );
                    // Same classification as the grpc-status branch: pick the
                    // configured error type from the error text, default `other`.
                    let error_type = kit.classify_error(&format!("{error:?}"));
                    kit.inc_stat(
                        &format!("errors___method__{method}___type__{error_type}"),
                        1.0,
                    );
                    kit.observe_stat(&format!("time full___method__{method}"), elapsed);
                    kit.set_task_status(crate::monitor::TaskStatus::OtherException, &method);
                    Err(error)
                }
            }
        };
        Box::pin(future)
    }
}

/// Boxed error type accepted by tonic's `Server::layer` stack
/// (`tonic::transport::Error` / `crate::Error` is `Box<dyn Error + Send + Sync>`).
type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Tonic layer that limits the number of concurrent in-flight gRPC requests
/// across the whole server — **all methods share one semaphore**, mirroring
/// Python `grpc.aio.server(maximum_concurrent_rpcs = grpc_queue_size)`.
///
/// Unlike `tower::limit::ConcurrencyLimit`, which **queues** requests and
/// keeps them buffered in memory (precisely the OOM pattern this layer is
/// meant to remove), [`ConcurrencyLimitLayer`] rejects immediately with gRPC
/// `RESOURCE_EXHAUSTED` (code 8) once the in-flight budget is exhausted. The
/// caller is signalled to retry elsewhere instead of piling up memory.
///
/// Tonic wraps the whole user layer stack with `RecoverError`, which converts
/// any `Err(Status)` returned here into a real `grpc-status` trailer — so the
/// rejection reaches the client as code 8 on the wire.
#[derive(Clone)]
pub struct ConcurrencyLimitLayer {
    semaphore: Arc<Semaphore>,
}

impl ConcurrencyLimitLayer {
    /// `limit` is the maximum number of requests that may be in-flight at once
    /// across the entire server (Python `grpc_queue_size`).
    pub fn new(limit: usize) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(limit)),
        }
    }
}

impl<S> Layer<S> for ConcurrencyLimitLayer {
    type Service = ConcurrencyLimitService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        ConcurrencyLimitService {
            inner,
            semaphore: self.semaphore.clone(),
        }
    }
}

/// Service wrapper produced by [`ConcurrencyLimitLayer`].
#[derive(Clone)]
pub struct ConcurrencyLimitService<S> {
    inner: S,
    semaphore: Arc<Semaphore>,
}

impl<S> Service<Request<BoxBody>> for ConcurrencyLimitService<S>
where
    S: Service<Request<BoxBody>, Response = Response<BoxBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<BoxError>,
{
    type Response = Response<BoxBody>;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, req: Request<BoxBody>) -> Self::Future {
        // Acquire a permit immediately and do NOT queue: when the global
        // in-flight budget is exhausted, reject with RESOURCE_EXHAUSTED (code
        // 8) so the caller retries elsewhere instead of the request piling up
        // in memory (the cause of the prod OOM restarts).
        let permit = match self.semaphore.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                return Box::pin(async move {
                    Err(
                        Status::resource_exhausted("service is overloaded, please retry later")
                            .into(),
                    )
                });
            }
        };

        let mut inner = self.inner.clone();
        Box::pin(async move {
            // Held for the duration of the handler future; released on drop
            // (including when the handler future is cancelled mid-flight).
            let _permit = permit;
            inner.call(req).await.map_err(Into::into)
        })
    }
}

/// Wraps a body and counts the bytes of every DATA frame it yields,
/// invoking `on_end` with the accumulated total exactly once, when the
/// stream is fully drained. Used for both requests and responses in the
/// gRPC case, where `size_hint().exact()` is `None`.
struct CountingBody {
    inner: BoxBody,
    counter: Arc<ByteCounter>,
    on_end: Option<Box<dyn FnOnce(usize) + Send>>,
}

impl CountingBody {
    fn new(inner: BoxBody, on_end: Option<Box<dyn FnOnce(usize) + Send>>) -> Self {
        Self {
            inner,
            counter: Arc::new(ByteCounter::new()),
            on_end,
        }
    }
}

impl HttpBody for CountingBody {
    type Data = Bytes;
    type Error = tonic::Status;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.as_mut().get_mut();
        match Pin::new(&mut this.inner).poll_frame(cx) {
            // Count the actual bytes of each DATA frame.
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(data) = frame.data_ref() {
                    this.counter.add(data.len());
                }
                // gRPC over HTTP/2 marks the end of a response with a
                // trailers frame; hyper may stop polling after trailers, so a
                // later `Poll::Ready(None)` may never reach us. Fire the size
                // metric here, on the trailers frame, to guarantee it is
                // emitted exactly once. `on_end` is take()'n, so a subsequent
                // terminal poll is a no-op.
                if frame.is_trailers() {
                    if let Some(cb) = this.on_end.take() {
                        cb(this.counter.load(Ordering::Relaxed));
                    }
                }
                Poll::Ready(Some(Ok(frame)))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            // End of stream: fire the size metric if not already emitted.
            Poll::Ready(None) => {
                if let Some(cb) = this.on_end.take() {
                    cb(this.counter.load(Ordering::Relaxed));
                }
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

/// Shared accumulator for a [`CountingBody`].
struct ByteCounter {
    bytes: AtomicUsize,
}

impl ByteCounter {
    fn new() -> Self {
        Self {
            bytes: AtomicUsize::new(0),
        }
    }

    fn add(&self, n: usize) {
        self.bytes.fetch_add(n, Ordering::Relaxed);
    }

    fn load(&self, o: Ordering) -> usize {
        self.bytes.load(o)
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

    #[tokio::test]
    async fn request_response_size_are_non_zero() {
        // Regression: gRPC bodies are streamed lazily, so `size_hint()` is
        // `None` and `request_size`/`response_size` used to observe `0`.
        // The CountingBody wraps the bodies and records the real bytes as the
        // frames are polled, so the histograms must end up with a sum > 0.
        let kit = ServiceKit::new(MonitorServerConfig::default(), 4, Some(2));
        let mut svc = GrpcMetricsLayer::new(kit.clone()).layer(EchoService);

        // Non-empty request body (must be drained by the handler to count).
        let body = tonic::body::boxed(
            http_body_util::Full::new(axum::body::Bytes::from_static(b"hello gRPC world"))
                .map_err(|e| e),
        );
        let request = HttpRequest::builder()
            .method(Method::POST)
            .uri(Uri::from_static("/doaid.users.v1.UsersService/CreateUser"))
            .body(body)
            .unwrap();
        let resp = svc.call(request).await.unwrap();

        // Drain the response body so the CountingBody sees the end-of-stream
        // and fires its `on_end` observation once.
        let mut m = std::pin::pin!(resp.into_body());
        while std::future::poll_fn(|cx| m.as_mut().poll_frame(cx))
            .await
            .is_some()
        {}

        let text = String::from_utf8_lossy(&kit.metrics_text()).to_string();
        // request_size was counted when the (empty) handler drained — full
        // body was buffered by tonic decode, so it should be counted.
        let req_line = text
            .lines()
            .find(|l| {
                l.starts_with("stats_hist_request_size_sum") && l.contains("method=\"CreateUser\"")
            })
            .expect("request_size_sum");
        let req_sum = req_line
            .rsplit(' ')
            .next()
            .unwrap()
            .trim()
            .parse::<f64>()
            .unwrap();
        assert!(
            req_sum > 0.0,
            "request_size_sum must be > 0, got: {req_line}"
        );

        let resp_line = text
            .lines()
            .find(|l| {
                l.starts_with("stats_hist_response_size_sum") && l.contains("method=\"CreateUser\"")
            })
            .expect("response_size_sum");
        let resp_sum = resp_line
            .rsplit(' ')
            .next()
            .unwrap()
            .trim()
            .parse::<f64>()
            .unwrap();
        assert!(
            resp_sum > 0.0,
            "response_size_sum must be > 0, got: {resp_line}"
        );
    }

    #[tokio::test]
    async fn concurrency_limit_rejects_when_budget_exhausted() {
        // Global in-flight budget of 1 across the whole server. The first
        // request acquires the only permit and blocks, so the second
        // concurrent request must be rejected with gRPC RESOURCE_EXHAUSTED
        // (code 8) — not queued/buffered.
        let layer = ConcurrencyLimitLayer::new(1);
        let mut svc = layer.layer(BlockingService);

        let build_req = || {
            HttpRequest::builder()
                .method(Method::POST)
                .uri(Uri::from_static("/doaid.users.v1.UsersService/CreateUser"))
                .body(empty_body())
                .unwrap()
        };

        let first = svc.call(build_req());
        // Give the first request a chance to acquire the only permit.
        tokio::task::yield_now().await;

        assert_eq!(layer.semaphore.available_permits(), 0);

        let second = svc.call(build_req()).await;
        let err = match second {
            Ok(_) => panic!("second request must be rejected when budget exhausted"),
            Err(e) => e,
        };
        let err = err
            .downcast::<tonic::Status>()
            .expect("rejection must be a tonic::Status");
        assert_eq!(
            err.code(),
            tonic::Code::ResourceExhausted,
            "expected RESOURCE_EXHAUSTED (8), got: {err}"
        );

        // The blocking first request still holds the permit; drop it (a
        // cancelled client / dropped future should release the permit).
        drop(first);
        tokio::task::yield_now().await;
        assert_eq!(layer.semaphore.available_permits(), 1);
    }

    #[tokio::test]
    async fn concurrency_limit_allows_within_budget() {
        let layer = ConcurrencyLimitLayer::new(2);
        let mut svc = layer.layer(EchoService);

        let build_req = || {
            HttpRequest::builder()
                .method(Method::POST)
                .uri(Uri::from_static("/doaid.users.v1.UsersService/CreateUser"))
                .body(empty_body())
                .unwrap()
        };

        // Two requests within the budget of 2 both succeed.
        let r1 = svc.call(build_req()).await.expect("first request");
        let r2 = svc.call(build_req()).await.expect("second request");
        assert_eq!(r1.status(), http::StatusCode::OK);
        assert_eq!(r2.status(), http::StatusCode::OK);
        // Permits released after completion.
        assert_eq!(layer.semaphore.available_permits(), 2);
    }

    #[tokio::test]
    async fn grpc_status_in_ok_response_is_counted_as_failure() {
        // tonic's generated server converts a handler `Err(status)` into an
        // `Ok(http::Response)` whose *headers* carry `grpc-status` != 0
        // (the service has `Error = Infallible`, so the metrics layer never
        // sees it as `Err`). Regression: such a request must be recorded as a
        // failure and land in `stats_errors`, not counted as a success.
        let kit = ServiceKit::new(MonitorServerConfig::default(), 4, Some(2));
        let layer = GrpcMetricsLayer::new(kit.clone());
        let mut svc = layer.layer(ErrorStatusService { code: 13 });

        let request = HttpRequest::builder()
            .method(Method::POST)
            .uri(Uri::from_static("/doaid.users.v1.TextPreparator/Prepare"))
            .body(empty_body())
            .unwrap();
        let _ = svc.call(request).await;

        let text = String::from_utf8_lossy(&kit.metrics_text()).to_string();
        let metric_value = |metric: &str, label: &str| -> f64 {
            text.lines()
                .filter(|l| l.starts_with(metric) && l.contains(label))
                .find_map(|l| l.rsplit(' ').next())
                .and_then(|v| v.trim().parse::<f64>().ok())
                .unwrap_or(0.0)
        };
        assert_eq!(
            metric_value("stats_processed", "status=\"failure\""),
            1.0,
            "non-zero grpc-status must be counted as a failure, metric text:\n{text}"
        );
        assert_eq!(
            metric_value("stats_errors", "type=\"other\""),
            1.0,
            "non-zero grpc-status must be counted in stats_errors, metric text:\n{text}"
        );
        assert_eq!(
            metric_value("stats_processed", "status=\"success\""),
            0.0,
            "non-zero grpc-status must not be counted as a success, metric text:\n{text}"
        );
    }

    #[tokio::test]
    async fn configured_error_type_is_used_instead_of_other() {
        // Regression for ErrorParser.classify: with `errors` config supplying
        // `service_errors = ["textprocessor", "translation"]`, a backend failure
        // whose grpc-message says "textprocessor: ..." must be recorded under
        // `stats_errors{type="textprocessor"}`, not the hardcoded `"other"`.
        let mut monitor = crate::config::MonitorServerConfig::default();
        monitor.errors.insert(
            "OTHER_EXCEPTION".to_string(),
            crate::config::ErrorEntry {
                service_errors: vec!["textprocessor".to_string(), "translation".to_string()],
                unhealthy: -1,
            },
        );
        let kit = ServiceKit::new(monitor, 4, Some(2));
        let layer = GrpcMetricsLayer::new(kit.clone());

        // Reuse ErrorStatusService but make it emit a grpc-message carrying a
        // configured service-error word. We wrap with a lossy passthrough so the
        // headers-mutating service can set GRPC_MESSAGE too.
        let mut svc = layer.layer(StatusWithMessageService {
            code: 13,
            message: "textprocessor: unexpected ValueError".to_string(),
        });

        let request = HttpRequest::builder()
            .method(Method::POST)
            .uri(Uri::from_static("/doaid.users.v1.TextPreparator/Prepare"))
            .body(empty_body())
            .unwrap();
        let _ = svc.call(request).await;

        let text = String::from_utf8_lossy(&kit.metrics_text()).to_string();
        let metric_value = |metric: &str, label: &str| -> f64 {
            text.lines()
                .filter(|l| l.starts_with(metric) && l.contains(label))
                .find_map(|l| l.rsplit(' ').next())
                .and_then(|v| v.trim().parse::<f64>().ok())
                .unwrap_or(0.0)
        };
        assert_eq!(
            metric_value("stats_errors", "type=\"textprocessor\""),
            1.0,
            "configured service-error must be used as the error type, metric text:\n{text}"
        );
        assert_eq!(
            metric_value("stats_errors", "type=\"other\""),
            0.0,
            "must not fall back to 'other' when a configured type matches, metric text:\n{text}"
        );
        assert_eq!(
            metric_value("stats_processed", "status=\"failure\""),
            1.0,
            "still counted as a failure, metric text:\n{text}"
        );
    }

    #[derive(Clone)]
    struct ErrorStatusService {
        code: i32,
    }

    impl Service<HttpRequest<BoxBody>> for ErrorStatusService {
        type Response = HttpResponse<BoxBody>;
        type Error = tonic::Status;
        type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: HttpRequest<BoxBody>) -> Self::Future {
            let code = self.code;
            Box::pin(async move {
                // Mimic tonic `Status::into_http()`: an Ok response whose
                // immediate headers carry `grpc-status` != 0.
                let mut response = HttpResponse::new(tonic::body::empty_body());
                response
                    .headers_mut()
                    .insert(Status::GRPC_STATUS, code.to_string().parse().unwrap());
                Ok(response)
            })
        }
    }

    /// Like `ErrorStatusService`, but also sets `grpc-message` so the metrics
    /// layer has text to classify into a configured error type.
    #[derive(Clone)]
    struct StatusWithMessageService {
        code: i32,
        message: String,
    }

    impl Service<HttpRequest<BoxBody>> for StatusWithMessageService {
        type Response = HttpResponse<BoxBody>;
        type Error = tonic::Status;
        type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: HttpRequest<BoxBody>) -> Self::Future {
            let code = self.code;
            let message = self.message.clone();
            Box::pin(async move {
                let mut response = HttpResponse::new(tonic::body::empty_body());
                response
                    .headers_mut()
                    .insert(Status::GRPC_STATUS, code.to_string().parse().unwrap());
                response.headers_mut().insert(
                    Status::GRPC_MESSAGE,
                    tonic::codegen::http::HeaderValue::from_str(&message).unwrap(),
                );
                Ok(response)
            })
        }
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

        fn call(&mut self, req: HttpRequest<BoxBody>) -> Self::Future {
            Box::pin(async move {
                // Drain the request body so the metrics CountingBody sees the
                // end-of-stream and fires its `on_end` observation (tonic does
                // this in production when it decodes the request message).
                let mut req_body = std::pin::pin!(req.into_body());
                while std::future::poll_fn(|cx| req_body.as_mut().poll_frame(cx))
                    .await
                    .is_some()
                {}
                // Return a non-empty body so the response-size histogram is
                // counted too (an empty body would correctly give sum 0).
                // Use `with_trailers` so the body emits a trailers frame like
                // real gRPC responses do; the metrics CountingBody fires its
                // `on_end` on the trailers frame, not only on a terminal None.
                let resp_body =
                    http_body_util::Full::new(axum::body::Bytes::from_static(b"a gRPC response"))
                        .map_err(|e| e)
                        .with_trailers(async {
                            let mut map = http::HeaderMap::new();
                            map.insert("grpc-status", "0".parse().unwrap());
                            Some(Ok(map))
                        });
                Ok(HttpResponse::new(tonic::body::boxed(resp_body)))
            })
        }
    }

    /// A service whose request never resolves; the in-flight permit it
    /// acquired stays held until the future is dropped. Used to hold the
    /// semaphore budget deterministically so a concurrent request can be
    /// observed being rejected.
    #[derive(Clone)]
    struct BlockingService;

    impl Service<HttpRequest<BoxBody>> for BlockingService {
        type Response = HttpResponse<BoxBody>;
        type Error = tonic::Status;
        type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: HttpRequest<BoxBody>) -> Self::Future {
            Box::pin(std::future::pending())
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
