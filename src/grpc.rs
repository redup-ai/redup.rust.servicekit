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
                    let error_type = "other";
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
                Poll::Ready(Some(Ok(frame)))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            // End of stream: fire the size metric exactly once.
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
                let resp_body = tonic::body::boxed(
                    http_body_util::Full::new(axum::body::Bytes::from_static(b"a gRPC response"))
                        .map_err(|e| e),
                );
                Ok(HttpResponse::new(resp_body))
            })
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
