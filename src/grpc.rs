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
        kit.inc_stat(&format!("request___method__{method}"), 1.0);
        kit.init_method(&method);
        let task_id = kit.task_tracker().start();

        let future = async move {
            let result = inner.call(req).await;
            let elapsed = start.elapsed().as_secs_f64();
            kit.task_tracker().finish(&task_id);

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
pub fn grpc_method_name(path: &str) -> String {
    let trimmed = path.trim_start_matches('/');
    trimmed
        .rsplit_once('/')
        .map(|(_, method)| method)
        .unwrap_or(trimmed)
        .to_string()
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
}
