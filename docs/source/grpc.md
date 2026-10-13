# gRPC layer

`GrpcMetricsLayer` wraps every request of a tonic server and records
per-method metrics (`stats_request`, `stats_processed`, `stats_errors`,
`stats_hist_time_full`, request/response sizes, EMA series).

```rust,ignore
let kit = ServiceKit::new(monitor, max_workers, hpa_workers);
let layer = redup_servicekit::grpc::GrpcMetricsLayer::new(kit.clone());

tonic::transport::Server::builder()
    .layer(layer)
    .add_service(my_service)
    .serve(addr)
    .await?;
```

The layer name is derived from the gRPC path
(`/doaid.users.v1.UsersService/CreateUser` -> `CreateUser`). Streaming RPC
sizes are measured via `size_hint` (best effort); v1 focuses on unary calls.