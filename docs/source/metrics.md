# Metrics

All metrics carry the `inittime` label (process start time), `inittime` first
— exactly like `redup.python.servicekit`. The registry is refreshed lazily on
each `/metrics` request (`ServiceKit::refresh`).

## Task metrics

| Metric | Type | Meaning |
| --- | --- | --- |
| `started_total` | counter | started tasks |
| `completed_total` | counter | completed tasks |
| `tasks_count` | gauge | tasks in flight |
| `current_tasks_time_spend` | gauge | sum of now - start over in-flight tasks |
| `current_tasks_max_time_spend` | gauge | max of now - start over in-flight tasks |
| `stats_tasks_time_spend_total` | counter | total time of completed tasks |
| `stats_tasks_with_current_time_spend_total` | gauge | spent + current (monotonic) |
| `stats_tasks_with_current_time_spend_normalised_total` | gauge | total / service_threads |
| `tasks_time_spent_quantile` | histogram | task duration (time buckets) |
| `service_threads` | gauge | hpa_max_workers (target HPA pool) |

## Per-method stats (gRPC layer)

| Metric | Labels |
| --- | --- |
| `stats_request` | `method` |
| `stats_processed` | `method`, `status` (success/failure) |
| `stats_errors` | `method`, `type` |
| `stats_aggregation_<key>` | method labels (EMA, long window alpha 2/101) |
| `stats_aggregation_last_<key>` | method labels (EMA, short window alpha 2/6) |
| `stats_hist_time_<key>` / `stats_hist_<size key>` | method labels (histogram) |

## Histogram buckets

- `time` (seconds): matches Python `BUCKETS_HIST["time"]` (47 upper bounds,
  3600 max);
- `size` (bytes): matches Python `BUCKETS_HIST["size"]` (55 upper bounds,
  2000000000 max).

Counter names already include the `_total` suffix (`started_total`,
`completed_total`, `stats_tasks_time_spend_total`) because the wire format of
the Rust exporting crate differs from the Python client in this detail; the
final exposed metric names match Python exactly.