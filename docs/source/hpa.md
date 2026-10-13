# HPA integration

`stats_tasks_with_current_time_spend_normalised_total` is a gauge equal to
`(spent + current) / service_threads`, where `service_threads` comes from
`hpa_max_workers` (fallback `max_workers`). It is recomputed on every scrape,
so Prometheus `rate(...[2m])` works.

A Deckhouse `PodMetric` query follows the bibliochecker pattern:

```yaml
hpa:
  type: Pods
  averageValue: "80"
  podMetric:
    query: round(100* (avg by(<<.GroupBy>>) (rate(stats_tasks_with_current_time_spend_normalised_total{<<.LabelMatchers>>}[2m]))))
```

Scraping uses pod annotations on the metrics port (`/metrics`).