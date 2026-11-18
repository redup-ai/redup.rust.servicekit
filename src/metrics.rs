//! Prometheus metric registry, bucket constants and EMA store.
//!
//! Metric names, label names, label ordering and histogram buckets are
//! semantically identical to `redup.python.servicekit`
//! (`redup_servicekit.metrics` + `MonitorStorage`).
//!
//! Naming rule: the Python client renders counters with a `_total` suffix
//! (`started` -> `started_total`), while the Rust `prometheus` crate does not
//! append it. To keep the wire format identical, counter metric names in this
//! crate already include the `_total` suffix, e.g. `started_total`,
//! `completed_total`, `stats_tasks_time_spend_total`.

use prometheus::{
    CounterVec, Encoder, GaugeVec, HistogramOpts, HistogramVec, Opts, Registry, TextEncoder,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Time histogram bucket boundaries (seconds), identical to `BUCKETS_HIST["time"]`.
pub const TIME_BUCKETS: &[f64] = &[
    0.0, 0.01, 0.02, 0.03, 0.05, 0.1, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0, 6.0, 7.0,
    8.0, 9.0, 10.0, 15.0, 20.0, 25.0, 30.0, 35.0, 40.0, 45.0, 50.0, 55.0, 60.0, 90.0, 120.0, 180.0,
    240.0, 270.0, 300.0, 360.0, 420.0, 480.0, 540.0, 600.0, 900.0, 1200.0, 1500.0, 1800.0, 3600.0,
];

/// Size histogram bucket boundaries (bytes), identical to `BUCKETS_HIST["size"]`.
pub const SIZE_BUCKETS: &[f64] = &[
    0.0,
    1.0,
    5.0,
    10.0,
    20.0,
    50.0,
    100.0,
    500.0,
    1000.0,
    5000.0,
    10000.0,
    15000.0,
    30000.0,
    150000.0,
    300000.0,
    450000.0,
    600000.0,
    750000.0,
    900000.0,
    1050000.0,
    1200000.0,
    1350000.0,
    1500000.0,
    1800000.0,
    2100000.0,
    2400000.0,
    2700000.0,
    3000000.0,
    3750000.0,
    4500000.0,
    6000000.0,
    7500000.0,
    9000000.0,
    10500000.0,
    12000000.0,
    13500000.0,
    15000000.0,
    17500000.0,
    20000000.0,
    25000000.0,
    35000000.0,
    50000000.0,
    65000000.0,
    80000000.0,
    100000000.0,
    120000000.0,
    140000000.0,
    160000000.0,
    200000000.0,
    260000000.0,
    360000000.0,
    500000000.0,
    750000000.0,
    1000000000.0,
    2000000000.0,
];

/// `stats_hist_<key>` bucket type selection (matches `BUCKETS_HIST` key).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BucketType {
    Time,
    Size,
}

impl BucketType {
    pub fn buckets(self) -> &'static [f64] {
        match self {
            BucketType::Time => TIME_BUCKETS,
            BucketType::Size => SIZE_BUCKETS,
        }
    }
}

/// Detect the bucket type for an observation key (Python rule: `time` or
/// `size` substring in the metric key).
pub fn bucket_type_for_key(key: &str) -> Option<BucketType> {
    if key.contains("time") {
        Some(BucketType::Time)
    } else if key.contains("size") {
        Some(BucketType::Size)
    } else {
        None
    }
}

/// Sanitize a metric name the same way as
/// Sanitize a metric name so it is always a valid Prometheus metric name
/// (`[a-zA-Z_][a-zA-Z0-9_]*`) — `GaugeVec::new`/`CounterVec::new`/`HistogramVec::new`
/// panic on an invalid name. `MonitorStorage._sanitize_metric_name` maps
/// space/dash/dot to underscore; we additionally strip every other invalid
/// character and guarantee the name does not start with a digit, so a
/// user-derived key (service info, stat key) can never poison the registry.
pub fn sanitize_metric_name(name: &str) -> String {
    let mut out = String::with_capacity(name.len().max(1));
    for (i, b) in name.bytes().enumerate() {
        let ok = if i == 0 {
            b.is_ascii_alphabetic() || b == b'_'
        } else {
            b.is_ascii_alphanumeric() || b == b'_'
        };
        out.push(if ok { b as char } else { '_' });
    }
    if out.is_empty() {
        out.push_str("unknown");
    } else if !(out.as_bytes()[0] == b'_' || out.as_bytes()[0].is_ascii_alphabetic()) {
        out.insert(0, '_');
    }
    out.to_lowercase()
}

/// Sanitize a Prometheus label name so `GaugeVec::new`/`CounterVec::new` can
/// never panic on user-derived labels (e.g. an empty gRPC method name after
/// `grpc_method_name("/")`). Prometheus label names must match
/// `[a-zA-Z_][a-zA-Z0-9_]*`; invalid characters are replaced with `_` and an
/// empty result falls back to `unknown`.
pub fn sanitize_label_name(name: &str) -> String {
    let mut out = String::with_capacity(name.len().max(1));
    for (i, b) in name.bytes().enumerate() {
        let ok = if i == 0 {
            b.is_ascii_alphabetic() || b == b'_'
        } else {
            b.is_ascii_alphanumeric() || b == b'_'
        };
        out.push(if ok { b as char } else { '_' });
    }
    if out.is_empty() {
        out.push_str("unknown");
    } else if !(out.as_bytes()[0] == b'_' || out.as_bytes()[0].is_ascii_alphabetic()) {
        out.insert(0, '_');
    }
    out
}

/// Split a Python-style stat key `name___label__value___label2__value2`.
/// The first part is the metric name; the remaining parts are `label__value` pairs.
pub fn parse_stat_key(key: &str) -> (String, Vec<(String, String)>) {
    let (name, rest) = key.split_once("___").unwrap_or((key, ""));
    let mut labels: Vec<(String, String)> = Vec::new();
    for label_part in rest.split("___").filter(|s| !s.is_empty()) {
        if let Some((label_name, label_value)) = label_part.split_once("__") {
            let label_name = sanitize_label_name(label_name);
            // Duplicate label names (e.g. "a-b" and "a b" both -> "a_b")
            // make `*Vec::new` panic; keep the first occurrence only so the
            // label-name/label-value counts stay aligned.
            if !labels.iter().any(|(n, _)| n == &label_name) {
                labels.push((label_name, label_value.to_string()));
            }
        }
    }
    (name.to_string(), labels)
}

/// Build label names for a metric from extra labels.
pub fn label_names(extra: &[&str]) -> Vec<String> {
    extra.iter().map(|s| sanitize_label_name(s)).collect()
}

/// Build label values for a metric from extra label values.
pub fn label_values(extra: &[&str]) -> Vec<String> {
    extra.iter().map(|s| s.to_string()).collect()
}

/// Build owned label values for a metric from parsed label pairs.
pub fn custom_gauge_values(labels: &[(String, String)]) -> Vec<String> {
    label_values(&labels.iter().map(|(_, v)| v.as_str()).collect::<Vec<_>>())
}

/// Prometheus registry with lazily created metrics keyed by metric name.
#[derive(Clone)]
pub struct MetricsRegistry {
    registry: Arc<Registry>,
    counter_vecs: Arc<Mutex<HashMap<String, CounterVec>>>,
    gauge_vecs: Arc<Mutex<HashMap<String, GaugeVec>>>,
    histogram_vecs: Arc<Mutex<HashMap<String, HistogramVec>>>,
}

impl MetricsRegistry {
    /// Create a new registry.
    pub fn new() -> Self {
        Self {
            registry: Arc::new(Registry::new()),
            counter_vecs: Arc::new(Mutex::new(HashMap::new())),
            gauge_vecs: Arc::new(Mutex::new(HashMap::new())),
            histogram_vecs: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl Default for MetricsRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricsRegistry {

    pub fn registry(&self) -> Arc<Registry> {
        self.registry.clone()
    }

    /// Export all metrics in Prometheus text format.
    pub fn export(&self) -> Vec<u8> {
        let encoder = TextEncoder::new();
        let mut buffer = Vec::new();
        if let Err(e) = encoder.encode(&self.registry.gather(), &mut buffer) {
            tracing::error!("failed to encode metrics: {e}");
        }
        buffer
    }

    /// Get or create a `CounterVec` (counter with the given labels).
    pub fn counter_vec(&self, name: &str, help: &str, extra_labels: &[&str]) -> CounterVec {
        // Metric names must be unique; cache and register by the *sanitized*
        // name so a raw name ("time full") and a style variant ("time-full")
        // that normalize to the same metric cannot double-register and panic.
        let key = sanitize_metric_name(name);
        let mut cache = self.counter_vecs.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(c) = cache.get(&key) {
            return c.clone();
        }
        let opts = Opts::new(&key, help);
        let label_names_owned = label_names(extra_labels);
        let label_refs: Vec<&str> = label_names_owned.iter().map(|s| s.as_str()).collect();
        let vec = CounterVec::new(opts, &label_refs).unwrap();
        if let Err(e) = self.registry.register(Box::new(vec.clone())) {
            if let Some(existing) = cache.get(&key) {
                return existing.clone();
            }
            tracing::error!("failed to register counter {key}: {e}");
        }
        cache.insert(key, vec.clone());
        vec
    }

    /// Get or create a `GaugeVec` (gauge with the given labels).
    pub fn gauge_vec(&self, name: &str, help: &str, extra_labels: &[&str]) -> GaugeVec {
        // Cache and register by the sanitized name (see `counter_vec`).
        let key = sanitize_metric_name(name);
        let mut cache = self.gauge_vecs.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(g) = cache.get(&key) {
            return g.clone();
        }
        let opts = Opts::new(&key, help);
        let label_names_owned = label_names(extra_labels);
        let label_refs: Vec<&str> = label_names_owned.iter().map(|s| s.as_str()).collect();
        let vec = GaugeVec::new(opts, &label_refs).unwrap();
        if let Err(e) = self.registry.register(Box::new(vec.clone())) {
            if let Some(existing) = cache.get(&key) {
                return existing.clone();
            }
            tracing::error!("failed to register gauge {key}: {e}");
        }
        cache.insert(key, vec.clone());
        vec
    }

    /// Get or create a `HistogramVec` with the given bucket type.
    pub fn histogram_vec(
        &self,
        name: &str,
        help: &str,
        extra_labels: &[&str],
        bucket_type: BucketType,
    ) -> HistogramVec {
        // Cache and register by the sanitized name (see `counter_vec`).
        let key = sanitize_metric_name(name);
        let mut cache = self
            .histogram_vecs
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if let Some(h) = cache.get(&key) {
            return h.clone();
        }
        let opts = HistogramOpts::new(&key, help).buckets(bucket_type.buckets().to_vec());
        let label_names_owned = label_names(extra_labels);
        let label_refs: Vec<&str> = label_names_owned.iter().map(|s| s.as_str()).collect();
        let vec = HistogramVec::new(opts, &label_refs).unwrap();
        if let Err(e) = self.registry.register(Box::new(vec.clone())) {
            if let Some(existing) = cache.get(&key) {
                return existing.clone();
            }
            tracing::error!("failed to register histogram {key}: {e}");
        }
        cache.insert(key, vec.clone());
        vec
    }

    /// Set a gauge value (single label-less series).
    pub fn set_gauge(&self, name: &str, help: &str, value: f64) {
        self.gauge_vec(name, help, &[])
            .with_label_values(&[])
            .set(value);
    }

    /// Increment a counter (single label-less series).
    pub fn inc_counter(&self, name: &str, help: &str, value: f64) {
        self.counter_vec(name, help, &[])
            .with_label_values(&[])
            .inc_by(value);
    }

    /// Gauge value getter for the label-less series (used by refresh).
    pub fn gauge_value(&self, name: &str) -> Option<f64> {
        let cache = self.gauge_vecs.lock().unwrap_or_else(|e| e.into_inner());
        let vec = cache.get(name)?;
        Some(vec.with_label_values(&[]).get())
    }

    /// Counter value getter for the label-less series (used by refresh).
    pub fn counter_value(&self, name: &str) -> Option<f64> {
        let cache = self.counter_vecs.lock().unwrap_or_else(|e| e.into_inner());
        let vec = cache.get(name)?;
        Some(vec.with_label_values(&[]).get())
    }

    /// Get a `stats_<key>` gauge, creating it lazily with the parsed labels.
    pub fn custom_gauge(&self, metric_name: &str, labels: &[(String, String)]) -> GaugeVec {
        let extra: Vec<&str> = labels.iter().map(|(n, _)| n.as_str()).collect();
        self.gauge_vec(
            metric_name,
            &format!("This is metric `{metric_name}` from ServiceKit."),
            &extra,
        )
    }

    /// Get a `stats_hist_<key>` histogram, creating it lazily with parsed labels.
    pub fn custom_histogram(
        &self,
        metric_name: &str,
        labels: &[(String, String)],
        bucket_type: BucketType,
    ) -> HistogramVec {
        let extra: Vec<&str> = labels.iter().map(|(n, _)| n.as_str()).collect();
        self.histogram_vec(
            metric_name,
            &format!("This is hist metric `{metric_name}` in stats from ServiceKit."),
            &extra,
            bucket_type,
        )
    }
}

/// EMA alphas matching Python `_STATS_EMA_ALPHA_LONG` / `_STATS_EMA_ALPHA_SHORT`.
pub const EMA_ALPHA_LONG: f64 = 2.0 / 101.0;
pub const EMA_ALPHA_SHORT: f64 = 2.0 / 6.0;

/// Exponential moving average store with long/short windows (Python semantics:
/// first value initializes the series; subsequent values blend with alpha).
#[derive(Debug, Default, Clone)]
pub struct EmaStore {
    long: HashMap<String, f64>,
    short: HashMap<String, f64>,
}

impl EmaStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Update both EMA series with a new observation.
    pub fn update(&mut self, key: &str, value: f64) {
        ema_update(&mut self.long, &mut self.short, key, value);
    }

    pub fn long(&self) -> &HashMap<String, f64> {
        &self.long
    }

    pub fn short(&self) -> &HashMap<String, f64> {
        &self.short
    }
}

/// Update long and short EMA series (Python `_update_ema` applied twice).
pub fn ema_update(
    long: &mut HashMap<String, f64>,
    short: &mut HashMap<String, f64>,
    key: &str,
    value: f64,
) {
    let entry = long.entry(key.to_string()).or_insert(value);
    *entry = EMA_ALPHA_LONG * value + (1.0 - EMA_ALPHA_LONG) * *entry;
    let entry = short.entry(key.to_string()).or_insert(value);
    *entry = EMA_ALPHA_SHORT * value + (1.0 - EMA_ALPHA_SHORT) * *entry;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buckets_match_python() {
        // Python: 47 numeric upper bounds + "INF" (48 entries); size 55 + INF.
        assert_eq!(TIME_BUCKETS.len(), 47);
        assert_eq!(SIZE_BUCKETS.len(), 55);
        assert_eq!(TIME_BUCKETS.last().copied(), Some(3600.0));
        assert_eq!(SIZE_BUCKETS.last().copied(), Some(2_000_000_000.0));
        assert_eq!(TIME_BUCKETS[0], 0.0);
        assert_eq!(TIME_BUCKETS[46], 3600.0);
        assert_eq!(SIZE_BUCKETS[0], 0.0);
        assert_eq!(SIZE_BUCKETS[54], 2_000_000_000.0);
    }

    #[test]
    fn sanitize() {
        assert_eq!(sanitize_metric_name("time full"), "time_full");
        assert_eq!(sanitize_metric_name("Request-Size"), "request_size");
        assert_eq!(sanitize_metric_name("a.b.c"), "a_b_c");
        // Must never panic `GaugeVec::new`/`CounterVec::new` (invalid names).
        assert_eq!(sanitize_metric_name("123"), "_23");
        assert_eq!(sanitize_metric_name("a/b:c"), "a_b_c");
        assert_eq!(sanitize_metric_name("м"), "__");
        assert_eq!(sanitize_metric_name(""), "unknown");
    }

    #[test]
    fn sanitize_label_names() {
        assert_eq!(sanitize_label_name("method"), "method");
        assert_eq!(sanitize_label_name(""), "unknown");
        assert_eq!(sanitize_label_name("1status"), "_status");
        assert_eq!(sanitize_label_name("stat.us"), "stat_us");
        assert_eq!(sanitize_label_name("мethod"), "__ethod");
    }

    #[test]
    fn parse_stat_key_empty_method_label() {
        // Regression: `grpc_method_name("/")` used to return "", which made
        // `processed___method__{method}___status__<status>` render as
        // `processed___method_____status__success`. The orphaned `__status__...`
        // segment parsed to an empty label name and panicked GaugeVec::new
        // (metrics.rs:230), poisoning the registry and taking all metrics down.
        let (name, labels) = parse_stat_key("processed___method_____status__success");
        assert_eq!(name, "processed");
        assert_eq!(
            labels,
            vec![("unknown".to_string(), "status__success".to_string())]
        );
        // With the fallback the happy path keeps method+status labels intact.
        let (name, labels) = parse_stat_key("processed___method__unknown___status__success");
        assert_eq!(name, "processed");
        assert_eq!(
            labels,
            vec![
                ("method".to_string(), "unknown".to_string()),
                ("status".to_string(), "success".to_string())
            ]
        );
    }

    #[test]
    fn parse_stat_key_labels() {
        let (name, labels) = parse_stat_key("request___method__foo");
        assert_eq!(name, "request");
        assert_eq!(labels, vec![("method".to_string(), "foo".to_string())]);

        let (name, labels) = parse_stat_key("processed___method__foo___status__success");
        assert_eq!(name, "processed");
        assert_eq!(
            labels,
            vec![
                ("method".to_string(), "foo".to_string()),
                ("status".to_string(), "success".to_string())
            ]
        );
    }

    #[test]
    fn parse_stat_key_dedupes_label_names() {
        // Regression: "a-b" and "a b" both sanitize to "a_b"; duplicates
        // would make `GaugeVec::new`/`CounterVec::new` panic (duplicate label
        // name), poisoning the registry. Only the first occurrence is kept.
        let (name, labels) = parse_stat_key("req___a-b__1___a b__2");
        assert_eq!(name, "req");
        assert_eq!(labels, vec![("a_b".to_string(), "1".to_string())]);
    }

    #[test]
    fn ema_formula_matches_python() {
        let mut ema = EmaStore::new();
        ema.update("time full___method__foo", 10.0);
        ema.update("time full___method__foo", 20.0);
        let expected_long = EMA_ALPHA_LONG * 20.0 + (1.0 - EMA_ALPHA_LONG) * 10.0;
        let expected_short = EMA_ALPHA_SHORT * 20.0 + (1.0 - EMA_ALPHA_SHORT) * 10.0;
        assert!((ema.long().get("time full___method__foo").unwrap() - expected_long).abs() < 1e-12);
        assert!(
            (ema.short().get("time full___method__foo").unwrap() - expected_short).abs() < 1e-12
        );
    }
}
