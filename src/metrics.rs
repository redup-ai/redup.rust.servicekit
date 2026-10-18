//! Prometheus metric registry, bucket constants and EMA store.
//!
//! Metric names, label names, label ordering and histogram buckets are
//! semantically identical to `redup.python.servicekit`
//! (`redup_servicekit.metrics` + `MonitorStorage`). All metrics carry the
//! `inittime` label (process start time string), `inittime` first.
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
/// `MonitorStorage._sanitize_metric_name` (space/dash/dot -> underscore, lowercase).
pub fn sanitize_metric_name(name: &str) -> String {
    name.chars()
        .map(|c| match c {
            ' ' | '-' | '.' => '_',
            _ => c,
        })
        .collect::<String>()
        .to_lowercase()
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
    let mut labels = Vec::new();
    for label_part in rest.split("___").filter(|s| !s.is_empty()) {
        if let Some((label_name, label_value)) = label_part.split_once("__") {
            labels.push((sanitize_label_name(label_name), label_value.to_string()));
        }
    }
    (name.to_string(), labels)
}

/// Build label names for a metric: `inittime` first, then extra labels.
pub fn label_names(extra: &[&str]) -> Vec<String> {
    let mut names = Vec::with_capacity(extra.len() + 1);
    names.push("inittime".to_string());
    names.extend(extra.iter().map(|s| sanitize_label_name(s)));
    names
}

/// Build label values for a metric: `inittime` first, then extra values.
pub fn label_values(inittime: &str, extra: &[&str]) -> Vec<String> {
    let mut values = Vec::with_capacity(extra.len() + 1);
    values.push(inittime.to_string());
    values.extend(extra.iter().map(|s| s.to_string()));
    values
}

/// Build owned label values for a metric from parsed label pairs.
pub fn custom_gauge_values(inittime: &str, labels: &[(String, String)]) -> Vec<String> {
    label_values(
        inittime,
        &labels.iter().map(|(_, v)| v.as_str()).collect::<Vec<_>>(),
    )
}

/// Prometheus registry with lazily created metrics keyed by metric name.
/// Every metric is label-vector based and always carries `inittime`.
#[derive(Clone)]
pub struct MetricsRegistry {
    registry: Arc<Registry>,
    inittime: String,
    counter_vecs: Arc<Mutex<HashMap<String, CounterVec>>>,
    gauge_vecs: Arc<Mutex<HashMap<String, GaugeVec>>>,
    histogram_vecs: Arc<Mutex<HashMap<String, HistogramVec>>>,
}

impl MetricsRegistry {
    /// Create a registry with the given `inittime` label value.
    pub fn new(inittime: String) -> Self {
        Self {
            registry: Arc::new(Registry::new()),
            inittime,
            counter_vecs: Arc::new(Mutex::new(HashMap::new())),
            gauge_vecs: Arc::new(Mutex::new(HashMap::new())),
            histogram_vecs: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn inittime(&self) -> &str {
        &self.inittime
    }

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

    /// Get or create a `CounterVec` (counter with `inittime` label).
    pub fn counter_vec(&self, name: &str, help: &str, extra_labels: &[&str]) -> CounterVec {
        let mut cache = self.counter_vecs.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(c) = cache.get(name) {
            return c.clone();
        }
        let opts = Opts::new(name, help);
        let label_names_owned = label_names(extra_labels);
        let label_refs: Vec<&str> = label_names_owned.iter().map(|s| s.as_str()).collect();
        let vec = CounterVec::new(opts, &label_refs).unwrap();
        self.registry
            .register(Box::new(vec.clone()))
            .expect("counter vec already registered");
        cache.insert(name.to_string(), vec.clone());
        vec
    }

    /// Get or create a `GaugeVec` (gauge with `inittime` label).
    pub fn gauge_vec(&self, name: &str, help: &str, extra_labels: &[&str]) -> GaugeVec {
        let mut cache = self.gauge_vecs.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(g) = cache.get(name) {
            return g.clone();
        }
        let opts = Opts::new(name, help);
        let label_names_owned = label_names(extra_labels);
        let label_refs: Vec<&str> = label_names_owned.iter().map(|s| s.as_str()).collect();
        let vec = GaugeVec::new(opts, &label_refs).unwrap();
        self.registry
            .register(Box::new(vec.clone()))
            .expect("gauge vec already registered");
        cache.insert(name.to_string(), vec.clone());
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
        let mut cache = self
            .histogram_vecs
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if let Some(h) = cache.get(name) {
            return h.clone();
        }
        let opts = HistogramOpts::new(name, help).buckets(bucket_type.buckets().to_vec());
        let label_names_owned = label_names(extra_labels);
        let label_refs: Vec<&str> = label_names_owned.iter().map(|s| s.as_str()).collect();
        let vec = HistogramVec::new(opts, &label_refs).unwrap();
        self.registry
            .register(Box::new(vec.clone()))
            .expect("histogram vec already registered");
        cache.insert(name.to_string(), vec.clone());
        vec
    }

    /// Set a gauge value (single `inittime` label).
    pub fn set_gauge(&self, name: &str, help: &str, value: f64) {
        self.gauge_vec(name, help, &[])
            .with_label_values(&[self.inittime.as_str()])
            .set(value);
    }

    /// Increment a counter (single `inittime` label).
    pub fn inc_counter(&self, name: &str, help: &str, value: f64) {
        self.counter_vec(name, help, &[])
            .with_label_values(&[self.inittime.as_str()])
            .inc_by(value);
    }

    /// Gauge value getter for the `inittime`-only series (used by refresh).
    pub fn gauge_value(&self, name: &str) -> Option<f64> {
        let cache = self.gauge_vecs.lock().unwrap_or_else(|e| e.into_inner());
        let vec = cache.get(name)?;
        Some(vec.with_label_values(&[self.inittime.as_str()]).get())
    }

    /// Counter value getter for the `inittime`-only series (used by refresh).
    pub fn counter_value(&self, name: &str) -> Option<f64> {
        let cache = self.counter_vecs.lock().unwrap_or_else(|e| e.into_inner());
        let vec = cache.get(name)?;
        Some(vec.with_label_values(&[self.inittime.as_str()]).get())
    }

    /// Get a `stats_<key>` gauge, creating it lazily with the parsed labels.
    pub fn custom_gauge(&self, metric_name: &str, labels: &[(String, String)]) -> GaugeVec {
        let extra: Vec<&str> = labels.iter().map(|(n, _)| n.as_str()).collect();
        let vec = self.gauge_vec(
            metric_name,
            &format!("This is metric `{metric_name}` from ServiceKit."),
            &extra,
        );
        let values = custom_gauge_values(&self.inittime, labels);
        let value_refs: Vec<&str> = values.iter().map(|s| s.as_str()).collect();
        vec.with_label_values(&value_refs);
        vec
    }

    /// Get a `stats_hist_<key>` histogram, creating it lazily with parsed labels.
    pub fn custom_histogram(
        &self,
        metric_name: &str,
        labels: &[(String, String)],
        bucket_type: BucketType,
    ) -> HistogramVec {
        let extra: Vec<&str> = labels.iter().map(|(n, _)| n.as_str()).collect();
        let vec = self.histogram_vec(
            metric_name,
            &format!("This is hist metric `{metric_name}` in stats from ServiceKit."),
            &extra,
            bucket_type,
        );
        let values = custom_gauge_values(&self.inittime, labels);
        let value_refs: Vec<&str> = values.iter().map(|s| s.as_str()).collect();
        vec.with_label_values(&value_refs);
        vec
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
