//! Monitoring core: task tracking, statistics and the main `ServiceKit`.
//!
//! Semantically identical to `redup.python.servicekit.monitoring`
//! (`MonitorStorage` + `MonitorServer`): stats are accumulated in memory and
//! the Prometheus registry is refreshed lazily on each `/metrics` request
//! (`refresh()`), exactly like `refresh_registry_for_metrics()`.

use crate::config::{now_inittime, MonitorServerConfig};
use crate::metrics::{
    bucket_type_for_key, custom_gauge_values, parse_stat_key, sanitize_metric_name, MetricsRegistry,
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Distinct error type strings from `MonitorServer.errors` config (`service_errors`),
/// used to classify a backend failure into `stats_errors{method,type}`. The string
/// `"other"` is excluded because it is the default bucket, mirroring Python
/// `ErrorParser._types`.
fn collect_error_types(monitor: &MonitorServerConfig) -> Vec<String> {
    monitor
        .resolved_errors()
        .values()
        .flat_map(|e| e.service_errors.iter().cloned())
        .filter(|t| !t.is_empty() && t != "other")
        .collect()
}

/// Task outcome status (Python `TaskStatus`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskStatus {
    Ok = 1,
    Fatal = 2,
    ConnectionError = 3,
    OtherException = -1,
    Internal = 4,
    Unknown = 5,
}

impl TaskStatus {
    pub fn name(&self) -> &'static str {
        match self {
            TaskStatus::Ok => "OK",
            TaskStatus::Fatal => "FATAL",
            TaskStatus::ConnectionError => "CONNECTION_ERROR",
            TaskStatus::OtherException => "OTHER_EXCEPTION",
            TaskStatus::Internal => "INTERNAL",
            TaskStatus::Unknown => "UNKNOWN",
        }
    }
}

/// Internal statistics storage (analog of `MonitorStorage._stats`).
#[derive(Debug, Default)]
struct Stats {
    /// Flat numeric stats (Python `_stats` counters), keyed by
    /// `name___label__value...` strings.
    counters: HashMap<String, f64>,
    /// EMA long series (Python `stats_ema_long`).
    ema_long: HashMap<String, f64>,
    /// EMA short series (Python `stats_ema_short`).
    ema_short: HashMap<String, f64>,
    /// In-flight tasks: id -> start instant.
    tasks: HashMap<String, Instant>,
    /// Health status series: task_type -> (status name, series number).
    statuses: HashMap<String, (String, u64)>,
}

/// Statistics store shared between task tracker and service kit.
#[derive(Default)]
struct StatsStore {
    inner: Mutex<Stats>,
    task_id_seq: AtomicU64,
}

impl StatsStore {
    fn new() -> Self {
        Self::default()
    }

    fn next_task_id(&self) -> String {
        format!(
            "task-{}-{}",
            std::process::id(),
            self.task_id_seq.fetch_add(1, Ordering::Relaxed)
        )
    }

    fn inc(&self, key: &str, count: f64) {
        let mut stats = self.inner.lock().unwrap();
        *stats.counters.entry(key.to_string()).or_insert(0.0) += count;
    }

    fn set(&self, key: &str, value: f64) {
        let mut stats = self.inner.lock().unwrap();
        stats.counters.insert(key.to_string(), value);
    }

    /// Insert a stat unless the key already exists (idempotent init).
    fn ensure(&self, key: &str, value: f64) {
        let mut stats = self.inner.lock().unwrap();
        stats.counters.entry(key.to_string()).or_insert(value);
    }

    fn observe(&self, key: &str, value: f64) {
        let mut stats = self.inner.lock().unwrap();
        // Split borrows: take maps out to avoid double mutable borrow.
        let mut ema_long = std::mem::take(&mut stats.ema_long);
        let mut ema_short = std::mem::take(&mut stats.ema_short);
        crate::metrics::ema_update(&mut ema_long, &mut ema_short, key, value);
        stats.ema_long = ema_long;
        stats.ema_short = ema_short;
    }

    fn add_task(&self, id: String, start: Instant) {
        let mut stats = self.inner.lock().unwrap();
        stats.tasks.insert(id, start);
    }

    fn remove_task(&self, id: &str) -> Option<Duration> {
        let mut stats = self.inner.lock().unwrap();
        let start = stats.tasks.remove(id)?;
        Some(start.elapsed())
    }
}

/// Task tracker for recording in-flight tasks (analog of the `tasks` section
/// in `MonitorStorage` plus `started_total`/`completed_total`/quantile metrics).
#[derive(Clone)]
pub struct TaskTracker {
    store: Arc<StatsStore>,
    registry: MetricsRegistry,
}

impl TaskTracker {
    /// Begin a task: increments `started_total` and registers the in-flight task.
    pub fn start(&self) -> String {
        let id = self.store.next_task_id();
        self.store.add_task(id.clone(), Instant::now());
        self.registry
            .inc_counter("started_total", "Total number of started tasks.", 1.0);
        id
    }

    /// Finish a task: removes it, increments `completed_total`,
    /// `stats_tasks_time_spend_total` and observes the quantile histogram.
    pub fn finish(&self, id: &str) -> Option<Duration> {
        let duration = self.store.remove_task(id)?;
        self.registry
            .inc_counter("completed_total", "Total number of completed tasks.", 1.0);
        self.registry.inc_counter(
            "stats_tasks_time_spend_total",
            "Total time spent on completed tasks.",
            duration.as_secs_f64(),
        );
        let hist = self.registry.histogram_vec(
            "tasks_time_spent_quantile",
            "Time spent on completing tasks.",
            &[],
            crate::metrics::BucketType::Time,
        );
        hist.with_label_values(&[self.registry.inittime()])
            .observe(duration.as_secs_f64());
        Some(duration)
    }

    /// Remove a task without metrics (e.g. on cancellation).
    pub fn discard(&self, id: &str) {
        self.store.remove_task(id);
    }

    /// Number of in-flight tasks.
    pub fn in_flight(&self) -> usize {
        self.store.inner.lock().unwrap().tasks.len()
    }
}

/// Health readiness probe abstraction used by the HTTP `/readyz` endpoint.
pub trait ReadyProbe: Send + Sync {
    fn is_ready(&self) -> bool;
}

impl ReadyProbe for std::sync::atomic::AtomicBool {
    fn is_ready(&self) -> bool {
        self.load(Ordering::Relaxed)
    }
}

impl ReadyProbe for Arc<std::sync::atomic::AtomicBool> {
    fn is_ready(&self) -> bool {
        self.load(Ordering::Relaxed)
    }
}

/// Main entry point (analog of `MonitorServer`). Holds the metrics registry,
/// statistics and config; refresh is called on `/metrics`.
pub struct ServiceKit {
    registry: MetricsRegistry,
    monitor: MonitorServerConfig,
    store: Arc<StatsStore>,
    service_threads: u32,
    task_tracker: TaskTracker,
    ready_probe: Mutex<Option<Arc<dyn ReadyProbe>>>,
    /// Distinct `service_errors` strings from config (minus `"other"`), for
    /// classifying a backend failure into `stats_errors{method,type}`.
    error_types: Vec<String>,
}

impl ServiceKit {
    /// Create a `ServiceKit` (analog of `MonitorServer().run(...)`).
    ///
    /// `hpa_max_workers` is the target HPA pool size used to normalize the
    /// task time stat; falls back to `max_workers` when `None`
    /// (Python: `hpa_max_workers if hpa_max_workers is not None else max_workers`).
    pub fn new(
        monitor: MonitorServerConfig,
        max_workers: u32,
        hpa_max_workers: Option<u32>,
    ) -> Arc<Self> {
        let registry = MetricsRegistry::new(now_inittime());
        let store = Arc::new(StatsStore::new());
        let service_threads = hpa_max_workers.unwrap_or(max_workers);
        let error_types = collect_error_types(&monitor);

        let kit = Arc::new(Self {
            registry: registry.clone(),
            monitor,
            store: store.clone(),
            service_threads,
            task_tracker: TaskTracker {
                store: store.clone(),
                registry: registry.clone(),
            },
            ready_probe: Mutex::new(None),
            error_types,
        });
        kit.init_metrics();
        kit
    }

    /// Initialize the fixed metric set (analog of `MonitorServer._init_metrics`).
    fn init_metrics(&self) {
        let r = &self.registry;
        // Counters (names already include `_total` for wire compatibility).
        r.counter_vec("started_total", "Total number of started tasks.", &[]);
        r.counter_vec("completed_total", "Total number of completed tasks.", &[]);
        r.counter_vec(
            "stats_tasks_time_spend_total",
            "Total time spent on completed tasks.",
            &[],
        );
        // Gauges.
        r.gauge_vec(
            "tasks_count",
            "The total number of tasks currently being processed.",
            &[],
        );
        r.gauge_vec(
            "current_tasks_time_spend",
            "Total time spent processing current tasks.",
            &[],
        );
        r.gauge_vec(
            "current_tasks_max_time_spend",
            "Maximum processing time for current tasks.",
            &[],
        );
        r.gauge_vec(
            "stats_tasks_with_current_time_spend_total",
            "Total time spent processing current and previous tasks.",
            &[],
        );
        r.gauge_vec(
            "stats_tasks_with_current_time_spend_normalised_total",
            "Total time spent on current and completed tasks per configured worker thread.",
            &[],
        );
        r.gauge_vec("service_threads", "Number of service threads.", &[]);
        // Histogram (existing in metrics contract; values appear on first finish).
        r.histogram_vec(
            "tasks_time_spent_quantile",
            "Time spent on completing tasks.",
            &[],
            crate::metrics::BucketType::Time,
        );
        // Initial zero values (Python `.labels(...).inc(0)` / `.set(...)`).
        let inittime = r.inittime();
        r.gauge_vec("tasks_count", "", &[])
            .with_label_values(&[inittime])
            .set(0.0);
        r.gauge_vec("current_tasks_time_spend", "", &[])
            .with_label_values(&[inittime])
            .set(0.0);
        r.gauge_vec("current_tasks_max_time_spend", "", &[])
            .with_label_values(&[inittime])
            .set(0.0);
        r.gauge_vec("stats_tasks_with_current_time_spend_total", "", &[])
            .with_label_values(&[inittime])
            .set(0.0);
        r.gauge_vec(
            "stats_tasks_with_current_time_spend_normalised_total",
            "",
            &[],
        )
        .with_label_values(&[inittime])
        .set(0.0);
        r.gauge_vec("service_threads", "", &[])
            .with_label_values(&[inittime])
            .set(self.service_threads as f64);
        // Service info gauges (Python adds `service_threads` itself).
        for (key, entry) in self
            .monitor
            .resolved_service_info(self.service_threads as f64)
        {
            let name = sanitize_metric_name(&key);
            r.gauge_vec(&name, &entry.info, &[])
                .with_label_values(&[inittime])
                .set(entry.value);
        }
        // Pre-create per-method stats are done by services via `init_method`.
    }

    /// Current `inittime` value.
    pub fn inittime(&self) -> &str {
        self.registry.inittime()
    }

    /// Metrics registry (export access).
    pub fn registry(&self) -> &MetricsRegistry {
        &self.registry
    }

    /// Configure the readiness probe used by `/readyz`.
    pub fn set_ready_probe(&self, probe: Arc<dyn ReadyProbe>) {
        *self.ready_probe.lock().unwrap() = Some(probe);
    }

    /// Current ready state (default false).
    pub fn is_ready(&self) -> bool {
        self.ready_probe
            .lock()
            .unwrap()
            .as_ref()
            .map(|p| p.is_ready())
            .unwrap_or(false)
    }

    /// Increment a stat counter, e.g. `request___method__Foo`.
    pub fn inc_stat(&self, key: &str, count: f64) {
        self.store.inc(key, count);
    }

    /// Set a stat value (initialized gauges for known methods).
    pub fn set_stat(&self, key: &str, value: f64) {
        self.store.set(key, value);
    }

    /// Initialize a stat counter only if it does not exist yet.
    pub fn ensure_stat(&self, key: &str, value: f64) {
        self.store.ensure(key, value);
    }

    /// Pre-register per-method stats (analog of `grpc_init_wrapper`):
    /// `request___method__M`, `processed___method__M___status__success|failure`,
    /// `errors___method__M___type__<t>` for every configured error type.
    pub fn init_method(&self, method: &str) {
        self.ensure_stat(&format!("request___method__{method}"), 0.0);
        self.ensure_stat(
            &format!("processed___method__{method}___status__success"),
            0.0,
        );
        self.ensure_stat(
            &format!("processed___method__{method}___status__failure"),
            0.0,
        );
        self.ensure_stat(&format!("errors___method__{method}___type__other"), 0.0);
        for error_type in &self.error_types {
            self.ensure_stat(
                &format!("errors___method__{method}___type__{error_type}"),
                0.0,
            );
        }
    }

    /// Classify a backend error message/text into an `stats_errors{type}` label.
    ///
    /// Mirrors Python `ErrorParser.parse`: any configured `service_errors`
    /// string (case-insensitive substring) found in the text wins; otherwise
    /// `"other"`. `"other"` itself is never a configured match, so it is the
    /// fallback bucket.
    pub fn classify_error(&self, text: &str) -> String {
        let lower = text.to_lowercase();
        if self.error_types.is_empty() {
            return "other".to_string();
        }
        // Prefer the longlisted (first configured) match for determinism; exact
        // substring scan over the configured set.
        for error_type in &self.error_types {
            if lower.contains(&error_type.to_lowercase()) {
                return error_type.clone();
            }
        }
        "other".to_string()
    }

    /// Observe a metric value: updates EMA long/short and, for keys with
    /// `time`/`size` substrings, records a `stats_hist_*` observation.
    pub fn observe_stat(&self, key: &str, value: f64) {
        self.store.observe(key, value);
        if let Some(bucket_type) = bucket_type_for_key(key) {
            let (metric_name, labels) = parse_stat_key(key);
            let hist_name = format!("stats_hist_{}", sanitize_metric_name(&metric_name));
            let hist = self
                .registry
                .custom_histogram(&hist_name, &labels, bucket_type);
            let values = custom_gauge_values(self.registry.inittime(), &labels);
            let value_refs: Vec<&str> = values.iter().map(|s| s.as_str()).collect();
            hist.with_label_values(&value_refs).observe(value);
        }
    }

    /// Task tracker for `start`/`finish` (RPC calls).
    pub fn task_tracker(&self) -> &TaskTracker {
        &self.task_tracker
    }

    /// Set a task status for health series (Python `set_task_status`).
    pub fn set_task_status(&self, status: TaskStatus, task_type: &str) {
        let mut stats = self.store.inner.lock().unwrap();
        let (old_status, old_series) = stats
            .statuses
            .get(task_type)
            .cloned()
            .unwrap_or_else(|| (TaskStatus::Ok.name().to_string(), 0));
        let (new_status, new_series) = if old_status == status.name() {
            (old_status, old_series + 1)
        } else {
            (status.name().to_string(), 1)
        };
        stats
            .statuses
            .insert(task_type.to_string(), (new_status.clone(), new_series));
        stats
            .counters
            .insert(format!("series.{task_type}"), new_series as f64);
        tracing::debug!(task_type, status = %new_status, series = new_series, "task status updated");
    }

    /// Refresh the Prometheus registry from current stats (called on `/metrics`).
    pub fn refresh(&self) {
        let snapshot = {
            let stats = self.store.inner.lock().unwrap();
            (
                stats.ema_long.clone(),
                stats.ema_short.clone(),
                stats.counters.clone(),
                stats.tasks.clone(),
            )
        };
        let (ema_long, ema_short, counters, tasks) = snapshot;
        let now = Instant::now();

        // EMA long/short -> stats_aggregation_<key> / stats_aggregation_last_<key>.
        for (key, value) in &ema_long {
            self.set_custom_stat(&format!("stats_aggregation_{key}"), *value);
        }
        for (key, value) in &ema_short {
            self.set_custom_stat(&format!("stats_aggregation_last_{key}"), *value);
        }
        // Counters -> stats_<key> (gauge on the wire, as in Python).
        for (key, value) in &counters {
            if key.starts_with("series.") {
                continue;
            }
            self.set_custom_stat(&format!("stats_{key}"), *value);
        }
        // Task series.
        let task_durations: Vec<f64> = tasks
            .values()
            .map(|t| now.duration_since(*t).as_secs_f64())
            .collect();
        let tasks_count = task_durations.len() as f64;
        let sum: f64 = task_durations.iter().sum();
        let max: f64 = task_durations.iter().copied().fold(0.0, f64::max);
        self.registry.set_gauge(
            "tasks_count",
            "The total number of tasks currently being processed.",
            tasks_count,
        );
        self.registry.set_gauge(
            "current_tasks_time_spend",
            "Total time spent processing current tasks.",
            sum,
        );
        self.registry.set_gauge(
            "current_tasks_max_time_spend",
            "Maximum processing time for current tasks.",
            max,
        );

        // Combined spent + current (monotonic gauge), then normalized.
        let spent = self
            .registry
            .counter_value("stats_tasks_time_spend_total")
            .unwrap_or(0.0);
        let combined = sum + spent;
        self.registry.set_gauge(
            "stats_tasks_with_current_time_spend_total",
            "Total time spent processing current and previous tasks.",
            combined,
        );
        let normalised = if self.service_threads > 0 {
            combined / self.service_threads as f64
        } else {
            0.0
        };
        self.registry.set_gauge(
            "stats_tasks_with_current_time_spend_normalised_total",
            "Total time spent on current and completed tasks per configured worker thread.",
            normalised,
        );
        self.registry.set_gauge(
            "service_threads",
            "Number of service threads.",
            self.service_threads as f64,
        );
    }

    fn set_custom_stat(&self, full_key: &str, value: f64) {
        // full_key may be "stats_request___method__Foo" or
        // "stats_aggregation_time full___method__Foo".
        let (metric_name, labels) = parse_stat_key(full_key);
        let sanitized = sanitize_metric_name(&metric_name);
        let vec = self.registry.custom_gauge(&sanitized, &labels);
        let values = custom_gauge_values(self.registry.inittime(), &labels);
        let value_refs: Vec<&str> = values.iter().map(|s| s.as_str()).collect();
        vec.with_label_values(&value_refs).set(value);
    }

    /// Export metrics in Prometheus text format (with a preceding refresh).
    pub fn metrics_text(&self) -> Vec<u8> {
        self.refresh();
        self.registry.export()
    }

    /// Current statuses (health series), analog of `get_statuses`.
    pub fn get_statuses(&self) -> Vec<(String, String, u64)> {
        let stats = self.store.inner.lock().unwrap();
        stats
            .statuses
            .iter()
            .map(|(k, (s, n))| (k.clone(), s.clone(), *n))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ErrorEntry;
    use std::collections::BTreeMap;

    fn kit_with_errors(errors: BTreeMap<String, ErrorEntry>) -> Arc<ServiceKit> {
        let monitor = MonitorServerConfig {
            port: 9999,
            service_info: Default::default(),
            errors,
        };
        ServiceKit::new(monitor, 4, Some(2))
    }

    fn single_error_entry(words: &[&str]) -> BTreeMap<String, ErrorEntry> {
        let mut m = BTreeMap::new();
        m.insert(
            "OTHER_EXCEPTION".to_string(),
            ErrorEntry {
                service_errors: words.iter().map(|s| s.to_string()).collect(),
                unhealthy: -1,
            },
        );
        m
    }

    #[test]
    fn classify_error_defaults_to_other_when_no_config() {
        let kit = ServiceKit::new(MonitorServerConfig::default(), 4, Some(2));
        assert_eq!(kit.classify_error("random backend failure"), "other");
    }

    #[test]
    fn classify_error_matches_configured_service_error() {
        let kit = kit_with_errors(single_error_entry(&["textprocessor", "translation"]));
        assert_eq!(kit.classify_error("textprocessor: boom"), "textprocessor");
        assert_eq!(kit.classify_error("translation: timeout"), "translation");
    }

    #[test]
    fn classify_error_is_case_insensitive() {
        let kit = kit_with_errors(single_error_entry(&["TextProcessor"]));
        assert_eq!(kit.classify_error("TEXTPROCESSOR: boom"), "TextProcessor");
    }

    #[test]
    fn classify_error_falls_back_to_other() {
        let kit = kit_with_errors(single_error_entry(&["textprocessor"]));
        assert_eq!(kit.classify_error("unknown failure"), "other");
    }

    #[test]
    fn other_is_never_a_configured_match() {
        let kit = kit_with_errors(single_error_entry(&["other", "textprocessor"]));
        // "other" must be excluded from matching; a bare "other" in the text
        // falls back to the default bucket.
        assert_eq!(kit.classify_error("other something"), "other");
        assert_eq!(kit.classify_error("textprocessor boom"), "textprocessor");
    }
}
