//! Integration tests for the servicekit: tasks, EMA, config, buckets,
//! gRPC layer and HTTP router end-to-end behavior.

use redup_servicekit::config::MonitorServerConfig;
use redup_servicekit::metrics::{sanitize_metric_name, BucketType};
use redup_servicekit::monitor::{ServiceKit, TaskStatus};
use redup_servicekit::Config;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

fn metrics_text(kit: &ServiceKit) -> String {
    let bytes = kit.metrics_text();
    String::from_utf8_lossy(&bytes).to_string()
}

#[test]
fn tasks_start_finish_metrics() {
    let kit = ServiceKit::new(MonitorServerConfig::default(), 4, Some(2));
    let tracker = kit.task_tracker();
    let id = tracker.start();
    assert_eq!(tracker.in_flight(), 1);
    std::thread::sleep(std::time::Duration::from_millis(20));
    let duration = tracker.finish(&id).unwrap();
    assert!(duration.as_secs_f64() > 0.0);
    assert_eq!(tracker.in_flight(), 0);

    let text = metrics_text(&kit);
    assert!(text.contains("started_total{inittime="));
    assert!(text.contains("completed_total{inittime="));
    assert!(text.contains("stats_tasks_time_spend_total{inittime="));
    assert!(text.contains("stats_tasks_with_current_time_spend_normalised_total{inittime="));
    assert!(text.contains("service_threads{inittime="));
    assert!(text.contains("tasks_time_spent_quantile_bucket"));
}

#[test]
fn custom_stats_and_ema() {
    let kit = ServiceKit::new(MonitorServerConfig::default(), 8, Some(4));
    kit.inc_stat("request___method__Foo", 1.0);
    kit.observe_stat("time full___method__Foo", 1.5);
    let text = metrics_text(&kit);
    assert!(text.contains("stats_request{inittime="));
    assert!(text.contains("method=\"Foo\""));
    assert!(text.contains("stats_aggregation_time_full"));
    assert!(text.contains("stats_aggregation_last_time_full"));
    assert!(text.contains("stats_hist_time_full"));
}

#[test]
fn config_load_and_inject() {
    let dir = std::env::temp_dir().join(format!("skit-config-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("config.yaml");
    std::fs::write(
        &path,
        "service:\n  max_workers: 4\n  hpa_max_workers: 2\nMonitorServer:\n  port: 9999\n  service info:\n    service_version:\n      info: Service version\n      value: 1\n",
    )
    .unwrap();
    let mut cfg = Config::load(&path).unwrap();
    cfg.inject_os_envs();
    let monitor: MonitorServerConfig = cfg.get("MonitorServer").unwrap().unwrap();
    assert_eq!(monitor.port, 9999);
    let max: Option<u32> = cfg.get_path("service.max_workers").unwrap();
    assert_eq!(max, Some(4));
}

#[test]
fn buckets_are_compatible() {
    // TIME_BUCKETS matches Python BUCKETS_HIST["time"] numeric bounds.
    assert_eq!(BucketType::Time.buckets().len(), 47);
    assert_eq!(BucketType::Size.buckets().len(), 55);
    assert_eq!(BucketType::Time.buckets()[0], 0.0);
    assert_eq!(BucketType::Time.buckets()[46], 3600.0);
}

#[test]
fn ready_probe_controls_readyz() {
    let kit = ServiceKit::new(MonitorServerConfig::default(), 4, Some(2));
    assert!(!kit.is_ready());
    let flag = Arc::new(AtomicBool::new(true));
    kit.set_ready_probe(flag.clone());
    assert!(kit.is_ready());
    flag.store(false, Ordering::Relaxed);
    assert!(!kit.is_ready());
    assert_eq!(sanitize_metric_name("time full"), "time_full");
}

#[test]
fn task_status_series() {
    let kit = ServiceKit::new(MonitorServerConfig::default(), 4, Some(2));
    kit.set_task_status(TaskStatus::Ok, "default");
    kit.set_task_status(TaskStatus::OtherException, "default");
    let statuses = kit.get_statuses();
    assert_eq!(statuses.len(), 1);
    assert_eq!(statuses[0].1, "OTHER_EXCEPTION");
}

#[test]
fn metrics_text_refresh_sets_zero_series() {
    let kit = ServiceKit::new(MonitorServerConfig::default(), 4, Some(2));
    let text = metrics_text(&kit);
    assert!(text.contains("current_tasks_time_spend{inittime="));
    assert!(text.contains("current_tasks_max_time_spend{inittime="));
    assert!(text.contains("tasks_count{inittime="));
    assert!(text.contains("service_threads{inittime="));
    assert!(text.contains("stats_tasks_with_current_time_spend_total{inittime="));
}
