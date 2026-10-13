//! Structured JSON console logging.
//!
//! Analog of `redup_servicekit.logging.init_console_log` using
//! `tracing-subscriber` JSON output with an env filter. Services may keep
//! their own logging setup; this helper provides the servicekit default.

/// Initialize JSON console logging at the given level (e.g. "info", "debug").
/// Honors `RUST_LOG` when set, otherwise uses `level`.
pub fn init_json_log(level: &str) {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(level));
    let _ = tracing_subscriber::fmt()
        .json()
        .with_env_filter(filter)
        .try_init();
}

/// Parse a log level string into a tracing level (used for validation).
pub fn parse_level(level: &str) -> Option<tracing::Level> {
    match level.to_ascii_lowercase().as_str() {
        "trace" => Some(tracing::Level::TRACE),
        "debug" => Some(tracing::Level::DEBUG),
        "info" => Some(tracing::Level::INFO),
        "warn" => Some(tracing::Level::WARN),
        "error" => Some(tracing::Level::ERROR),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn level_parse() {
        assert!(parse_level("INFO").is_some());
        assert!(parse_level("debug").is_some());
        assert!(parse_level("xyz").is_none());
    }
}
