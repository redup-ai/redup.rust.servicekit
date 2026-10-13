//! YAML configuration singleton with merge and environment overrides.
//!
//! Mirrors `redup_servicekit.config.ConfigSingleton` from
//! `redup.python.servicekit`: deep merge of additional YAML files or values,
//! environment overrides with `___` (triple underscore) for nesting and
//! `__` (double underscore) for spaces in key names, comma-separated env
//! values become lists. Only keys already present in the config are overridden.

use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

type Map = serde_yaml::Mapping;

/// Error returned by configuration operations.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("configuration not initialized")]
    NotInitialized,
    #[error("config path does not exist: {0}")]
    PathNotFound(String),
    #[error("config path must be a .yaml file: {0}")]
    NotYaml(String),
    #[error("cannot read {path}: {source}")]
    Read {
        path: String,
        source: std::io::Error,
    },
    #[error("cannot parse yaml {path}: {source}")]
    Parse {
        path: String,
        source: serde_yaml::Error,
    },
    #[error("cannot extract key `{key}`: {source}")]
    Extract {
        key: String,
        source: serde_yaml::Error,
    },
    #[error("cannot parse yaml value: {0}")]
    ValueParse(#[from] serde_yaml::Error),
}

fn as_map(value: &serde_yaml::Value) -> Option<&Map> {
    match value {
        serde_yaml::Value::Mapping(map) => Some(map),
        _ => None,
    }
}

fn as_map_mut(value: &mut serde_yaml::Value) -> Option<&mut Map> {
    match value {
        serde_yaml::Value::Mapping(map) => Some(map),
        _ => None,
    }
}

fn key_str(key: &str) -> serde_yaml::Value {
    serde_yaml::Value::String(key.to_string())
}

fn get_key<'a>(map: &'a Map, key: &str) -> Option<&'a serde_yaml::Value> {
    map.get(key_str(key))
}

fn insert_key(map: &mut Map, key: &str, value: serde_yaml::Value) {
    map.insert(key_str(key), value);
}

fn is_mapping(value: &serde_yaml::Value) -> bool {
    matches!(value, serde_yaml::Value::Mapping(_))
}

/// Recursive deep merge: source wins over target.
fn deep_merge(source: &serde_yaml::Value, target: &mut serde_yaml::Value) {
    let (Some(source_map), Some(target_map)) = (as_map(source), as_map_mut(target)) else {
        *target = source.clone();
        return;
    };
    let source_entries: Vec<(serde_yaml::Value, serde_yaml::Value)> = source_map
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    for (key, source_value) in source_entries {
        let existing = target_map.get(&key).cloned();
        match existing {
            Some(mut existing_value)
                if is_mapping(&existing_value) && is_mapping(&source_value) =>
            {
                deep_merge(&source_value, &mut existing_value);
                target_map.insert(key, existing_value);
            }
            _ => {
                target_map.insert(key, source_value.clone());
            }
        }
    }
}

/// Configuration loaded from a YAML file, with merge and env overrides.
#[derive(Debug, Clone, Default)]
pub struct Config {
    data: serde_yaml::Value,
    path: Option<PathBuf>,
}

impl Config {
    /// Load a YAML config file (must be `.yaml`).
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        if !path.exists() {
            return Err(ConfigError::PathNotFound(path.display().to_string()));
        }
        if path.extension().and_then(|e| e.to_str()) != Some("yaml") {
            return Err(ConfigError::NotYaml(path.display().to_string()));
        }
        let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.display().to_string(),
            source,
        })?;
        let value: serde_yaml::Value =
            serde_yaml::from_str(&text).map_err(|source| ConfigError::Parse {
                path: path.display().to_string(),
                source,
            })?;
        Ok(Self {
            data: value,
            path: Some(path.to_path_buf()),
        })
    }

    /// Open an empty config (fill via `merge_value`).
    pub fn new() -> Self {
        Self::default()
    }

    /// Source path (when loaded from a file).
    pub fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    /// Load a config file, or an empty config if the file does not exist.
    pub fn load_or_default(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        if path.exists() {
            Self::load(path)
        } else {
            Ok(Self::default())
        }
    }

    /// Merge another YAML file into the current config (deep merge).
    pub fn merge_path(&mut self, path: impl AsRef<Path>) -> Result<(), ConfigError> {
        let text = std::fs::read_to_string(path.as_ref()).map_err(|source| ConfigError::Read {
            path: path.as_ref().display().to_string(),
            source,
        })?;
        let value: serde_yaml::Value =
            serde_yaml::from_str(&text).map_err(|source| ConfigError::Parse {
                path: path.as_ref().display().to_string(),
                source,
            })?;
        self.merge_value(&value);
        Ok(())
    }

    /// Merge a YAML value into the current config (deep merge).
    pub fn merge_value(&mut self, value: &serde_yaml::Value) {
        deep_merge(value, &mut self.data);
    }

    /// Override config values from environment variables.
    ///
    /// Env names like `SECTION___SUBSECTION___KEY` map to nested keys;
    /// `__` in a segment is replaced by a space in the key name.
    /// Key matching is exact (as in Python). Comma-separated values become
    /// lists. Only keys already present in the config are overridden.
    pub fn inject_os_envs(&mut self) {
        for (env_name, env_value) in std::env::vars() {
            let segments: Vec<&str> = env_name.split("___").collect();
            if segments.len() == 1 {
                continue;
            }
            // Resolve the path of actual key names first (read-only pass).
            let mut path: Vec<String> = Vec::new();
            let mut node = &self.data;
            let mut found = true;
            for segment in &segments[..segments.len() - 1] {
                let key = segment.replace("__", " ");
                let Some(map) = as_map(node) else {
                    found = false;
                    break;
                };
                let Some(value) = map.get(key_str(&key)) else {
                    found = false;
                    break;
                };
                if !is_mapping(value) {
                    found = false;
                    break;
                }
                path.push(key);
                node = value;
            }
            if !found {
                continue;
            }
            let leaf_requested = segments[segments.len() - 1].replace("__", " ");
            let Some(map) = as_map(node) else {
                continue;
            };
            if !map.contains_key(key_str(&leaf_requested)) {
                continue;
            }
            let value: serde_yaml::Value = if env_value.contains(',') {
                let items: Vec<serde_yaml::Value> = env_value
                    .split(',')
                    .map(|part| serde_yaml::Value::String(part.trim().to_string()))
                    .collect();
                serde_yaml::Value::Sequence(items)
            } else {
                serde_yaml::Value::String(env_value.clone())
            };
            // Apply the resolved path mutably.
            let mut node = &mut self.data;
            for key in &path {
                let map = as_map_mut(node).expect("resolved path is a mapping");
                node = map.get_mut(key_str(key)).expect("resolved key exists");
            }
            let map = as_map_mut(node).expect("leaf is a mapping");
            insert_key(map, &leaf_requested, value);
        }
    }

    /// Typed extraction of a top-level key (Python `config.get("MonitorServer")`).
    pub fn get<T: DeserializeOwnedForConfig>(&self, key: &str) -> Result<Option<T>, ConfigError> {
        let Some(map) = as_map(&self.data) else {
            return Ok(None);
        };
        let Some(value) = get_key(map, key) else {
            return Ok(None);
        };
        T::from_value(value)
            .map(Some)
            .map_err(|source| ConfigError::Extract {
                key: key.to_string(),
                source,
            })
    }

    /// Typed extraction of a nested key using a dotted path (`service.max_workers`).
    pub fn get_path<T: DeserializeOwnedForConfig>(
        &self,
        path: &str,
    ) -> Result<Option<T>, ConfigError> {
        let mut current = &self.data;
        for segment in path.split('.') {
            let Some(map) = as_map(current) else {
                return Ok(None);
            };
            let Some(value) = get_key(map, segment) else {
                return Ok(None);
            };
            current = value;
        }
        T::from_value(current)
            .map(Some)
            .map_err(|source| ConfigError::Extract {
                key: path.to_string(),
                source,
            })
    }

    /// Current raw YAML value.
    pub fn value(&self) -> &serde_yaml::Value {
        &self.data
    }
}

/// Helper trait keeping `get`/`get_path` generic over deserializable values.
pub trait DeserializeOwnedForConfig: Sized {
    fn from_value(value: &serde_yaml::Value) -> Result<Self, serde_yaml::Error>;
}

impl<T: serde::de::DeserializeOwned> DeserializeOwnedForConfig for T {
    fn from_value(value: &serde_yaml::Value) -> Result<Self, serde_yaml::Error> {
        serde_yaml::from_value(value.clone())
    }
}

/// One entry of `MonitorServer.service info` (`{info, value}` as in Python).
#[derive(Debug, Clone, Deserialize)]
pub struct ServiceInfoEntry {
    pub info: String,
    #[serde(deserialize_with = "deserialize_f64")]
    pub value: f64,
}

/// Deserialize f64 from a number or a numeric string (env overrides are strings).
fn deserialize_f64<'de, D>(deserializer: D) -> Result<f64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum NumOrStr {
        Num(f64),
        Str(String),
    }
    match NumOrStr::deserialize(deserializer)? {
        NumOrStr::Num(n) => Ok(n),
        NumOrStr::Str(s) => s
            .trim()
            .parse::<f64>()
            .map_err(|e| serde::de::Error::custom(format!("invalid number: {e}"))),
    }
}

/// Deserialize u16 from a number or a numeric string (env overrides are strings).
fn deserialize_u16<'de, D>(deserializer: D) -> Result<u16, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum NumOrStr {
        Num(u16),
        Str(String),
    }
    match NumOrStr::deserialize(deserializer)? {
        NumOrStr::Num(n) => Ok(n),
        NumOrStr::Str(s) => s
            .trim()
            .parse::<u16>()
            .map_err(|e| serde::de::Error::custom(format!("invalid number: {e}"))),
    }
}

/// One error mapping entry of `MonitorServer.errors`
/// (`{service_errors: [...], unhealthy: N}` as in Python).
#[derive(Debug, Clone, Deserialize)]
pub struct ErrorEntry {
    #[serde(default)]
    pub service_errors: Vec<String>,
    #[serde(default = "default_unhealthy")]
    pub unhealthy: i32,
}

fn default_unhealthy() -> i32 {
    -1
}

fn default_port() -> u16 {
    9999
}

/// Process start time string, same format as Python
/// `str(datetime.datetime.now())` (local time, `%Y-%m-%d %H:%M:%S.%f`).
/// Used as the `inittime` label value for all metrics.
pub fn now_inittime() -> String {
    use chrono::Local;
    Local::now().format("%Y-%m-%d %H:%M:%S%.6f").to_string()
}

/// `MonitorServer` config section, identical to Python.
#[derive(Debug, Clone, Deserialize)]
pub struct MonitorServerConfig {
    #[serde(default = "default_port", deserialize_with = "deserialize_u16")]
    pub port: u16,
    #[serde(default, rename = "service info")]
    pub service_info: BTreeMap<String, ServiceInfoEntry>,
    #[serde(default)]
    pub errors: BTreeMap<String, ErrorEntry>,
}

impl Default for MonitorServerConfig {
    fn default() -> Self {
        Self {
            port: default_port(),
            service_info: BTreeMap::new(),
            errors: default_errors(),
        }
    }
}

/// Default error statuses (identical to Python `MonitorServer.run`).
pub fn default_errors() -> BTreeMap<String, ErrorEntry> {
    let mut errors = BTreeMap::new();
    for status in [
        "CONNECTION_ERROR",
        "FATAL",
        "INTERNAL",
        "OTHER_EXCEPTION",
        "UNKNOWN",
    ] {
        let service_errors = if status == "OTHER_EXCEPTION" {
            vec!["other".to_string()]
        } else {
            Vec::new()
        };
        errors.insert(
            status.to_string(),
            ErrorEntry {
                service_errors,
                unhealthy: default_unhealthy(),
            },
        );
    }
    errors
}

impl MonitorServerConfig {
    /// Resolve errors: defaults merged with configured entries (like Python).
    pub fn resolved_errors(&self) -> BTreeMap<String, ErrorEntry> {
        let mut errors = default_errors();
        for (key, entry) in &self.errors {
            errors.insert(key.clone(), entry.clone());
        }
        errors
    }

    /// Service info entries, always including `service_threads`
    /// (Python `MonitorServer.run` adds it after config-driven entries).
    pub fn resolved_service_info(
        &self,
        service_threads: f64,
    ) -> BTreeMap<String, ServiceInfoEntry> {
        let mut info = self.service_info.clone();
        info.insert(
            "service_threads".to_string(),
            ServiceInfoEntry {
                info: "number of service threads setups by the config".to_string(),
                value: service_threads,
            },
        );
        info
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn config_from_str(content: &str) -> Config {
        let value: serde_yaml::Value = serde_yaml::from_str(content).unwrap();
        Config {
            data: value,
            path: None,
        }
    }

    fn write_temp_yaml(content: &str) -> PathBuf {
        let path =
            std::env::temp_dir().join(format!("redup_servicekit_test_{}.yaml", std::process::id()));
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(content.as_bytes()).unwrap();
        path
    }

    #[test]
    fn load_and_get() {
        let path = write_temp_yaml("service:\n  max_workers: 4\nMonitorServer:\n  port: 8080\n");
        let cfg = Config::load(&path).unwrap();
        assert_eq!(cfg.get_path::<u32>("service.max_workers").unwrap(), Some(4));
        let monitor: MonitorServerConfig = cfg.get("MonitorServer").unwrap().unwrap();
        assert_eq!(monitor.port, 8080);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn merge_deep() {
        let mut cfg = config_from_str("a:\n  b: 1\n  c: 2\n");
        cfg.merge_value(&serde_yaml::from_str("a:\n  b: 10\n  d: 3\n").unwrap());
        let a: BTreeMap<String, i64> = cfg.get_path("a").unwrap().unwrap();
        assert_eq!(a.get("b"), Some(&10));
        assert_eq!(a.get("c"), Some(&2));
        assert_eq!(a.get("d"), Some(&3));
    }

    #[test]
    fn env_injection_only_existing_keys() {
        let mut cfg = config_from_str(
            "MonitorServer:\n  port: 9999\n  service info:\n    service_version:\n      info: Service version\n      value: 1\n",
        );
        std::env::set_var("MonitorServer___port", "1234");
        std::env::set_var(
            "MonitorServer___service__info___service_version___value",
            "42",
        );
        std::env::set_var("NOT_IN_CONFIG___KEY", "1");
        cfg.inject_os_envs();
        let monitor: MonitorServerConfig = cfg.get("MonitorServer").unwrap().unwrap();
        assert_eq!(monitor.port, 1234);
        assert_eq!(
            monitor.service_info.get("service_version").map(|e| e.value),
            Some(42.0)
        );
    }

    #[test]
    fn list_from_comma_env() {
        let mut cfg = config_from_str(
            "MonitorServer:\n  errors:\n    OTHER_EXCEPTION:\n      service_errors: [a]\n      unhealthy: -1\n",
        );
        std::env::set_var(
            "MonitorServer___errors___OTHER_EXCEPTION___service_errors",
            "a,b,c",
        );
        cfg.inject_os_envs();
        let monitor: MonitorServerConfig = cfg.get("MonitorServer").unwrap().unwrap();
        assert_eq!(
            monitor
                .errors
                .get("OTHER_EXCEPTION")
                .unwrap()
                .service_errors,
            vec!["a".to_string(), "b".to_string(), "c".to_string()]
        );
    }

    #[test]
    fn default_monitor_port() {
        let monitor = MonitorServerConfig::default();
        assert_eq!(monitor.port, 9999);
        assert_eq!(
            monitor
                .errors
                .get("OTHER_EXCEPTION")
                .unwrap()
                .service_errors,
            vec!["other".to_string()]
        );
    }
}
