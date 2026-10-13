# Configuration

The `Config` type mirrors `redup_servicekit.config.ConfigSingleton`:

- `Config::load(path)` loads a `.yaml` file;
- `Config::merge_path(path)` / `Config::merge_value(value)` deep-merge extra
  config (source wins);
- `Config::inject_os_envs()` overrides existing keys from environment
  variables: `SECTION___SUBSECTION___KEY` maps to nested keys, `__` in a
  segment becomes a space, comma-separated values become lists;
- `Config::get::<T>(key)` / `Config::get_path::<T>("a.b.c")` extract typed
  values.

The `MonitorServer` section is identical to Python:

```yaml
service:
  max_workers: 8
  hpa_max_workers: 8

MonitorServer:
  port: 9999
  service info:
    service_version:
      info: Service version
      value: 1
  errors:
    OTHER_EXCEPTION:
      service_errors: [other]
      unhealthy: -1
```