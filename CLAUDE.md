# calix-collector-common

Shared Python library that extracts reusable patterns across Calix data collectors.

## Purpose

Provides base classes and factory functions for telemetry, rate limiting, ClickHouse writing,
and configuration. Used by calix-bls-collector, calix-us-fiscal-data, calix-treasury-direct,
and calix-fred-collector.

## Components

- **config.py** — `make_config(default_path)` factory + `get_redis_url(cfg)` helper
- **telemetry.py** — `setup_telemetry(service_prefix, component, cfg)` → (logger, meter, tracer)
- **rate_limiter.py** — `create_rate_limiter(cfg, key_prefix, default_rps)` → Redis-backed Limiter
- **clickhouse_writer.py** — `ClickHouseWriter(cfg, logger, meter, tracer, metric_prefix)` base class
- **api_key_pool.py** — `ApiKeyPool(keys, budget_per_minute, logger)` with least-loaded selection

## Usage

```bash
pip install -e /home/patrick/repos/calix-collector-common
```

## Conventions

- Docstrings: Sphinx-compatible (`:param:`, `:type:`, `:returns:`, `:rtype:`, `:raises:`)
- Type hints: Python 3.12+, all signatures typed
- Tests: pytest, mock external dependencies, no network calls
