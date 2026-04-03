# calix-collector-common

Shared Python library for Calix data collectors. Extracts common patterns for telemetry,
rate limiting, ClickHouse writing, configuration, and API key management.

## Installation

```bash
pip install -e .
```

## Components

### Config

```python
from calix_collector.config import make_config, get_redis_url
from functools import lru_cache

@lru_cache(maxsize=1)
def get_config():
    return make_config("/Calix/Flows/my-collector")

cfg = get_config()
redis_url = get_redis_url(cfg)
```

### Telemetry

```python
from calix_collector.telemetry import setup_telemetry

logger, meter, tracer = setup_telemetry("calix-my-collector", "component-name", cfg)
```

### Rate Limiter

```python
from calix_collector.rate_limiter import create_rate_limiter

limiter = create_rate_limiter(cfg, "RateLimiter:MyCollector", default_rps=10)
```

### ClickHouse Writer

```python
from calix_collector.clickhouse_writer import ClickHouseWriter

writer = ClickHouseWriter(cfg, logger, meter, tracer, metric_prefix="my_collector")
writer.insert_rows("my_table", ["col1", "col2"], [[val1, val2], ...])
```

### API Key Pool

```python
from calix_collector.api_key_pool import ApiKeyPool

pool = ApiKeyPool(keys=["key1", "key2", ...], budget_per_minute=120, logger=logger)
key = pool.acquire()       # returns least-loaded key
pool.report_error(key, 60) # cooldown on 429
```

## Development

```bash
pip install -e ".[dev]"
pytest tests/ -v --cov=calix_collector
```
