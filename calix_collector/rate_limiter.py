"""Redis-backed rate limiter for Calix data collectors.

Uses pyrate-limiter with a Redis bucket for distributed rate limiting
across multiple workers.
"""

from redis import Redis

from pyrate_limiter import Duration, Limiter, Rate, RedisBucket, SingleBucketFactory, MonotonicClock


def create_rate_limiter(
    cfg,
    key_prefix: str,
    default_rps: int = 10,
) -> Limiter:
    """Create a Redis-backed rate limiter.

    :param cfg: CalixConfiguration instance (must have ``Redis.Redis.*`` and
        optionally ``max_request_per_seconds``).
    :type cfg: CalixConfiguration
    :param key_prefix: Redis key prefix (e.g. ``'RateLimiter:FRED'``).
    :type key_prefix: str
    :param default_rps: Default requests per second if not in config.
    :type default_rps: int
    :returns: Configured Limiter (delays on limit, never raises).
    :rtype: pyrate_limiter.Limiter
    """
    from calix_collector.config import get_redis_url

    max_rps = int(getattr(cfg.config, 'max_request_per_seconds', default_rps))
    redis_url = get_redis_url(cfg)

    redis_conn = Redis.from_url(redis_url)
    rates = [Rate(max_rps, Duration.SECOND)]
    bucket = RedisBucket(rates, redis_conn, bucket_key=key_prefix, script_hash="")
    clock = MonotonicClock()
    factory = SingleBucketFactory(bucket, clock)
    return Limiter(factory)
