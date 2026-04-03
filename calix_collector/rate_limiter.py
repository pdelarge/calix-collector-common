"""Rate limiter for Calix data collectors.

Uses pyrate-limiter with an in-memory bucket for per-process rate limiting.
"""

from pyrate_limiter import Duration, InMemoryBucket, Limiter, Rate


def create_rate_limiter(
    cfg,
    key_prefix: str,
    default_rps: int = 10,
) -> Limiter:
    """Create a rate limiter.

    :param cfg: CalixConfiguration instance (optionally has
        ``max_request_per_seconds``).
    :type cfg: CalixConfiguration
    :param key_prefix: Identifier for the limiter (used for logging).
    :type key_prefix: str
    :param default_rps: Default requests per second if not in config.
    :type default_rps: int
    :returns: Configured Limiter.
    :rtype: pyrate_limiter.Limiter
    """
    max_rps = int(getattr(cfg.config, 'max_request_per_seconds', default_rps))
    rates = [Rate(max_rps, Duration.SECOND)]
    bucket = InMemoryBucket(rates)
    return Limiter(bucket)
