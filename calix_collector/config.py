"""Configuration factory for Calix data collectors.

Provides a cached configuration loader backed by etcd via pycalixconfig,
and a Redis URL builder utility.
"""

import os
from functools import lru_cache

from pycalixconfig import CalixConfiguration


def make_config(default_path: str) -> CalixConfiguration:
    """Create a cached CalixConfiguration for a collector.

    :param default_path: Default etcd path if CALIX_CONFIG_PATH is not set.
    :type default_path: str
    :returns: Configuration instance loaded from etcd.
    :rtype: CalixConfiguration

    Environment variables:
        CALIX_CONFIG_SERVER_URL: URL of the calix-config-server
        CALIX_LABEL: DEV or PROD (default: DEV)
        CALIX_CONFIG_PATH: etcd base path (overrides default_path)

    .. code-block:: python

        # In each collector's config.py:
        from calix_collector.config import make_config
        from functools import lru_cache

        @lru_cache(maxsize=1)
        def get_config():
            return make_config("/Calix/Flows/fred-collector")
    """
    path = os.environ.get('CALIX_CONFIG_PATH', default_path)
    label = os.environ.get('CALIX_LABEL', 'DEV')
    return CalixConfiguration(path, labels=[label])


def get_redis_url(cfg: CalixConfiguration) -> str:
    """Build Redis connection URL from config.

    :param cfg: CalixConfiguration instance.
    :type cfg: CalixConfiguration
    :returns: Redis URL in format ``redis://:password@host:port/0``.
    :rtype: str
    """
    r = cfg.config.Redis.Redis
    return f"redis://:{r.password}@{r.host}:{r.port}/0"
