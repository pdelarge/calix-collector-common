"""Multi-key API pool with per-key budget tracking.

Provides thread-safe key rotation with least-loaded selection and
cooldown support for rate-limited APIs.
"""

import threading
import time
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class ApiKeyState:
    """Tracks usage state of a single API key.

    :param key: The API key string.
    :type key: str
    :param requests_this_minute: Requests consumed in the current minute window.
    :type requests_this_minute: int
    :param minute_window_start: Monotonic timestamp when current minute window started.
    :type minute_window_start: float
    :param cooldown_until: Monotonic timestamp until which this key is in cooldown.
    :type cooldown_until: float
    :param total_requests: Total requests ever made with this key.
    :type total_requests: int
    :param total_errors: Total errors (429s) for this key.
    :type total_errors: int
    """

    key: str
    requests_this_minute: int = 0
    minute_window_start: float = field(default_factory=time.monotonic)
    cooldown_until: float = 0.0
    total_requests: int = 0
    total_errors: int = 0

    def is_available(self, now: float, budget_per_minute: int) -> bool:
        """Check if this key has budget remaining and is not in cooldown.

        :param now: Current monotonic time.
        :type now: float
        :param budget_per_minute: Maximum requests per minute.
        :type budget_per_minute: int
        :returns: True if the key can be used.
        :rtype: bool
        """
        if now < self.cooldown_until:
            return False
        if now - self.minute_window_start >= 60.0:
            self.requests_this_minute = 0
            self.minute_window_start = now
        return self.requests_this_minute < budget_per_minute

    def consume(self):
        """Mark one request consumed."""
        self.requests_this_minute += 1
        self.total_requests += 1

    def put_in_cooldown(self, seconds: float = 60.0):
        """Put this key in cooldown (e.g., after receiving 429).

        :param seconds: Cooldown duration in seconds.
        :type seconds: float
        """
        self.cooldown_until = time.monotonic() + seconds
        self.total_errors += 1


class ApiKeyPool:
    """Thread-safe API key pool with per-key budget tracking.

    Selects the key with the most remaining budget (least-loaded).
    Falls back to waiting if all keys are exhausted.

    :param keys: List of API key strings.
    :type keys: list[str]
    :param budget_per_minute: Max requests per minute per key (FRED default: 120).
    :type budget_per_minute: int
    :param logger: Logger instance.
    :type logger: logging.Logger

    .. code-block:: python

        pool = ApiKeyPool(keys=["key1", "key2", ...], budget_per_minute=120, logger=logger)
        key = pool.acquire()  # returns the best available key string
        pool.report_error(key, cooldown_seconds=60)  # on 429
    """

    def __init__(self, keys: list[str], budget_per_minute: int = 120, logger=None):
        self._states = [ApiKeyState(key=k) for k in keys]
        self._budget_per_minute = budget_per_minute
        self._lock = threading.Lock()
        self._logger = logger

    def acquire(self, timeout: float = 30.0) -> str:
        """Acquire the best available API key.

        Selects the key with the most remaining budget (least-loaded).

        :param timeout: Max seconds to wait for an available key.
        :type timeout: float
        :returns: API key string.
        :rtype: str
        :raises RuntimeError: If no key becomes available within timeout.
        """
        deadline = time.monotonic() + timeout

        while time.monotonic() < deadline:
            with self._lock:
                now = time.monotonic()
                best: Optional[ApiKeyState] = None
                best_remaining = -1

                for state in self._states:
                    if state.is_available(now, self._budget_per_minute):
                        remaining = self._budget_per_minute - state.requests_this_minute
                        if remaining > best_remaining:
                            best = state
                            best_remaining = remaining

                if best is not None:
                    best.consume()
                    return best.key

            # All keys exhausted — wait briefly and retry
            time.sleep(0.5)

        raise RuntimeError("All API keys exhausted — no key available within timeout")

    def report_error(self, key: str, cooldown_seconds: float = 60.0):
        """Report a 429 or error for a specific key, putting it in cooldown.

        :param key: The API key string that received the error.
        :type key: str
        :param cooldown_seconds: How long to put the key in cooldown.
        :type cooldown_seconds: float
        """
        with self._lock:
            for state in self._states:
                if state.key == key:
                    state.put_in_cooldown(cooldown_seconds)
                    if self._logger:
                        self._logger.warning(
                            f"API key ...{key[-4:]} in cooldown for {cooldown_seconds}s"
                        )
                    break

    def stats(self) -> list[dict]:
        """Return usage stats for all keys (for Grafana/admin display).

        :returns: List of dicts with per-key stats.
        :rtype: list[dict]
        """
        with self._lock:
            return [
                {
                    "key_suffix": s.key[-4:],
                    "requests_this_minute": s.requests_this_minute,
                    "total_requests": s.total_requests,
                    "total_errors": s.total_errors,
                    "in_cooldown": time.monotonic() < s.cooldown_until,
                }
                for s in self._states
            ]

    @property
    def key_count(self) -> int:
        """Return the number of keys in the pool.

        :returns: Number of keys.
        :rtype: int
        """
        return len(self._states)
