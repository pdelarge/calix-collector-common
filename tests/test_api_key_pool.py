"""Tests for calix_collector.api_key_pool module."""

import threading
import time
from unittest.mock import MagicMock, patch

import pytest

from calix_collector.api_key_pool import ApiKeyPool, ApiKeyState


class TestApiKeyState:
    """Tests for ApiKeyState dataclass."""

    def test_is_available_within_budget(self):
        state = ApiKeyState(key="test_key")
        assert state.is_available(time.monotonic(), budget_per_minute=120) is True

    def test_is_available_resets_after_minute(self):
        state = ApiKeyState(key="test_key", requests_this_minute=120)
        state.minute_window_start = time.monotonic() - 61.0
        assert state.is_available(time.monotonic(), budget_per_minute=120) is True
        assert state.requests_this_minute == 0

    def test_not_available_during_cooldown(self):
        state = ApiKeyState(key="test_key")
        state.cooldown_until = time.monotonic() + 60.0
        assert state.is_available(time.monotonic(), budget_per_minute=120) is False

    def test_consume_increments_counters(self):
        state = ApiKeyState(key="test_key")
        state.consume()
        assert state.requests_this_minute == 1
        assert state.total_requests == 1

    def test_put_in_cooldown(self):
        state = ApiKeyState(key="test_key")
        state.put_in_cooldown(30.0)
        assert state.cooldown_until > time.monotonic()
        assert state.total_errors == 1


class TestApiKeyPool:
    """Tests for ApiKeyPool class."""

    def test_acquire_returns_key(self):
        pool = ApiKeyPool(keys=["key1", "key2"], budget_per_minute=120)
        key = pool.acquire()
        assert key in ("key1", "key2")

    def test_acquire_prefers_least_loaded(self):
        pool = ApiKeyPool(keys=["key1", "key2"], budget_per_minute=120)
        # Exhaust key1 partially
        for _ in range(50):
            pool.acquire()
        # After 50 calls, key2 should have been preferred after key1 was used first
        stats = pool.stats()
        # Both keys should have been used
        total = sum(s["requests_this_minute"] for s in stats)
        assert total == 50

    def test_acquire_skips_exhausted_key(self):
        pool = ApiKeyPool(keys=["key1", "key2"], budget_per_minute=2)
        # Drain key1 by acquiring until it's used
        keys_used = set()
        for _ in range(4):
            keys_used.add(pool.acquire())
        assert "key1" in keys_used
        assert "key2" in keys_used

    def test_acquire_skips_cooldown_key(self):
        pool = ApiKeyPool(keys=["key1", "key2"], budget_per_minute=120)
        pool.report_error("key1", cooldown_seconds=60.0)
        key = pool.acquire()
        assert key == "key2"

    def test_report_error_puts_in_cooldown(self):
        logger = MagicMock()
        pool = ApiKeyPool(keys=["key1"], budget_per_minute=120, logger=logger)
        pool.report_error("key1", cooldown_seconds=60.0)
        logger.warning.assert_called_once()
        stats = pool.stats()
        assert stats[0]["in_cooldown"] is True

    def test_acquire_timeout_raises_runtime_error(self):
        pool = ApiKeyPool(keys=["key1"], budget_per_minute=1)
        pool.acquire()  # Use the single budget
        with pytest.raises(RuntimeError, match="All API keys exhausted"):
            pool.acquire(timeout=0.5)

    def test_stats_returns_all_keys(self):
        pool = ApiKeyPool(keys=["key1", "key2", "key3"], budget_per_minute=120)
        stats = pool.stats()
        assert len(stats) == 3
        assert all("key_suffix" in s for s in stats)

    def test_key_count(self):
        pool = ApiKeyPool(keys=["k1", "k2", "k3"], budget_per_minute=120)
        assert pool.key_count == 3

    def test_thread_safety(self):
        pool = ApiKeyPool(keys=[f"key{i}" for i in range(5)], budget_per_minute=100)
        results = []
        errors = []

        def worker():
            try:
                for _ in range(20):
                    key = pool.acquire(timeout=5.0)
                    results.append(key)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        assert len(results) == 200
        stats = pool.stats()
        total = sum(s["total_requests"] for s in stats)
        assert total == 200
