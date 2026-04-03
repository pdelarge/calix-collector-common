"""Tests for calix_collector.rate_limiter module."""

from unittest.mock import MagicMock, patch


class TestCreateRateLimiter:
    """Tests for create_rate_limiter factory."""

    @patch("calix_collector.rate_limiter.Limiter")
    @patch("calix_collector.rate_limiter.SingleBucketFactory")
    @patch("calix_collector.rate_limiter.RedisBucket")
    @patch("calix_collector.rate_limiter.Redis")
    @patch("calix_collector.config.get_redis_url", return_value="redis://:pass@host:6379/0")
    def test_creates_limiter_with_config_rps(
        self, mock_redis_url, mock_redis, mock_bucket, mock_factory, mock_limiter,
    ):
        from calix_collector.rate_limiter import create_rate_limiter

        cfg = MagicMock()
        cfg.config.max_request_per_seconds = 15

        result = create_rate_limiter(cfg, "RateLimiter:TEST", default_rps=10)

        mock_redis.from_url.assert_called_once_with("redis://:pass@host:6379/0")
        mock_bucket.assert_called_once()
        mock_limiter.assert_called_once()
        assert result is mock_limiter.return_value

    @patch("calix_collector.rate_limiter.Limiter")
    @patch("calix_collector.rate_limiter.SingleBucketFactory")
    @patch("calix_collector.rate_limiter.RedisBucket")
    @patch("calix_collector.rate_limiter.Redis")
    @patch("calix_collector.config.get_redis_url", return_value="redis://:pass@host:6379/0")
    def test_uses_default_rps_when_not_in_config(
        self, mock_redis_url, mock_redis, mock_bucket, mock_factory, mock_limiter,
    ):
        from calix_collector.rate_limiter import create_rate_limiter

        cfg = MagicMock(spec=[])

        class FakeConfig:
            pass

        cfg.config = FakeConfig()

        result = create_rate_limiter(cfg, "RateLimiter:TEST", default_rps=5)

        mock_limiter.assert_called_once()
        assert result is mock_limiter.return_value
