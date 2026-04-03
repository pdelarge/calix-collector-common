"""Tests for calix_collector.config module."""

from unittest.mock import MagicMock, patch

from calix_collector.config import get_redis_url, make_config


class TestMakeConfig:
    """Tests for make_config factory."""

    @patch("calix_collector.config.CalixConfiguration")
    def test_uses_default_path(self, mock_calix_cfg):
        make_config("/Calix/Flows/test")
        mock_calix_cfg.assert_called_once_with("/Calix/Flows/test", labels=["DEV"])

    @patch.dict("os.environ", {"CALIX_CONFIG_PATH": "/custom/path"})
    @patch("calix_collector.config.CalixConfiguration")
    def test_env_overrides_default_path(self, mock_calix_cfg):
        make_config("/Calix/Flows/test")
        mock_calix_cfg.assert_called_once_with("/custom/path", labels=["DEV"])

    @patch.dict("os.environ", {"CALIX_LABEL": "PROD"})
    @patch("calix_collector.config.CalixConfiguration")
    def test_label_from_env(self, mock_calix_cfg):
        make_config("/Calix/Flows/test")
        mock_calix_cfg.assert_called_once_with("/Calix/Flows/test", labels=["PROD"])


class TestGetRedisUrl:
    """Tests for get_redis_url helper."""

    def test_builds_redis_url(self):
        cfg = MagicMock()
        cfg.config.Redis.Redis.host = "redis.local"
        cfg.config.Redis.Redis.port = 6379
        cfg.config.Redis.Redis.password = "secret"

        url = get_redis_url(cfg)
        assert url == "redis://:secret@redis.local:6379/0"
