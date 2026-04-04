"""Tests for calix_collector.clickhouse_writer module."""

from unittest.mock import MagicMock, patch


class TestClickHouseWriter:
    """Tests for ClickHouseWriter class."""

    @patch("calix_collector.clickhouse_writer.clickhouse_connect")
    def _make_writer(self, mock_ch):
        from calix_collector.clickhouse_writer import ClickHouseWriter

        cfg = MagicMock()
        cfg.config.ClickHouse.ClickHouse.host = "ch.local"
        cfg.config.ClickHouse.ClickHouse.port = 8123
        cfg.config.ClickHouse.ClickHouse.database = "default"
        cfg.config.ClickHouse.ClickHouse.user = "admin"
        cfg.config.ClickHouse.ClickHouse.password = "pass"

        logger = MagicMock()
        meter = MagicMock()
        tracer = MagicMock()

        writer = ClickHouseWriter(cfg, logger, meter, tracer, metric_prefix="test")
        mock_client = mock_ch.get_client.return_value
        return writer, mock_client, logger, meter, tracer

    def test_insert_rows_calls_client(self):
        writer, mock_client, logger, meter, tracer = self._make_writer()
        rows = [["a", 1], ["b", 2]]

        result = writer.insert_rows("test_table", ["col1", "col2"], rows)

        assert result == 2
        mock_client.insert.assert_called_once_with(
            "test_table", rows, column_names=["col1", "col2"],
            settings={"max_partitions_per_insert_block": 0},
        )

    def test_insert_empty_rows_returns_zero(self):
        writer, mock_client, *_ = self._make_writer()

        result = writer.insert_rows("test_table", ["col1"], [])

        assert result == 0
        mock_client.insert.assert_not_called()

    def test_ensure_table_calls_command(self):
        writer, mock_client, logger, *_ = self._make_writer()
        ddl = "CREATE TABLE IF NOT EXISTS test (id Int32) ENGINE = MergeTree()"

        writer.ensure_table("test", ddl)

        mock_client.command.assert_called_once_with(ddl)

    def test_query_delegates_to_client(self):
        writer, mock_client, *_ = self._make_writer()

        writer.query("SELECT 1", params={"a": 1})

        mock_client.query.assert_called_once_with("SELECT 1", parameters={"a": 1})

    def test_close_closes_client(self):
        writer, mock_client, *_ = self._make_writer()

        writer.close()

        mock_client.close.assert_called_once()
