"""Base ClickHouse writer for Calix data collectors.

Provides batch insert, table creation, and query capabilities with
OTel instrumentation.
"""

import clickhouse_connect
from typing import Any


class ClickHouseWriter:
    """Batch writer for inserting data into ClickHouse.

    :param cfg: CalixConfiguration (must have ``ClickHouse.ClickHouse.*``).
    :type cfg: CalixConfiguration
    :param logger: Logger instance.
    :type logger: logging.Logger
    :param meter: OTel meter for row counting.
    :type meter: opentelemetry.metrics.Meter
    :param tracer: OTel tracer for span instrumentation.
    :type tracer: opentelemetry.trace.Tracer
    :param metric_prefix: Prefix for metric names (e.g. ``'fred'``, ``'treasury'``).
    :type metric_prefix: str

    .. code-block:: python

        writer = ClickHouseWriter(cfg, logger, meter, tracer, metric_prefix="fred")
        writer.ensure_table("fred_observations", DDL_STRING)
        writer.insert_rows("fred_observations", ["col1", "col2"], [[v1, v2], ...])
        writer.close()
    """

    def __init__(self, cfg, logger, meter, tracer, metric_prefix: str = "collector"):
        ch = cfg.config.ClickHouse.ClickHouse
        self._client = clickhouse_connect.get_client(
            host=ch.host,
            port=int(ch.port),
            database=ch.database,
            username=ch.user,
            password=ch.password,
        )
        self._logger = logger
        self._tracer = tracer
        self._row_counter = meter.create_counter(
            f'{metric_prefix}.rows.ingested',
            description='Rows inserted into ClickHouse',
        )

    def insert_rows(self, table: str, columns: list[str], rows: list[list[Any]]) -> int:
        """Insert a batch of rows into a ClickHouse table.

        :param table: Target table name.
        :type table: str
        :param columns: Column names for the insert.
        :type columns: list[str]
        :param rows: List of row data (each row is a list of values).
        :type rows: list[list[Any]]
        :returns: Number of rows inserted.
        :rtype: int
        """
        if not rows:
            return 0

        with self._tracer.start_as_current_span(f"clickhouse.insert.{table}") as span:
            span.set_attribute("table", table)
            span.set_attribute("row_count", len(rows))
            self._client.insert(
                table, rows, column_names=columns,
                settings={"max_partitions_per_insert_block": 0},
            )
            self._row_counter.add(len(rows), {"table": table})
            self._logger.info(f"Inserted {len(rows)} rows into {table}")
            return len(rows)

    def ensure_table(self, table_name: str, ddl: str):
        """Execute CREATE TABLE IF NOT EXISTS.

        :param table_name: Table name (for logging).
        :type table_name: str
        :param ddl: Full DDL statement.
        :type ddl: str
        """
        self._logger.info(f"Ensuring ClickHouse table: {table_name}")
        self._client.command(ddl)

    def query(self, sql: str, params: dict | None = None):
        """Execute a query and return results.

        :param sql: SQL query string.
        :type sql: str
        :param params: Optional query parameters.
        :type params: dict | None
        :returns: Query result set.
        """
        return self._client.query(sql, parameters=params)

    def close(self):
        """Close the ClickHouse client connection."""
        self._client.close()
