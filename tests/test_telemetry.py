"""Tests for calix_collector.telemetry module."""

import logging
from unittest.mock import MagicMock, patch


class TestSetupTelemetry:
    """Tests for setup_telemetry function."""

    @patch("calix_collector.telemetry.BatchSpanProcessor")
    @patch("calix_collector.telemetry.TracerProvider")
    @patch("calix_collector.telemetry.OTLPSpanExporter")
    @patch("calix_collector.telemetry.PeriodicExportingMetricReader")
    @patch("calix_collector.telemetry.MeterProvider")
    @patch("calix_collector.telemetry.OTLPMetricExporter")
    @patch("calix_collector.telemetry.LogConfig")
    def test_returns_logger_meter_tracer(
        self, mock_log_config, mock_metric_exporter, mock_meter_provider,
        mock_reader, mock_span_exporter, mock_tracer_provider, mock_batch_processor,
    ):
        from calix_collector.telemetry import setup_telemetry

        cfg = MagicMock()
        cfg.config.log_level = "INFO"
        cfg.config.Observability.OTel.endpoint_logs = "http://otel:4318/v1/logs"
        cfg.config.Observability.OTel.endpoint_metrics = "http://otel:4318/v1/metrics"
        cfg.config.Observability.OTel.endpoint_traces = "http://otel:4318/v1/traces"

        logger, meter, tracer = setup_telemetry("calix-test", "component", cfg)

        assert isinstance(logger, logging.Logger)
        assert logger.name == "calix-test.component"
        mock_log_config.assert_called_once()

    @patch("calix_collector.telemetry.BatchSpanProcessor")
    @patch("calix_collector.telemetry.TracerProvider")
    @patch("calix_collector.telemetry.OTLPSpanExporter")
    @patch("calix_collector.telemetry.PeriodicExportingMetricReader")
    @patch("calix_collector.telemetry.MeterProvider")
    @patch("calix_collector.telemetry.OTLPMetricExporter")
    @patch("calix_collector.telemetry.LogConfig")
    def test_service_name_format(
        self, mock_log_config, mock_metric_exporter, mock_meter_provider,
        mock_reader, mock_span_exporter, mock_tracer_provider, mock_batch_processor,
    ):
        from calix_collector.telemetry import setup_telemetry

        cfg = MagicMock()
        cfg.config.log_level = "DEBUG"
        cfg.config.Observability.OTel.endpoint_logs = "http://otel:4318/v1/logs"
        cfg.config.Observability.OTel.endpoint_metrics = "http://otel:4318/v1/metrics"
        cfg.config.Observability.OTel.endpoint_traces = "http://otel:4318/v1/traces"

        logger, meter, tracer = setup_telemetry("calix-fred-collector", "starred-sync", cfg)

        assert logger.name == "calix-fred-collector.starred-sync"
