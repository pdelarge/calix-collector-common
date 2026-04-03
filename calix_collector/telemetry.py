"""Unified OpenTelemetry bootstrap for Calix data collectors.

Configures logging (via PyPowerLogger), metrics, and traces from etcd config
in a single call.
"""

import logging
import os
import socket

from opentelemetry import metrics, trace
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from PyPowerLogger import Config as LogConfig


def setup_telemetry(
    service_prefix: str,
    component: str,
    cfg=None,
) -> tuple[logging.Logger, metrics.Meter, trace.Tracer]:
    """Bootstrap OTel logs, metrics, and traces from etcd config.

    :param service_prefix: Service name prefix (e.g. ``'calix-fred-collector'``).
    :type service_prefix: str
    :param component: Component suffix (e.g. ``'starred-sync'``).
    :type component: str
    :param cfg: CalixConfiguration instance (must have ``Observability.OTel.*``).
    :type cfg: CalixConfiguration
    :returns: Tuple of (logger, meter, tracer).
    :rtype: tuple[logging.Logger, opentelemetry.metrics.Meter, opentelemetry.trace.Tracer]
    """
    label = os.environ.get('CALIX_LABEL', 'DEV')
    service_name = f'{service_prefix}.{component}'
    hostname = os.environ.get('OTEL_RESOURCE_HOST_NAME', socket.gethostname())

    resource = Resource.create({
        'service.name': service_name,
        'host.name': hostname,
    })

    # Logs
    log_level = getattr(cfg.config, 'log_level', 'TRACE' if label == 'DEV' else 'INFO')
    LogConfig(
        name=service_name,
        level=log_level,
        enable_stream_handler=True,
        enable_otel_handler=True,
        otel_endpoint=cfg.config.Observability.OTel.endpoint_logs,
        otel_resource=resource,
    )
    logger = logging.getLogger(service_name)

    # Metrics
    metric_exporter = OTLPMetricExporter(
        endpoint=cfg.config.Observability.OTel.endpoint_metrics,
    )
    reader = PeriodicExportingMetricReader(metric_exporter, export_interval_millis=30_000)
    meter_provider = MeterProvider(resource=resource, metric_readers=[reader])
    metrics.set_meter_provider(meter_provider)
    meter = metrics.get_meter(service_name)

    # Traces
    trace_exporter = OTLPSpanExporter(
        endpoint=cfg.config.Observability.OTel.endpoint_traces,
    )
    tracer_provider = TracerProvider(resource=resource)
    tracer_provider.add_span_processor(BatchSpanProcessor(trace_exporter))
    trace.set_tracer_provider(tracer_provider)
    tracer = trace.get_tracer(service_name)

    return logger, meter, tracer
