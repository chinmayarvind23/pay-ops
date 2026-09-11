"""Standalone service processes export trace evidence without cloud credentials."""

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.trace import ProxyTracerProvider

from payops.sandbox.models import Role


def configure_tracing(role: Role) -> None:
    """Reuse host instrumentation; otherwise install one process-wide console exporter."""
    if isinstance(trace.get_tracer_provider(), ProxyTracerProvider):
        provider = TracerProvider(
            resource=Resource.create({"service.name": f"payops-sandbox-{role}"})
        )
        provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))
        trace.set_tracer_provider(provider)
