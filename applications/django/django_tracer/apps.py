import os
from pathlib import Path

from django.apps import AppConfig
from django.conf import settings


class WorkerConfig(AppConfig):
    name = "django_tracer"

    def ready(self):
        # 1. Guard: Only run in the worker process, skip the reloader parent
        if os.environ.get("RUN_MAIN") == "false":
            return

        # 2. Bootstrap the native StackTracer engine
        self._bootstrap_native()

        # 3. Optionally layer on OpenTelemetry instrumentation
        if getattr(settings, "STACKTRACER_OTEL_MODE", False):
            self._init_otel()

    def _bootstrap_native(self):
        """Core initialization logic for the native engine."""
        import stacktracer

        base_dir = Path(__file__).resolve().parent.parent
        config_path = str(base_dir / "stacktracer.yaml")

        stacktracer.init(config=config_path)
        stacktracer.mark_deployment("deployment")

    def _init_otel(self):
        """Full OTEL instrumentation suite."""
        from opentelemetry import trace
        from opentelemetry.instrumentation.django import (
            DjangoInstrumentor,
        )
        from opentelemetry.instrumentation.psycopg2 import (
            Psycopg2Instrumentor,
        )
        from opentelemetry.instrumentation.redis import (
            RedisInstrumentor,
        )
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import (
            BatchSpanProcessor,
        )

        from stacktracer.bridge.otel_bridge import (
            StackTracerSpanExporter,
        )

        # Setup OTEL Pipeline
        provider = TracerProvider()
        provider.add_span_processor(
            BatchSpanProcessor(StackTracerSpanExporter())
        )
        trace.set_tracer_provider(provider)

        # Instrument common libraries
        DjangoInstrumentor().instrument()
        Psycopg2Instrumentor().instrument()
        RedisInstrumentor().instrument()
