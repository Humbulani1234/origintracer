import os
from pathlib import Path

from django.apps import AppConfig
from django.conf import settings


class WorkerConfig(AppConfig):
    name = "django_tracer"

    def ready(self):
        # Only run in the worker process, skip the reloader parent
        if os.environ.get("RUN_MAIN") == "false":
            return

        # Bootstrap the native OriginTracer engine
        self._bootstrap_native()

        # Optionally layer on OpenTelemetry instrumentation
        if getattr(settings, "ORIGINTRACER_OTEL_MODE", False):
            self._init_otel()

    def _bootstrap_native(self):
        """
        Core initialization logic for the native engine.
        """
        import origintracer

        base_dir = Path(__file__).resolve().parent.parent
        config_path = str(base_dir / "origintracer.yaml")

        origintracer.init(config=config_path)
        origintracer.mark_deployment("deployment")

    def _init_otel(self):
        """
        Full OTEL instrumentation suite.
        """
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

        from origintracer.bridge.otel_bridge import (
            OriginTracerSpanExporter,
        )

        # Setup OTEL Pipeline
        provider = TracerProvider()
        provider.add_span_processor(
            BatchSpanProcessor(OriginTracerSpanExporter())
        )
        trace.set_tracer_provider(provider)

        # Instrument common libraries
        DjangoInstrumentor().instrument()
        Psycopg2Instrumentor().instrument()
        RedisInstrumentor().instrument()
