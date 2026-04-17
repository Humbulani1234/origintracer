from django.http import JsonResponse
from django.urls import include, path

import origintracer

urlpatterns = [
    path("", include("django_tracer.urls")),
]


def tracer_stats_view(request):
    engine = origintracer.get_engine()
    if not engine:
        return JsonResponse({"error": "No engine"}, status=500)

    # Extract real-time metrics from the live engine memory
    stats = {
        "buf_depth": (
            len(engine._buffer)
            if hasattr(engine, "_buffer")
            else 0
        ),
        "buf_dropped": (
            getattr(engine._buffer, "_dropped", 0)
            if hasattr(engine, "_buffer")
            else 0
        ),
        "node_count": len(list(engine.graph.all_nodes())),
        "edge_count": len(list(engine.graph.all_edges())),
    }
    return JsonResponse(stats)


urlpatterns += [path("__tracer__/stats/", tracer_stats_view)]
