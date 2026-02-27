"""
mysite/views.py

Four views that exercise different probe paths.

GET /              sync view — baseline, hits no database
GET /async/        async view — exercises asyncio probes
GET /slow/         async view with a simulated blocking call
                   triggers the loop_starvation causal rule
GET /db/           async view that queries the database
                   triggers db.query probes
"""

import asyncio
import time

from django.http import JsonResponse
from django.views import View


class IndexView(View):
    """
    Synchronous baseline view.
    Demonstrates: request.entry → django.middleware → django.view → request.exit
    No async, no database. Fastest possible path through Django.
    """
    def get(self, request):
        return JsonResponse({
            "view": "index",
            "message": "StackTracer Django demo",
            "probe": "try /async/, /slow/, /db/ for more interesting traces",
        })


class AsyncView(View):
    """
    Async view with two concurrent tasks.
    Demonstrates: asyncio.task.create fires twice, loop ticks stay fast.
    Both tasks yield properly so the event loop stays healthy.
    """
    async def get(self, request):
        async def fetch_a():
            await asyncio.sleep(0.01)   # yields to event loop — healthy await
            return "result_a"

        async def fetch_b():
            await asyncio.sleep(0.02)   # yields to event loop — healthy await
            return "result_b"

        result_a, result_b = await asyncio.gather(
            asyncio.create_task(fetch_a()),
            asyncio.create_task(fetch_b()),
        )

        return JsonResponse({
            "view": "async",
            "result_a": result_a,
            "result_b": result_b,
            "note": "Two tasks ran concurrently. Check asyncio.loop.tick avg_duration_ns — should be < 1ms.",
        })


class SlowView(View):
    """
    Async view with a deliberately blocking call.

    time.sleep() does NOT yield to the event loop — it blocks the entire
    OS thread. Every other coroutine waits for the full duration.

    This is the canonical example of event loop starvation.
    StackTracer's loop_starvation causal rule will fire after a few requests.

    In the REPL:
        CAUSAL WHERE tags = "blocking"    ← fires with 80% confidence
        BLAME WHERE system = "django"     ← points to this view
        SHOW latency WHERE system = "django"  ← slow_view node stands out
    """
    async def get(self, request):
        # This blocks the event loop for 200ms.
        # Compare with /async/ which uses await asyncio.sleep() instead.
        time.sleep(0.2)   # ← intentional blocking call for demonstration

        return JsonResponse({
            "view": "slow",
            "blocked_ms": 200,
            "note": (
                "time.sleep() blocked the event loop. "
                "Run CAUSAL in the StackTracer REPL to detect it. "
                "asyncio.loop.tick avg_duration_ns will be >> 10ms."
            ),
        })


class DbView(View):
    """
    Async view that queries the database via Django ORM.
    Demonstrates: db.query.start and db.query.end probes firing.

    Django's async ORM (sync_to_async wrapper) is used here.
    This is the correct pattern — it runs the ORM query in a thread pool
    so the event loop is not blocked.

    If you were to call ORM methods directly in an async view without
    sync_to_async, Django raises SynchronousOnlyOperation.
    StackTracer would detect the blocking call on the async path.
    """
    async def get(self, request):
        from django.contrib.auth import get_user_model
        from django.db import connection
        from asgiref.sync import sync_to_async

        User = get_user_model()

        # sync_to_async runs the ORM call in a thread pool worker.
        # The event loop yields here — this is correct async ORM usage.
        @sync_to_async
        def get_user_count():
            return User.objects.count()

        count = await get_user_count()

        return JsonResponse({
            "view": "db",
            "user_count": count,
            "note": (
                "ORM query ran via sync_to_async. "
                "Check db.query.start and db.query.end in the REPL. "
                "Run: SHOW latency WHERE system = 'database'"
            ),
        })