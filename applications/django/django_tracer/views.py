"""
django_tracer/views.py

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
        return JsonResponse(
            {
                "view": "index",
                "message": "StackTracer Django demo",
                "probe": "try /async/, /slow/, /db/ for more interesting traces",
            }
        )


class AsyncView(View):
    """
    Async view with two concurrent tasks.
    Demonstrates: asyncio.task.create fires twice, loop ticks stay fast.
    Both tasks yield properly so the event loop stays healthy.
    """

    async def get(self, request):
        async def fetch_a():
            await asyncio.sleep(
                0.01
            )  # yields to event loop — healthy await
            return "result_a"

        async def fetch_b():
            await asyncio.sleep(
                0.02
            )  # yields to event loop — healthy await
            return "result_b"

        result_a, result_b = await asyncio.gather(
            asyncio.create_task(fetch_a()),
            asyncio.create_task(fetch_b()),
        )

        return JsonResponse(
            {
                "view": "async",
                "result_a": result_a,
                "result_b": result_b,
                "note": "Two tasks ran concurrently. Check asyncio.loop.tick avg_duration_ns — should be < 1ms.",
            }
        )


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
        time.sleep(
            0.2
        )  # ← intentional blocking call for demonstration

        return JsonResponse(
            {
                "view": "slow",
                "blocked_ms": 200,
                "note": (
                    "time.sleep() blocked the event loop. "
                    "Run CAUSAL in the StackTracer REPL to detect it. "
                    "asyncio.loop.tick avg_duration_ns will be >> 10ms."
                ),
            }
        )


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

        return JsonResponse(
            {
                "view": "db",
                "user_count": count,
                "note": (
                    "ORM query ran via sync_to_async. "
                    "Check db.query.start and db.query.end in the REPL. "
                    "Run: SHOW latency WHERE system = 'database'"
                ),
            }
        )


# django_tracer/views.py


class NPlusOneView(View):
    """
    Simulates the N+1 query problem.

    The bug: fetch all authors, then for each author fire a separate
    query to get their books. 10 authors = 11 queries (1 + 10).

    The fix (commented out below): use select_related() or
    prefetch_related() to fetch everything in 1-2 queries.

    StackTracer will show N+1 django.db.query events all sharing
    the same trace_id — the signature of this problem in the graph.

    # run once in django shell: python manage.py shell to seed some data
    from django_tracer.models import Author, Book

    for i in range(10):
        a = Author.objects.create(name=f"Author {i}")
        for j in range(5):
            Book.objects.create(title=f"Book {i}-{j}", author=a)

    request.entry        django::/n1/
    django.view.enter    django::NPlusOneView
    django.db.query      SELECT * FROM author              ← query 1
    django.db.query      SELECT * FROM book WHERE author=1 ← query 2
    django.db.query      SELECT * FROM book WHERE author=2 ← query 3
    django.db.query      SELECT * FROM book WHERE author=3 ← query 4
    ...                  (10 more identical queries)
    django.view.exit     django::NPlusOneView
    request.exit         django::/n1/
    """

    def get(self, request):
        from django_tracer.models import (
            Author,
        )  # adjust to your app

        # Query 1 — fetch all authors
        from stacktracer.context.vars import get_trace_id

        print(f">>> trace_id in view: {get_trace_id()}")
        authors = list(Author.objects.all())

        results = []
        for author in authors:
            # Query 2..N — one per author, fetching their books separately
            # This is the bug. Django does NOT batch these automatically.
            books = list(author.book_set.all())
            results.append(
                {
                    "author": author.name,
                    "book_count": len(books),
                }
            )

        # The fix — replace the two lines above with:
        # authors = Author.objects.prefetch_related("book_set").all()
        # then the loop does zero extra queries

        return JsonResponse(
            {"authors": results, "query_count": len(authors) + 1}
        )
