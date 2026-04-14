"""
GET /  - sync view — baseline, hits no database
GET /async/  - async view — exercises asyncio probes
GET /slow/  - async view with a simulated blocking call
              triggers the loop_starvation causal rule
GET /db/  - async view that queries the database
            triggers db.query probes
GET /cascade/  - calls down internal service
GET /regression/  - calls
GET /external/  - calls
GET /payment/  - calls
"""

import asyncio
import time

import requests
from django.http import JsonResponse
from django.views import View


class IndexView(View):
    """
    Synchronous baseline view.
    Demonstrates: request.entry >> django.middleware >> django.view
    >> request.exit
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
            20
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
        from asgiref.sync import sync_to_async
        from django.contrib.auth import get_user_model
        from django.db import connection

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

    The fix: use select_related() or prefetch_related() to fetch everything
    in 1-2 queries.

    OriginTracer will show N+1 django.db.query events all sharing
    the same trace_id - the signature of this problem in the graph.

    # run once in django shell: python manage.py shell to seed some data
    from django_tracer.models import Author, Book

    for i in range(10):
        a = Author.objects.create(name=f"Author {i}")
        for j in range(5):
            Book.objects.create(title=f"Book {i}-{j}", author=a)

    request.entry >> django::/n1/
    django.view.enter >> django::NPlusOneView
    django.db.query >> SELECT * FROM author < query 1
    django.db.query >> SELECT * FROM book WHERE author=1 < query 2
    django.db.query >> SELECT * FROM book WHERE author=2 < query 3
    django.db.query >> SELECT * FROM book WHERE author=3 < query 4
    ...                (10 more identical queries)
    django.view.exit >> django::NPlusOneView
    request.exit >> django::/n1/
    """

    def get(self, request):
        # adjust to your app
        # Query 1 — fetch all authors
        from django_tracer.models import (
            Author,
        )
        from origintracer.context.vars import get_trace_id

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


class CascadeView(View):
    """
    Calls an internal service that may be down.
    When the upstream worker dies, cascade_failure fires.
    Kill one gunicorn worker while hitting this endpoint.

    GET /cascade/
    """

    def get(self, request):
        try:
            # calls itself on a different port simulating internal service
            resp = requests.get(
                "http://127.0.0.1:8001/", timeout=1
            )
            return JsonResponse(
                {"upstream": "ok", "status": resp.status_code}
            )
        except requests.exceptions.ConnectionError:
            return JsonResponse({"upstream": "down"}, status=503)


class RegressionView(View):
    """
    A view that got a new ORM call added in a recent deployment.
    Run MARK DEPLOYMENT before adding the extra query to establish
    baseline, then hit this endpoint — post_deployment_regression fires.

    GET /regression/
    """

    async def get(self, request):
        from asgiref.sync import sync_to_async
        from django.contrib.auth import get_user_model

        from django_tracer.models import Author

        User = get_user_model()

        @sync_to_async
        def queries():
            count = User.objects.count()
            # this line was added in new deployment — new edge in graph
            authors = list(Author.objects.all()[:5])
            return count, authors

        count, authors = await queries()
        return JsonResponse(
            {
                "users": count,
                "authors": len(authors),
                "note": "Run MARK DEPLOYMENT before this view to detect regression",
            }
        )


class ExternalView(View):
    """
    Calls an external service with a deliberate slow response.
    Use httpx to hit a slow endpoint — external_dependency_timeout fires
    when avg response > 2s.

    GET /external/
    pip install httpx first.
    """

    async def get(self, request):
        import httpx

        try:
            async with httpx.AsyncClient(timeout=3.0) as client:
                # httpbin's delay endpoint simulates a slow external service
                resp = await client.get(
                    "https://httpbin.org/delay/3"
                )
                return JsonResponse(
                    {
                        "external": "ok",
                        "status": resp.status_code,
                    }
                )
        except httpx.TimeoutException:
            return JsonResponse(
                {"external": "timeout"}, status=504
            )


class PaymentView(View):
    """
    Demonstrates duplicate write — payment saved twice per request.
    duplicate_transaction fires when INSERT ratio > 1.5x per view call.

    GET /payment/
    Requires: Payment model with amount field.
    """

    def get(self, request):
        from django_tracer.models import Payment

        # bug — create() already saves, .save() fires a second INSERT
        payment = Payment(amount=100)
        payment.save()  # INSERT 1
        payment.save()  # INSERT 2 — duplicate, same object
        return JsonResponse(
            {"payment_id": payment.id, "note": "double save bug"}
        )
