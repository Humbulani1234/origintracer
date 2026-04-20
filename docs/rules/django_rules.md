# Django Causal Rules

Two rules observe database behaviour in the RuntimeGraph.

---

## N+1 Queries

**Confidence:** 0.90  
**Tags:** `db` `performance` `n+1`

### What it detects

A database query node whose `call_count` is 5x or more than the view node
that called it — the classic ORM N+1 pattern where a query fires inside a
loop iterating over a queryset.

### Why this matters

```
# What you wrote
for author in Author.objects.all():        # 1 query
    print(author.books.count())            # 1 query per author

# What hits the database
SELECT * FROM authors                      # 1
SELECT * FROM books WHERE author_id=1      # N
SELECT * FROM books WHERE author_id=2      # N
...                                        # 100 authors = 101 queries
```

Each individual query is fast. The problem is volume — 101 queries where
1 would do. Under load this saturates the connection pool before any single
query looks slow in isolation.

### Firing conditions

| Condition | Detail |
|---|---|
| Node is a DB node | `metadata.probe` starts with `django.db` |
| Query `call_count` ≥ 5 | Filters out one-off queries |
| `query.call_count / view.call_count` ≥ 5x | Threshold for N+1 classification |
| View caller exists | Looks up to two hops — direct caller and grandcaller |

### Return payload

```python
{
    "n_plus_one_patterns": [
        {
            "query":         str,    # node ID e.g. "django::SELECT book (author_id=%s)"
            "view":          str,    # node ID e.g. "django::NPlusOneView"
            "query_count":   int,
            "view_count":    int,
            "ratio":         float,  # e.g. 10.0
            "avg_query_ms":  float,
            "hint":          str,    # e.g. "Use select_related()... wasted queries: 9"
        }
        # up to 10 hits, sorted by ratio descending
    ]
}
```

### Real example

```
django::NPlusOneView          call_count=1
django::SELECT author         call_count=1   ← fine
django::SELECT book (author_id=%s)  call_count=10   ← N+1, ratio=10x
```

### Fix

```python
# Before
queryset = Author.objects.all()

# After — one query with a JOIN
queryset = Author.objects.select_related("book_set")

# Or for reverse/many relations
queryset = Author.objects.prefetch_related("books")
```

### Tuning

The threshold is `THRESHOLD = 5` in `_n_plus_one_queries`. Lower it to
catch subtler patterns, raise it to reduce noise on batch endpoints that
legitimately run multiple queries per view.

---

## DB Query Hotspot

**Confidence:** 0.70  
**Tags:** `db` `performance`

### What it detects

A single query pattern accounts for more than 30% of all observed database
calls. Distinct from N+1 — fires even without a view caller, for example a
Celery task hammering the same query in a background loop.

### Why this matters

A hotspot query is a single point of failure for your database. At 30% share
it dominates connection time, lock contention, and cache eviction. The
confidence is lower than N+1 (0.70 vs 0.90) because high share alone does
not confirm a problem — a deliberately cached hot read is fine. The payload
gives you the data to decide.

### Firing conditions

| Condition | Detail |
|---|---|
| DB nodes exist | At least one `django.db` node in the graph |
| Node `call_count` > 5 | Filters transient one-off queries |
| Node share > 30% | `node.call_count / total_db_calls > 0.30` |

!!! note
    `_is_db_node()` checks `metadata.probe`, not `node_type`, because Django
    DB query nodes carry `node_type="django"` (the probe prefix). Filtering
    on `node_type` would miss them entirely.

### Return payload

```python
{
    "hotspot_queries": [
        {
            "node":       str,    # node ID e.g. "django::SELECT * FROM sessions"
            "call_count": int,
            "pct":        float,  # e.g. 43.2 (percent of all DB calls)
            "avg_ms":     float,
        }
    ]
}
```

### What to do when this fires

1. Check `pct` — above 50% is severe, 30–50% warrants investigation
2. If the query is a read — add a cache layer (`django-cache-machine`,
   Redis) or materialise the result
3. If the query is a write — check for missing bulk operations
   (`bulk_create`, `bulk_update`)
4. If it is a background task — check whether the task is being dispatched
   too aggressively or whether results can be batched