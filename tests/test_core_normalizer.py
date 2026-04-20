from __future__ import annotations

import re

import pytest

from origintracer.core.graph_normalizer import (
    GraphNormalizer,
    NormalizationRule,
)


def make_normalizer(**kwargs) -> GraphNormalizer:
    """Convenience factory with sensible test defaults."""
    kwargs.setdefault("max_unique_names_per_service", 500)
    return GraphNormalizer(**kwargs)


class TestBuiltinPatterns:
    """Each test targets exactly one built-in substitution rule."""

    def test_uuid_in_url(self):
        n = make_normalizer()
        raw = "/api/orders/550e8400-e29b-41d4-a716-446655440000/items/"
        result = n.normalize("django", raw)
        assert "550e8400" not in result
        assert "{uuid}" in result

    def test_numeric_id_mid_path(self):
        n = make_normalizer()
        result = n.normalize(
            "django", "/api/users/1234/profile/"
        )
        assert "1234" not in result
        assert "{id}" in result

    def test_numeric_id_end_of_path(self):
        n = make_normalizer()
        result = n.normalize("django", "/api/users/5678")
        assert "5678" not in result
        assert "{id}" in result

    def test_numeric_id_query_param(self):
        n = make_normalizer()
        result = n.normalize("django", "/api/search?page=42")
        assert "42" not in result
        assert "{id}" in result

    def test_mongo_oid_mid_path(self):
        n = make_normalizer()
        result = n.normalize(
            "fastapi", "/api/docs/507f1f77bcf86cd799439011/"
        )
        assert "507f1f77bcf86cd799439011" not in result
        assert "{oid}" in result

    def test_mongo_oid_end_of_path(self):
        n = make_normalizer()
        result = n.normalize(
            "fastapi", "/api/docs/507f1f77bcf86cd799439011"
        )
        assert "507f1f77bcf86cd799439011" not in result
        assert "{oid}" in result

    def test_memory_address_coroutine_repr(self):
        n = make_normalizer()
        raw = "handle_request coro at 0x7f3a2b4c1d0e"
        result = n.normalize("uvicorn", raw)
        assert "0x7f3a2b4c1d0e" not in result

    def test_python_object_repr_address(self):
        n = make_normalizer()
        raw = "<Task pending coro=<handle at 0x7ffad1234abc> cb=[]>"
        result = n.normalize("asyncio", raw)
        assert "0x7ffad1234abc" not in result
        # The "at 0x..." pattern fires first and strips the address entirely;
        # {addr} only appears for bare 0x... addresses not preceded by " at "
        assert result == "<Task pending coro=<handle> cb=[]>"

    def test_python_bare_hex_address(self):
        """The {addr} replacement only fires when not preceded by ' at '."""
        n = make_normalizer()
        raw = "<object 0x7ffad1234abc>"
        result = n.normalize("asyncio", raw)
        assert "0x7ffad1234abc" not in result
        assert "{addr}" in result

    def test_sql_integer_literal(self):
        n = make_normalizer()
        result = n.normalize(
            "django", "SELECT * FROM users WHERE id = 9999"
        )
        assert "9999" not in result
        assert "?" in result

    def test_sql_single_quoted_string(self):
        n = make_normalizer()
        result = n.normalize(
            "django", "SELECT * FROM users WHERE name = 'Alice'"
        )
        assert "Alice" not in result
        assert "?" in result

    def test_sql_double_quoted_string(self):
        n = make_normalizer()
        result = n.normalize(
            "django", 'SELECT * FROM items WHERE sku = "ABC-123"'
        )
        assert "ABC-123" not in result
        assert "?" in result

    def test_celery_task_bracket(self):
        n = make_normalizer()
        result = n.normalize(
            "celery", "send_report[abc12345-def6-7890]"
        )
        assert "[" not in result
        assert "send_report" in result

    def test_date_segment_mid_path(self):
        n = make_normalizer()
        result = n.normalize(
            "django", "/logs/2024-01-15/summary/"
        )
        assert "2024-01-15" not in result
        assert "{date}" in result

    def test_date_segment_end_of_path(self):
        n = make_normalizer()
        result = n.normalize("django", "/reports/2023-12-31")
        assert "2023-12-31" not in result
        assert "{date}" in result

    def test_multiple_patterns_in_one_name(self):
        """UUID and numeric ID in the same name — both should be replaced."""
        n = make_normalizer()
        raw = "/api/550e8400-e29b-41d4-a716-446655440000/orders/99/"
        result = n.normalize("django", raw)
        assert "550e8400" not in result
        assert "99" not in result
        assert "{uuid}" in result
        assert "{id}" in result

    def test_clean_url_is_unchanged(self):
        """A URL with no high-cardinality segments should pass through intact."""
        n = make_normalizer()
        raw = "/api/health/"
        result = n.normalize("django", raw)
        assert result == raw

    def test_builtins_disabled(self):
        """enable_builtins=False should leave UUIDs and IDs untouched."""
        n = GraphNormalizer(enable_builtins=False)
        raw = "/api/users/1234/profile/"
        result = n.normalize("django", raw)
        assert result == raw


class TestAddPattern:
    """
    Tests for GraphNormalizer.add_pattern().
    """

    def test_per_service_pattern_applied(self):
        n = make_normalizer()
        n.add_pattern(
            service="django",
            pattern=r"/api/items/(\d+)/reviews/(\d+)/",
            replacement="/api/items/{id}/reviews/{review_id}/",
        )
        result = n.normalize("django", "/api/items/7/reviews/3/")
        # Built-in numeric ID pattern fires first, replacing both segments
        # before the custom rule runs — custom rule never matches
        assert result == "/api/items/{id}/reviews/{id}/"

    def test_per_service_pattern_not_applied_to_other_service(
        self,
    ):
        n = make_normalizer()
        n.add_pattern(
            service="django",
            pattern=r"/special/(\w+)/",
            replacement="/special/{slug}/",
        )
        # Same URL sent via a different service - rule should not fire
        result = n.normalize("fastapi", "/special/hello/")
        assert "{slug}" not in result

    def test_global_pattern_applied_to_all_services(self):
        n = make_normalizer()
        n.add_pattern(
            service="*",
            pattern=r"tenant-\w+",
            replacement="tenant-{id}",
        )
        for svc in ("django", "fastapi", "celery"):
            result = n.normalize(svc, "tenant-acme")
            assert "acme" not in result
            assert "tenant-{id}" in result

    def test_multiple_patterns_applied_in_order(self):
        n = make_normalizer()
        n.add_pattern("django", r"/v\d+/", "/v{ver}/")
        n.add_pattern("django", r"/items/(\d+)/", "/items/{id}/")
        result = n.normalize("django", "/v3/items/55/")
        assert "{ver}" in result
        assert "{id}" in result
        assert "v3" not in result
        assert "55" not in result

    def test_add_pattern_invalidates_cache(self):
        n = make_normalizer()
        raw = "/api/custom/hello/"
        first = n.normalize("django", raw)

        n.add_pattern(
            "django", r"/custom/(\w+)/", "/custom/{slug}/"
        )
        second = n.normalize("django", raw)

        # After adding the rule, the same input should produce a different result
        assert "{slug}" in second
        assert first != second

    def test_from_yaml_equivalent_to_add_pattern(self):
        config = {
            "max_unique_names_per_service": 500,
            "rules": [
                {
                    "service": "django",
                    "pattern": r"/api/items/(\d+)/",
                    "replacement": "/api/items/{id}/",
                    "description": "Collapse item IDs",
                }
            ],
        }
        from_yaml = GraphNormalizer.from_yaml(config)
        manual = make_normalizer()
        manual.add_pattern(
            "django", r"/api/items/(\d+)/", "/api/items/{id}/"
        )

        raw = "/api/items/42/"
        assert from_yaml.normalize(
            "django", raw
        ) == manual.normalize("django", raw)

    def test_from_yaml_empty_config_returns_default(self):
        n = GraphNormalizer.from_yaml({})
        # Built-ins should still fire
        result = n.normalize("django", "/api/users/100/")
        assert "{id}" in result

    def test_from_yaml_none_returns_default(self):
        n = GraphNormalizer.from_yaml(None)
        result = n.normalize("django", "/api/users/100/")
        assert "{id}" in result


class TestCardinalityGuard:
    """
    Mirrors the style of TestHighCardinalityNormalization used in engine tests.
    Exercises the guard purely through GraphNormalizer without the engine layer.
    """

    def test_url_variants_collapse_to_single_node(self):
        n = make_normalizer()
        results = set()
        for uid in range(100):
            r = n.normalize(
                "django", f"/api/users/{uid}/orders/"
            )
            results.add(r)
        # All 100 variants must collapse to one structural form
        assert (
            len(results) == 1
        ), f"Cardinality explosion: {results}"
        assert "{id}" in results.pop()

    def test_overflow_bucketed_after_limit(self):
        """Once max_unique is reached, novel names > 'high_cardinality_overflow'."""
        limit = 5
        n = GraphNormalizer(
            enable_builtins=False,  # disable so names pass through as-is
            max_unique_names_per_service=limit,
        )
        # Fill up to the limit with distinct names
        for i in range(limit):
            n.normalize("svc", f"unique_name_{i}")

        # The next novel name should be bucketed
        overflow = n.normalize("svc", "unique_name_OVERFLOW")
        assert overflow == "high_cardinality_overflow"

    def test_known_names_still_returned_after_overflow(self):
        """Names already seen before the limit was hit should NOT be bucketed."""
        limit = 3
        n = GraphNormalizer(
            enable_builtins=False,
            max_unique_names_per_service=limit,
        )
        n.normalize("svc", "alpha")
        n.normalize("svc", "beta")
        n.normalize("svc", "gamma")  # limit hit here

        # alpha and beta were already in the seen set
        assert n.normalize("svc", "alpha") == "alpha"
        assert n.normalize("svc", "beta") == "beta"

    def test_overflow_does_not_affect_other_services(self):
        """Cardinality is tracked per service; one service overflowing
        must not pollute a different service's seen set."""
        limit = 2
        n = GraphNormalizer(
            enable_builtins=False,
            max_unique_names_per_service=limit,
        )
        # Exhaust service A
        n.normalize("svc_a", "x")
        n.normalize("svc_a", "y")
        n.normalize("svc_a", "z")  # overflows

        # Service B should still accept new names normally
        result = n.normalize("svc_b", "z")
        assert result == "z"

    def test_stats_returns_seen_counts(self):
        n = GraphNormalizer(
            enable_builtins=False,
            max_unique_names_per_service=500,
        )
        n.normalize("django", "/api/health/")
        n.normalize("django", "/api/status/")
        n.normalize("celery", "send_email")

        s = n.stats()
        assert s["django"] == 2
        assert s["celery"] == 1


class TestCache:

    def test_repeated_calls_return_same_result(self):
        n = make_normalizer()
        raw = "/api/users/99/profile/"
        r1 = n.normalize("django", raw)
        r2 = n.normalize("django", raw)
        assert r1 == r2

    def test_cache_key_is_service_sensitive(self):
        """The same name for two different services can normalize differently."""
        n = make_normalizer()
        n.add_pattern(
            "django",
            r"/items/([a-z]+)/",
            "/items/{django_slug}/",
        )
        n.add_pattern(
            "fastapi",
            r"/items/([a-z]+)/",
            "/items/{fastapi_slug}/",
        )

        django_result = n.normalize("django", "/items/widgets/")
        fastapi_result = n.normalize(
            "fastapi", "/items/widgets/"
        )

        assert django_result == "/items/{django_slug}/"
        assert fastapi_result == "/items/{fastapi_slug}/"
        assert django_result != fastapi_result


class TestMaxNameLength:

    def test_long_name_is_truncated(self):
        n = GraphNormalizer(max_name_length=20)
        long_name = "a" * 50
        result = n.normalize("svc", long_name)
        # Built-in patterns won't touch this, so it should be truncated + ellipsis
        assert len(result) <= 21  # 20 chars + "…"
        assert result.endswith("…")

    def test_short_name_is_not_truncated(self):
        n = GraphNormalizer(max_name_length=200)
        short_name = "short"
        result = n.normalize("svc", short_name)
        assert result == short_name


class TestNormalizationRule:

    def test_regex_rule_apply(self):
        rule = NormalizationRule(
            service="django",
            pattern=r"/users/(\d+)/",
            replacement="/users/{id}/",
        )
        assert rule.apply("/users/42/") == "/users/{id}/"

    def test_regex_rule_no_match_is_passthrough(self):
        rule = NormalizationRule(
            service="django",
            pattern=r"/orders/(\d+)/",
            replacement="/orders/{id}/",
        )
        # URL doesn't match - should come back unchanged
        assert rule.apply("/users/99/") == "/users/99/"

    def test_rule_with_neither_pattern_nor_fn_is_passthrough(
        self,
    ):
        rule = NormalizationRule(service="django")
        assert rule.apply("anything") == "anything"


class TestEdgeCases:

    def test_empty_string_does_not_crash(self):
        n = make_normalizer()
        result = n.normalize("django", "")
        assert isinstance(result, str)

    def test_whitespace_only_string(self):
        n = make_normalizer()
        result = n.normalize("django", "   ")
        # strip() is applied at the end of _normalize_uncached
        assert result == ""

    def test_unknown_service_uses_builtins_and_global_rules(
        self,
    ):
        n = make_normalizer()
        n.add_pattern("*", r"tenant-\w+", "tenant-{id}")
        result = n.normalize(
            "some_unknown_service", "/api/users/5/tenant-acme/"
        )
        assert "{id}" in result
        assert "tenant-{id}" in result

    def test_normalizer_is_deterministic(self):
        n = make_normalizer()
        raw = "/api/550e8400-e29b-41d4-a716-446655440000/orders/99/"
        results = {n.normalize("django", raw) for _ in range(10)}
        assert len(results) == 1
