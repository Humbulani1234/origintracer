"""Authentication primitives for the OriginTracer FastAPI service.

The service accepts long-lived API keys for instrumented workers and signed
OIDC access tokens for people managing those keys. Account data is currently
scoped to the OIDC subject; organization membership can be added separately.
"""

from __future__ import annotations

import hashlib
import secrets
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import jwt
from jwt import PyJWKClient


@dataclass(frozen=True)
class Principal:
    """Identity established from a verified credential."""

    customer_id: str
    method: str
    subject: str | None = None


class ApiKeyStore:
    """Issue, verify, list, and revoke customer-scoped API keys.

    Parameters
    ----------
    connection : Any or None
        Dedicated PostgreSQL connection. If omitted, keys are held in memory
        and are lost when the backend process restarts.
    """

    def __init__(self, connection: Any = None) -> None:
        self._connection = connection
        self._lock = threading.RLock()
        self._memory: dict[str, dict[str, Any]] = {}
        if self._connection is not None:
            with self._connection.cursor() as cursor:
                cursor.execute(
                    """CREATE TABLE IF NOT EXISTS origintracer_api_keys (
                        key_id TEXT PRIMARY KEY,
                        customer_id TEXT NOT NULL,
                        name TEXT NOT NULL,
                        key_prefix TEXT NOT NULL,
                        key_hash TEXT NOT NULL UNIQUE,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        revoked_at TIMESTAMPTZ NULL
                    )"""
                )
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_ot_api_keys_customer "
                    "ON origintracer_api_keys (customer_id, created_at DESC)"
                )
            self._connection.commit()

    @staticmethod
    def _digest(key: str) -> str:
        return hashlib.sha256(key.encode("utf-8")).hexdigest()

    def create(
        self, customer_id: str, name: str
    ) -> tuple[str, dict[str, Any]]:
        """Create a key and return its plaintext once with safe metadata.

        Parameters
        ----------
        customer_id : str
            Account namespace that owns the key.
        name : str
            Human-readable key label.

        Returns
        -------
        tuple[str, dict[str, Any]]
            Plaintext key and metadata safe to return after creation.
        """
        key = f"ot_live_{secrets.token_urlsafe(32)}"
        row = {
            "key_id": str(uuid.uuid4()),
            "customer_id": customer_id,
            "name": name,
            "key_prefix": key[:16],
            "key_hash": self._digest(key),
            "created_at": datetime.now(timezone.utc).isoformat(),
            "revoked_at": None,
        }
        with self._lock:
            if self._connection is None:
                self._memory[row["key_id"]] = row
            else:
                with self._connection.cursor() as cursor:
                    cursor.execute(
                        """INSERT INTO origintracer_api_keys
                           (key_id, customer_id, name, key_prefix, key_hash)
                           VALUES (%s, %s, %s, %s, %s)""",
                        (
                            row["key_id"],
                            customer_id,
                            name,
                            row["key_prefix"],
                            row["key_hash"],
                        ),
                    )
                self._connection.commit()
        return key, self._public(row)

    def verify(self, key: str) -> str | None:
        """Return the owning account for an active key, if it exists."""
        digest = self._digest(key)
        with self._lock:
            if self._connection is None:
                return next(
                    (
                        row["customer_id"]
                        for row in self._memory.values()
                        if row["key_hash"] == digest
                        and row["revoked_at"] is None
                    ),
                    None,
                )
            with self._connection.cursor() as cursor:
                cursor.execute(
                    """SELECT customer_id FROM origintracer_api_keys
                       WHERE key_hash = %s AND revoked_at IS NULL""",
                    (digest,),
                )
                result = cursor.fetchone()
            return result[0] if result else None

    def list_for_customer(
        self, customer_id: str
    ) -> list[dict[str, Any]]:
        """Return safe metadata for keys owned by one account."""
        with self._lock:
            if self._connection is None:
                rows = [
                    row
                    for row in self._memory.values()
                    if row["customer_id"] == customer_id
                ]
            else:
                with self._connection.cursor() as cursor:
                    cursor.execute(
                        """SELECT key_id, customer_id, name, key_prefix,
                                  created_at, revoked_at
                           FROM origintracer_api_keys
                           WHERE customer_id = %s
                           ORDER BY created_at DESC""",
                        (customer_id,),
                    )
                    rows = [
                        dict(
                            zip(
                                (
                                    "key_id",
                                    "customer_id",
                                    "name",
                                    "key_prefix",
                                    "created_at",
                                    "revoked_at",
                                ),
                                result,
                            )
                        )
                        for result in cursor.fetchall()
                    ]
        return [self._public(row) for row in rows]

    def revoke(self, customer_id: str, key_id: str) -> bool:
        """Revoke a key only when it belongs to the requesting account."""
        now = datetime.now(timezone.utc).isoformat()
        with self._lock:
            if self._connection is None:
                row = self._memory.get(key_id)
                if not row or row["customer_id"] != customer_id:
                    return False
                row["revoked_at"] = now
                return True
            with self._connection.cursor() as cursor:
                cursor.execute(
                    """UPDATE origintracer_api_keys SET revoked_at = NOW()
                       WHERE key_id = %s AND customer_id = %s
                         AND revoked_at IS NULL""",
                    (key_id, customer_id),
                )
                changed = cursor.rowcount > 0
            self._connection.commit()
            return changed

    def customers(self) -> set[str]:
        """Return account IDs with issued keys for startup snapshot loading."""
        with self._lock:
            if self._connection is None:
                return {
                    row["customer_id"]
                    for row in self._memory.values()
                }
            with self._connection.cursor() as cursor:
                cursor.execute(
                    "SELECT DISTINCT customer_id FROM origintracer_api_keys"
                )
                return {row[0] for row in cursor.fetchall()}

    @staticmethod
    def _public(row: dict[str, Any]) -> dict[str, Any]:
        return {
            key: value
            for key, value in row.items()
            if key not in {"key_hash", "customer_id"}
        }


class OIDCVerifier:
    """Verify OIDC access tokens against the provider's signing keys.

    Parameters
    ----------
    issuer : str
        Exact OIDC issuer URL configured by the identity provider.
    audience : str
        Audience claim assigned to OriginTracer API access tokens.
    jwks_url : str or None
        Provider JSON Web Key Set URL. Defaults to Keycloak's standard URL.
    """

    def __init__(
        self,
        issuer: str,
        audience: str,
        jwks_url: str | None = None,
    ) -> None:
        self.issuer = issuer.rstrip("/")
        self.audience = audience
        self.jwks_url = jwks_url or (
            f"{self.issuer}/protocol/openid-connect/certs"
        )
        self._jwks = PyJWKClient(self.jwks_url, cache_keys=True)

    def verify(self, token: str) -> Principal:
        """Verify signature and standard claims; scope the account by subject."""
        signing_key = self._jwks.get_signing_key_from_jwt(token)
        claims = jwt.decode(
            token,
            signing_key.key,
            algorithms=["RS256"],
            audience=self.audience,
            issuer=self.issuer,
            options={
                "require": ["exp", "iat", "sub", "iss", "aud"]
            },
        )
        subject = claims.get("sub")
        if not isinstance(subject, str) or not subject:
            raise jwt.InvalidTokenError("Token has no subject")
        return Principal(
            customer_id=subject, method="oidc", subject=subject
        )


def authenticate(
    token: str,
    configured_keys: dict[str, str],
    key_store: ApiKeyStore,
    oidc: OIDCVerifier | None,
) -> Principal | None:
    """Resolve a bearer token to a configured key, issued key, or OIDC user."""
    customer_id = configured_keys.get(token)
    if customer_id:
        return Principal(
            customer_id=customer_id, method="configured_api_key"
        )
    customer_id = key_store.verify(token)
    if customer_id:
        return Principal(
            customer_id=customer_id, method="api_key"
        )
    if oidc is not None:
        return oidc.verify(token)
    return None
