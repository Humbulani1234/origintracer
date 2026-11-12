"""LangGraph Server authentication for OriginTracer end-user sessions."""

from __future__ import annotations

import os
from typing import Any, cast

import httpx
from langgraph_sdk import Auth

auth = Auth()


def _backend_url() -> str:
    """Return the configured OriginTracer API root."""
    return os.getenv(
        "ORIGINTRACER_API_URL", "http://127.0.0.1:8001"
    ).rstrip("/")


@auth.authenticate
async def authenticate_user(
    authorization: str | None,
) -> Auth.types.MinimalUserDict:
    """Validate an OriginTracer OIDC token and expose its identity to the graph.

    Parameters
    ----------
    authorization : str or None
        Bearer access token sent by the authenticated frontend.

    Returns
    -------
    Auth.types.MinimalUserDict
        Verified subject and the short-lived access token for scoped backend
        tool requests.

    Raises
    ------
    Auth.exceptions.HTTPException
        If the token is missing, invalid, or the OriginTracer API is unavailable.
    """
    if not authorization or not authorization.lower().startswith(
        "bearer "
    ):
        raise Auth.exceptions.HTTPException(
            status_code=401, detail="Sign in is required."
        )

    token = authorization[7:].strip()
    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            response = await client.get(
                f"{_backend_url()}/api/v1/auth/me",
                headers={"Authorization": f"Bearer {token}"},
            )
    except httpx.HTTPError as exc:
        raise Auth.exceptions.HTTPException(
            status_code=503,
            detail="OriginTracer authentication is unavailable.",
        ) from exc

    if response.status_code != 200:
        raise Auth.exceptions.HTTPException(
            status_code=401,
            detail="The sign-in token is invalid or expired.",
        )

    try:
        identity = (
            response.json().get("subject")
            or response.json()["customer_id"]
        )
    except (ValueError, KeyError, TypeError) as exc:
        raise Auth.exceptions.HTTPException(
            status_code=401,
            detail="The sign-in token could not be verified.",
        ) from exc

    # LangGraph passes authenticated user data to graph runtime configuration.
    # The token is kept out of graph state and message history.
    return cast(
        Auth.types.MinimalUserDict,
        {
            "identity": str(identity),
            "display_name": str(identity),
            # Preserve this runtime-only field so tools can make user-scoped
            # backend calls. LangGraph's public user TypedDict omits it.
            "access_token": token,
        },
    )


@auth.on
async def deny_unhandled_requests(
    ctx: Auth.types.AuthContext,
    value: Any,
) -> bool:
    """Deny every LangGraph API operation without an explicit policy."""
    return False


@auth.on.threads.create
async def create_owned_thread(
    ctx: Auth.types.AuthContext,
    value: Auth.types.ThreadsCreate,
) -> None:
    """Attach the authenticated subject as owner of each new conversation."""
    metadata = value.setdefault("metadata", {})
    metadata["owner"] = ctx.user.identity


def _owner_filter(
    ctx: Auth.types.AuthContext,
) -> Auth.types.FilterType:
    """Return the thread metadata filter for the authenticated caller."""
    return {"owner": ctx.user.identity}


@auth.on.threads.read
async def scope_thread_read(
    ctx: Auth.types.AuthContext, value: Auth.types.ThreadsRead
) -> Auth.types.FilterType:
    """Restrict thread reads to the authenticated caller's threads."""
    return _owner_filter(ctx)


@auth.on.threads.search
async def scope_thread_search(
    ctx: Auth.types.AuthContext, value: Auth.types.ThreadsSearch
) -> Auth.types.FilterType:
    """Restrict thread search to the authenticated caller's threads."""
    return _owner_filter(ctx)


@auth.on.threads.update
async def scope_thread_update(
    ctx: Auth.types.AuthContext, value: Auth.types.ThreadsUpdate
) -> Auth.types.FilterType:
    """Restrict thread updates to the authenticated caller's threads."""
    return _owner_filter(ctx)


@auth.on.threads.delete
async def scope_thread_delete(
    ctx: Auth.types.AuthContext, value: Auth.types.ThreadsDelete
) -> Auth.types.FilterType:
    """Restrict thread deletion to the authenticated caller's threads."""
    return _owner_filter(ctx)


@auth.on.threads.create_run
async def scope_thread_run(
    ctx: Auth.types.AuthContext, value: Auth.types.RunsCreate
) -> Auth.types.FilterType:
    """Restrict runs to the authenticated caller's threads."""
    return _owner_filter(ctx)


@auth.on.assistants.read
async def allow_assistant_read(
    ctx: Auth.types.AuthContext,
    value: Auth.types.AssistantsRead,
) -> bool:
    """Allow signed-in users to read the configured investigation graph."""
    return True


@auth.on.assistants.search
async def allow_assistant_search(
    ctx: Auth.types.AuthContext,
    value: Auth.types.AssistantsSearch,
) -> bool:
    """Allow signed-in users to find the configured investigation graph."""
    return True
