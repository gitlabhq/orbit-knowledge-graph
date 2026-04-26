"""
Async wrapper around the generated OpenCode SDK client.

Provides the same interface the harness expects (OpenCodeClient, typed responses)
while delegating all HTTP calls to the openapi-python-client generated code.

To regenerate the SDK: mise run eval:sync-spec
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any

import httpx

from opencode_sdk.client import Client
from opencode_sdk.api.default import (
    session_create,
    session_delete,
    session_abort,
    session_list,
    session_messages,
    session_prompt,
    session_status,
    session_todo,
    session_diff,
)
from opencode_sdk.api.session import session_get, session_children
from opencode_sdk.api.default import global_health
from opencode_sdk.models.session_create_body import SessionCreateBody
from opencode_sdk.models.session_prompt_body import SessionPromptBody
from opencode_sdk.models.text_part_input import TextPartInput
from opencode_sdk.types import UNSET


def _to_dict(obj: Any) -> Any:
    """Convert an attrs-based SDK model to a plain dict, recursively."""
    if hasattr(obj, "to_dict"):
        return obj.to_dict()
    if isinstance(obj, list):
        return [_to_dict(item) for item in obj]
    return obj


# ---------------------------------------------------------------------------
# Thin wrappers so consumers can call .model_dump() like before.
# ---------------------------------------------------------------------------

class _DictMixin:
    """Wraps an attrs-based SDK model, adds model_dump() for compat."""

    _raw: Any

    def __getattr__(self, name: str) -> Any:
        return getattr(self._raw, name)

    def model_dump(self) -> dict[str, Any]:
        return _to_dict(self._raw)


class SessionInfo(_DictMixin):
    def __init__(self, raw: Any) -> None:
        object.__setattr__(self, "_raw", raw)

    @property
    def id(self) -> str:
        return self._raw.id

    @property
    def title(self) -> str:
        return self._raw.title

    @property
    def parentID(self) -> str | None:
        pid = getattr(self._raw, "parent_id", UNSET)
        return None if pid is UNSET else pid


class MessageInfo(_DictMixin):
    def __init__(self, raw: Any) -> None:
        object.__setattr__(self, "_raw", raw)

    @property
    def id(self) -> str:
        return self._raw.id

    @property
    def role(self) -> str:
        return self._raw.role

    @property
    def finish(self) -> str | None:
        val = getattr(self._raw, "finish", UNSET)
        return None if val is UNSET else val

    @property
    def cost(self) -> float:
        return getattr(self._raw, "cost", 0.0)

    @property
    def tokens(self) -> dict[str, Any]:
        t = getattr(self._raw, "tokens", UNSET)
        return _to_dict(t) if t is not UNSET else {}


class PartData(_DictMixin):
    def __init__(self, raw: Any) -> None:
        object.__setattr__(self, "_raw", raw)

    @property
    def type(self) -> str:
        return self._raw.type


class MessageWithParts(_DictMixin):
    def __init__(self, raw: Any) -> None:
        object.__setattr__(self, "_raw", raw)
        self.info = MessageInfo(raw.info)
        self.parts = [PartData(p) for p in raw.parts]

    def model_dump(self) -> dict[str, Any]:
        return {
            "info": self.info.model_dump(),
            "parts": [p.model_dump() for p in self.parts],
        }


class TodoItem(_DictMixin):
    def __init__(self, raw: Any) -> None:
        object.__setattr__(self, "_raw", raw)


class FileDiff(_DictMixin):
    def __init__(self, raw: Any) -> None:
        object.__setattr__(self, "_raw", raw)


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

@dataclass
class OpenCodeClient:
    """Async client for a single OpenCode server instance."""

    base_url: str
    _client: Client = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._client = Client(
            base_url=self.base_url,
            timeout=httpx.Timeout(60.0, connect=10.0),
            httpx_args={"limits": httpx.Limits(max_connections=20)},
        )

    async def close(self) -> None:
        async_client = self._client.get_async_httpx_client()
        await async_client.aclose()

    # -- health ---------------------------------------------------------------

    async def health_check(self) -> bool:
        try:
            resp = await global_health.asyncio_detailed(client=self._client)
            return resp.status_code == 200
        except httpx.HTTPError:
            return False

    async def wait_ready(self, timeout: float = 30.0, poll_interval: float = 0.5) -> None:
        deadline = asyncio.get_event_loop().time() + timeout
        while asyncio.get_event_loop().time() < deadline:
            if await self.health_check():
                return
            await asyncio.sleep(poll_interval)
        raise TimeoutError(f"OpenCode server at {self.base_url} not ready after {timeout}s")

    # -- session --------------------------------------------------------------

    async def create_session(self, title: str | None = None) -> SessionInfo:
        body = SessionCreateBody()
        if title:
            body.title = title
        result = await session_create.asyncio(client=self._client, body=body)
        if result is None:
            raise RuntimeError("session_create returned None")
        return SessionInfo(result)

    async def get_session(self, session_id: str) -> SessionInfo:
        result = await session_get.asyncio(client=self._client, session_id=session_id)
        if result is None:
            raise RuntimeError(f"session_get returned None for {session_id}")
        return SessionInfo(result)

    async def delete_session(self, session_id: str) -> bool:
        await session_delete.asyncio_detailed(client=self._client, session_id=session_id)
        return True

    async def abort_session(self, session_id: str) -> bool:
        await session_abort.asyncio_detailed(client=self._client, session_id=session_id)
        return True

    async def get_session_children(self, session_id: str) -> list[SessionInfo]:
        result = await session_children.asyncio(client=self._client, session_id=session_id)
        if result is None:
            return []
        return [SessionInfo(s) for s in result]

    # -- messages -------------------------------------------------------------

    async def list_messages(self, session_id: str) -> list[MessageWithParts]:
        result = await session_messages.asyncio(client=self._client, session_id=session_id)
        if result is None:
            return []
        return [MessageWithParts(m) for m in result]

    async def send_message(
        self,
        session_id: str,
        text: str,
        *,
        model: dict[str, str] | None = None,
        agent: str | None = None,
        tools: dict[str, Any] | None = None,
        system: str | None = None,
    ) -> MessageWithParts:
        # Build the prompt body. POST /session/{id}/message blocks until done.
        part = TextPartInput(type_="text", text=text)
        body = SessionPromptBody(parts=[part])

        client_with_timeout = self._client.with_timeout(
            httpx.Timeout(600.0, connect=10.0)
        )
        result = await session_prompt.asyncio(
            client=client_with_timeout,
            session_id=session_id,
            body=body,
        )

        if result is not None:
            return MessageWithParts(result)

        # Empty response — fallback to listing messages
        msgs = await self.list_messages(session_id)
        for msg in reversed(msgs):
            if msg.info.role == "assistant":
                return msg
        # Return an empty assistant message as last resort
        from opencode_sdk.models.assistant_message import AssistantMessage
        from opencode_sdk.models.assistant_message_time import AssistantMessageTime
        from opencode_sdk.models.assistant_message_path import AssistantMessagePath
        from opencode_sdk.models.assistant_message_tokens import AssistantMessageTokens
        empty = AssistantMessage(
            id="", session_id=session_id, role="assistant",
            time=AssistantMessageTime.from_dict({}),
            parent_id="", model_id="", provider_id="",
            mode="", agent="",
            path=AssistantMessagePath.from_dict({}),
            cost=0.0,
            tokens=AssistantMessageTokens.from_dict({}),
        )
        return MessageWithParts(
            type("FakeMsg", (), {"info": empty, "parts": []})()
        )

    # -- session artifacts ----------------------------------------------------

    async def get_diff(self, session_id: str) -> list[FileDiff]:
        result = await session_diff.asyncio(client=self._client, session_id=session_id)
        if result is None:
            return []
        return [FileDiff(d) for d in result]

    async def get_todos(self, session_id: str) -> list[TodoItem]:
        result = await session_todo.asyncio(client=self._client, session_id=session_id)
        if result is None:
            return []
        return [TodoItem(t) for t in result]
