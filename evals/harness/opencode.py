"""
Hand-written async httpx client for the OpenCode HTTP API.

Covers the ~12 endpoints needed by the eval harness.
Types are hand-written pydantic models matching the API responses we consume.

API reference: @opencode-ai/sdk v1.4.10
Source: ~/.opencode/node_modules/@opencode-ai/sdk/dist/gen/types.gen.d.ts
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any

import httpx
from pydantic import BaseModel


# ---------------------------------------------------------------------------
# Response models (only the shapes we actually use)
# ---------------------------------------------------------------------------

class SessionInfo(BaseModel):
    id: str
    title: str
    parentID: str | None = None
    slug: str = ""
    version: str = ""
    share: str | None = None
    time: dict[str, Any] = {}


class MessageInfo(BaseModel, extra="allow"):
    id: str
    role: str
    parentID: str | None = None
    mode: str | None = None
    agent: str | None = None
    variant: str | None = None
    modelID: str | None = None
    finish: str | None = None
    cost: float = 0.0
    tokens: dict[str, Any] = {}
    path: dict[str, str] = {}
    time: dict[str, Any] = {}
    system: Any = None
    tool: dict[str, Any] | None = None


class PartData(BaseModel, extra="allow"):
    type: str
    text: str | None = None
    tool: str | None = None
    id: str | None = None
    state: Any = None
    input: Any = None
    output: Any = None
    metadata: dict[str, Any] | None = None
    content: Any = None


class MessageWithParts(BaseModel, extra="allow"):
    info: MessageInfo
    parts: list[PartData] = []


class TodoItem(BaseModel):
    content: str = ""
    status: str = ""
    priority: str = ""


class FileDiff(BaseModel):
    path: str = ""
    type: str = ""
    additions: int = 0
    deletions: int = 0
    patch: str = ""


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

@dataclass
class OpenCodeClient:
    """Async client for a single OpenCode server instance."""

    base_url: str
    _http: httpx.AsyncClient = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._http = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=httpx.Timeout(60.0, connect=10.0),
            limits=httpx.Limits(max_connections=20),
        )

    async def close(self) -> None:
        await self._http.aclose()

    # -- health ---------------------------------------------------------------

    async def health_check(self) -> bool:
        try:
            r = await self._http.get("/project/current")
            return r.status_code == 200
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
        body: dict[str, Any] = {}
        if title:
            body["title"] = title
        r = await self._http.post("/session", json=body)
        r.raise_for_status()
        return SessionInfo.model_validate(r.json())

    async def get_session(self, session_id: str) -> SessionInfo:
        r = await self._http.get(f"/session/{session_id}")
        r.raise_for_status()
        return SessionInfo.model_validate(r.json())

    async def delete_session(self, session_id: str) -> bool:
        r = await self._http.delete(f"/session/{session_id}")
        r.raise_for_status()
        return True

    async def abort_session(self, session_id: str) -> bool:
        r = await self._http.post(f"/session/{session_id}/abort")
        r.raise_for_status()
        return True

    async def get_session_children(self, session_id: str) -> list[SessionInfo]:
        r = await self._http.get(f"/session/{session_id}/children")
        r.raise_for_status()
        return [SessionInfo.model_validate(s) for s in r.json()]

    # -- messages -------------------------------------------------------------

    async def list_messages(self, session_id: str) -> list[MessageWithParts]:
        r = await self._http.get(f"/session/{session_id}/message")
        r.raise_for_status()
        return [MessageWithParts.model_validate(m) for m in r.json()]

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
        body: dict[str, Any] = {
            "parts": [{"type": "text", "text": text}],
        }
        if model:
            body["model"] = model
        if agent:
            body["agent"] = agent
        if tools:
            body["tools"] = tools
        if system:
            body["system"] = system
        r = await self._http.post(
            f"/session/{session_id}/message",
            json=body,
            timeout=httpx.Timeout(600.0, connect=10.0),
        )
        r.raise_for_status()
        content = r.text.strip()
        if content:
            return MessageWithParts.model_validate(r.json())

        # API returned async — poll until the last assistant message is done
        return await self._poll_completion(session_id)

    _TERMINAL_FINISH = {"stop", "end_turn", "max_tokens", "error"}

    async def _poll_completion(
        self, session_id: str, poll_interval: float = 1.0
    ) -> MessageWithParts:
        """Poll messages until the last assistant message has a terminal finish reason."""
        while True:
            msgs = await self.list_messages(session_id)
            for msg in reversed(msgs):
                if msg.info.role != "assistant":
                    continue
                if msg.info.finish in self._TERMINAL_FINISH:
                    return msg
                break  # last assistant msg not done yet
            await asyncio.sleep(poll_interval)

    # -- session artifacts ----------------------------------------------------

    async def get_diff(self, session_id: str) -> list[FileDiff]:
        r = await self._http.get(f"/session/{session_id}/diff")
        r.raise_for_status()
        return [FileDiff.model_validate(d) for d in r.json()]

    async def get_todos(self, session_id: str) -> list[TodoItem]:
        r = await self._http.get(f"/session/{session_id}/todo")
        r.raise_for_status()
        return [TodoItem.model_validate(t) for t in r.json()]


