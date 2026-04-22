"""
Session snapshot capture and SSE event demuxer.

The EventDemuxer maintains a single SSE connection per arm and routes
events to per-session async queues by session_id. This avoids N concurrent
SSE streams when running tasks at concurrency > 1.

The snapshot builder collects all session state after the prompt completes:
messages (with all parts), children, diffs, todos, and events.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import httpx
from httpx_sse import aconnect_sse

from harness.opencode import MessageWithParts, OpenCodeClient, SessionInfo

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Event Demuxer
# ---------------------------------------------------------------------------

@dataclass
class EventDemuxer:
    """Single SSE connection, demuxes events to per-session queues."""

    base_url: str
    _subscriptions: dict[str, asyncio.Queue[dict[str, Any]]] = field(
        default_factory=dict, init=False
    )
    _task: asyncio.Task[None] | None = field(default=None, init=False)
    _running: bool = field(default=False, init=False)
    _http: httpx.AsyncClient | None = field(default=None, init=False)

    async def start(self) -> None:
        self._running = True
        self._http = httpx.AsyncClient(base_url=self.base_url, timeout=None)
        self._task = asyncio.create_task(self._listen(), name="event-demuxer")

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        if self._http:
            await self._http.aclose()
            self._http = None

    def subscribe(self, session_id: str) -> asyncio.Queue[dict[str, Any]]:
        q: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self._subscriptions[session_id] = q
        return q

    def unsubscribe(self, session_id: str) -> None:
        self._subscriptions.pop(session_id, None)

    async def _listen(self) -> None:
        assert self._http is not None
        while self._running:
            try:
                async with aconnect_sse(self._http, "GET", "/event") as sse:
                    async for event in sse.aiter_sse():
                        if not self._running:
                            break
                        try:
                            data = json.loads(event.data) if event.data else {}
                        except json.JSONDecodeError:
                            continue

                        evt = {
                            "type": event.event or "message",
                            "data": data,
                            "ts": datetime.now(timezone.utc).isoformat(),
                        }

                        # Route to the right session queue.
                        # Events may contain session_id at various nesting levels.
                        sid = _extract_session_id(data)
                        if sid and sid in self._subscriptions:
                            try:
                                self._subscriptions[sid].put_nowait(evt)
                            except asyncio.QueueFull:
                                logger.warning("event queue full for session %s", sid)
            except httpx.HTTPError as e:
                if self._running:
                    logger.warning("SSE connection error, reconnecting: %s", e)
                    await asyncio.sleep(1.0)
            except asyncio.CancelledError:
                break


def _extract_session_id(data: dict[str, Any]) -> str | None:
    """Try to find a session_id in the event payload."""
    if "sessionID" in data:
        return data["sessionID"]
    if "session_id" in data:
        return data["session_id"]
    if isinstance(data.get("properties"), dict):
        props = data["properties"]
        return props.get("sessionID") or props.get("session_id")
    return None


# ---------------------------------------------------------------------------
# Session Snapshot
# ---------------------------------------------------------------------------

@dataclass
class SessionSnapshot:
    """Full trace of a single eval task session."""

    session: SessionInfo
    messages: list[MessageWithParts]
    children: list[SessionInfo]
    diffs: list[Any]
    todos: list[Any]
    events: list[dict[str, Any]]
    timing: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "session": self.session.model_dump(),
            "messages": [m.model_dump() for m in self.messages],
            "children": [c.model_dump() for c in self.children],
            "diffs": [d.model_dump() if hasattr(d, "model_dump") else d for d in self.diffs],
            "todos": [t.model_dump() if hasattr(t, "model_dump") else t for t in self.todos],
            "events": self.events,
            "timing": self.timing,
        }


async def capture_snapshot(
    client: OpenCodeClient,
    session_id: str,
    event_queue: asyncio.Queue[dict[str, Any]],
    started_at: datetime,
) -> SessionSnapshot:
    """Collect all session data into a snapshot after the prompt completes."""

    session, messages, children, diffs, todos = await asyncio.gather(
        client.get_session(session_id),
        client.list_messages(session_id),
        client.get_session_children(session_id),
        client.get_diff(session_id),
        client.get_todos(session_id),
    )

    events: list[dict[str, Any]] = []
    while not event_queue.empty():
        try:
            events.append(event_queue.get_nowait())
        except asyncio.QueueEmpty:
            break

    now = datetime.now(timezone.utc)
    timing = {
        "created_at": started_at.isoformat(),
        "completed_at": now.isoformat(),
        "duration_ms": int((now - started_at).total_seconds() * 1000),
    }

    return SessionSnapshot(
        session=session,
        messages=messages,
        children=children,
        diffs=diffs,
        todos=todos,
        events=events,
        timing=timing,
    )
