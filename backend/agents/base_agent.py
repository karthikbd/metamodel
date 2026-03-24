"""
Base class for all hydration agents.
Each agent emits log events via an async generator so the API layer can
stream them to the frontend over SSE without buffering.
"""
import asyncio
import time
import uuid
from abc import ABC, abstractmethod
from typing import AsyncGenerator


class AgentEvent:
    def __init__(self, agent: str, level: str, message: str, data: dict | None = None):
        self.id       = str(uuid.uuid4())
        self.agent    = agent
        self.level    = level    # "info" | "warn" | "error" | "success"
        self.message  = message
        self.data     = data or {}
        self.ts       = time.time()

    def to_dict(self) -> dict:
        return {
            "id":      self.id,
            "agent":   self.agent,
            "level":   self.level,
            "message": self.message,
            "data":    self.data,
            "ts":      self.ts,
        }


class BaseAgent(ABC):
    name: str = "base"

    def __init__(self, scan_run_id: str, repo_root: str, force: bool = False):
        self.scan_run_id = scan_run_id
        self.repo_root   = repo_root
        self.force       = force   # bypass file-hash cache when True
        self._events: asyncio.Queue = asyncio.Queue()

    async def _emit(self, level: str, message: str, data: dict | None = None):
        event = AgentEvent(self.name, level, message, data)
        await self._events.put(event)

    async def info(self, msg: str, **kwargs):
        await self._emit("info", msg, kwargs or None)

    async def warn(self, msg: str, **kwargs):
        await self._emit("warn", msg, kwargs or None)

    async def error(self, msg: str, **kwargs):
        await self._emit("error", msg, kwargs or None)

    async def success(self, msg: str, **kwargs):
        await self._emit("success", msg, kwargs or None)

    @abstractmethod
    async def run(self) -> None:
        """Do work, calling self.info/warn/error/success to emit events."""
        ...

    async def stream(self) -> AsyncGenerator[AgentEvent, None]:
        """Drive run() and drain the event queue, yielding each event."""
        task = asyncio.create_task(self._run_and_close())
        while True:
            try:
                event = await asyncio.wait_for(self._events.get(), timeout=0.2)
                yield event
            except asyncio.TimeoutError:
                if task.done():
                    # Drain any remaining events then stop
                    while not self._events.empty():
                        yield await self._events.get()
                    break
        if task.exception():
            raise task.exception()

    async def _run_and_close(self):
        await self.run()
