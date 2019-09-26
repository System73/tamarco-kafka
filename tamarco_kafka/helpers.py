import asyncio
from threading import Event


class AsyncEvent:
    """Asynchronous interface to the threading.Event object, based in polling."""

    def __init__(self):
        self.event = Event()

    def __await__(self):
        return self.wait().__await__()

    def set(self):  # noqa: A003
        self.event.set()

    async def _poll_event(self):
        while not self.event.is_set():
            await asyncio.sleep(0.1)

    async def wait(self):
        await self._poll_event()
