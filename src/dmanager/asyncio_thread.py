import asyncio
import threading

class AsyncioEventLoopThread:
    def __init__(self):
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(
            target=self._run_loop,
            daemon=True
        )
        self.thread.start()

    def _run_loop(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    def submit(self, coro):
        return asyncio.run_coroutine_threadsafe(coro, self.loop)

    def shutdown(self):
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join(30.0)
        if self.thread.is_alive():
            raise Exception("Failed to join async thread after timeout!")

__all__ = ["AsyncioEventLoopThread"]
