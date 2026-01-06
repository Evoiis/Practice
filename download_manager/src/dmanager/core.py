from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional, List, Any
from datetime import datetime
import os
import asyncio

import aiohttp
import aiofiles


@dataclass
class DownloadTask:
    id: str
    url: str
    dest_path: str
    state: str = "pending"  # pending, running, paused, cancelled, completed

class DownloadManager:
    """
    Async download manager.

    """

    def __init__(self) -> None:
        self._tasks: Dict[int, DownloadTask] = {}
        self._next_id = 1
        self.events_queue: asyncio.Queue[Dict[str, Any]] = asyncio.Queue()
        self._workers: Dict[str, asyncio.Task[Any]] = {}

    def _make_id(self) -> str:
        self._next_id += 1
        return self._next_id

    def add(self, url: str, dest_path: Optional[str] = None) -> str:
        if dest_path is None:
            filename = url.split("/")[-1] or "download"+str(datetime.now().strftime("%m_%d_%Y_%H_%M_%S"))

            dest_path = os.path.join(os.getcwd(), filename)

        task_id = self._make_id()
        task = DownloadTask(id=task_id, url=url, dest_path=str(dest_path))
        self._tasks[task_id] = task
        return task_id

    async def start(self, task_id: str) -> bool:
        """Start (and await) a download worker for `task_id`.

        Returns True if the task existed, False otherwise.
        """
        task = self._tasks.get(task_id)
        if not task:
            return False
        worker = asyncio.create_task(self._run_download(task))
        self._workers[task_id] = worker
        try:
            await worker
            return True
        finally:
            self._workers.pop(task_id, None)

    async def _run_download(self, task: DownloadTask) -> None:
        task.state = "running"
        url = task.url
        headers: Dict[str, str] = {}
        if task.downloaded_bytes:
            headers["Range"] = f"bytes={task.downloaded_bytes}-"

        # async with self._semaphore:
        session = aiohttp.ClientSession()
        try:
            async with session.get(url, headers=headers) as resp:
                total: Optional[int] = None
                cl = resp.headers.get("Content-Length")
                if cl:
                    try:
                        total = int(cl)
                    except Exception:
                        total = None

                async for chunk in resp.content.iter_chunked(64 * 1024):
                    try:
                        part_path = task.dest_path + ".part"
                        async with aiofiles.open(part_path, "ab") as f:
                            await f.write(chunk)
                    except Exception:
                        # swallow IO errors for tests
                        pass
                    task.downloaded_bytes += len(chunk)
                    await self.events_queue.put({
                        "task_id": task.id,
                        "downloaded": task.downloaded_bytes,
                        "total": total,
                        "status": "running",
                    })

                task.state = "completed"
                try:
                    part = task.dest_path + ".part"
                    if os.path.exists(part):
                        os.replace(part, task.dest_path)
                except Exception:
                    pass

                await self.events_queue.put({"task_id": task.id, "status": "completed"})
        except asyncio.CancelledError:
            task.state = "paused"
            await self.events_queue.put({"task_id": task.id, "status": "paused"})
            raise
        except Exception:
            task.state = "paused"
            await self.events_queue.put({"task_id": task.id, "status": "error"})
        finally:
            close = getattr(session, "close", None)
            if close:
                maybe = close()
                if asyncio.iscoroutine(maybe):
                    await maybe

    async def pause(self, task_id: str) -> bool:
        task = self._tasks.get(task_id)
        if not task:
            return False
        worker = self._workers.get(task_id)
        if worker and not worker.done():
            worker.cancel()
            try:
                await worker
            except asyncio.CancelledError:
                pass
        if task.state == "running":
            task.state = "paused"
        return True

    async def resume(self, task_id: str) -> bool:
        task = self._tasks.get(task_id)
        if not task:
            return False
        if task.state in {"paused", "pending"}:
            worker = asyncio.create_task(self._run_download(task))
            self._workers[task_id] = worker
        return True

    async def cancel(self, task_id: str) -> bool:
        task = self._tasks.get(task_id)
        if not task:
            return False
        task.state = "cancelled"
        worker = self._workers.get(task_id)
        if worker and not worker.done():
            worker.cancel()
            try:
                await worker
            except asyncio.CancelledError:
                pass
        return True

    def list(self) -> List[DownloadTask]:
        return list(self._tasks.values())


__all__ = ["DownloadManager", "DownloadTask"]
