from enum import Enum
from dataclasses import dataclass
from typing import Dict, Optional, List, Any
from mimetypes import guess_extension
from datetime import datetime
import os
import asyncio
import aiohttp
import aiofiles


# TODO: Support default download folder

# TODO: Define an Event Class
@dataclass
class DownloadEvent:
    task_id: int
    state: DownloadState
    error_string: Optional[str]
    percent_completed: int
    download_speed: float

class DownloadState(Enum):
    PAUSED = 0
    RUNNING = 1
    COMPLETED = 2
    ERROR = -1

@dataclass
class DownloadMetadata:
    task_id: int
    url: str
    output_file: str
    state: DownloadState = DownloadState.PAUSED
    etag: str
    server_supports_http_range: bool = False


class DownloadManager:
    """
    Async download manager using asyncio and aiohttp.
    """

    def __init__(self) -> None:
        # TODO Add clean up self._downloads logic
        self._downloads: Dict[int, DownloadMetadata] = {}
        self._next_id = 0
        self.events_queue: asyncio.Queue[Dict[str, Any]] = asyncio.Queue()
        # TODO make sure self._tasks are cleaned up
        self._tasks: Dict[int, asyncio.Task[Any]] = {}

    def _iterate_id(self) -> str:
        self._next_id += 1
        return self._next_id

    def add_and_start_download(self, url: str, output_file: Optional[str] = "") -> int:
        id = self.add_download(url, output_file)
        self.start_download(id)
        return id

    def add_download(self, url: str, output_file: Optional[str] = "") -> int:
        id = self._iterate_id()
        self._downloads[id] = DownloadMetadata(task_id=id, url=url, output_file=output_file)
        return id

    async def start_download(self, task_id: int) -> bool:
        """
        Wraps download coroutine into task to start the download
        """

        if task_id not in self._downloads:
            return False

        self._tasks[task_id] = asyncio.create_task(self._download_file_coroutine(self._downloads[task_id]))

    async def _download_file_coroutine(self, download: DownloadMetadata) -> None:
        output_file_size = os.path.getsize(download.output_file) if os.path.exists(download.output_file) else 0

        expected_file_size = None

        # TODO: Need to separate header checks into ON FIRST RUN and ON RESUME
        async with aiohttp.ClientSession() as session:
            async with session.head(download.url) as resp:

                if download.output_file == "":
                    if "Etag" in resp.headers:
                        download.output_file = resp.headers["Etag"] + guess_extension(resp.headers.get("Content-Type", ".file"))
                    else:
                        download.output_file = datetime.now().strftime("%m_%d_%Y_%H_%M_%S")
                        if "Content-Type" in resp.headers:
                            guess_file_extension = guess_extension(resp.headers["Content-Type"])
                            if guess_file_extension is None:
                                download.output_file += ".file"
                            else:
                                download.output_file += guess_file_extension
                        else:
                            download.output_file += ".file"
                
                if "Accept-Ranges" in resp.headers:
                    download.server_supports_http_range = resp.headers["Accept-Ranges"] == "bytes"
                
                expected_file_size = int(resp.headers["Content-Length"]) if "Content-Length" in resp.headers else None     

        if expected_file_size is not None:
            if output_file_size >= expected_file_size:
                # File already downloaded
                await self.events_queue.put(DownloadEvent(
                    task_id=download.task_id,
                    state=download.state
                ))
                return

        # Actually download the file --------------------------------------------------------------------
        headers = {}
        if download.server_supports_range:
            headers["Range"] = f"bytes={output_file_size}-"

        session = aiohttp.ClientSession()
        try:
            download.state = DownloadState.RUNNING
            await self.events_queue.put(
                DownloadEvent(
                    task_id=download.task_id,
                    state=download.state
                )
            )

            async with session.get(download.url, headers=headers) as resp:
                # TODO parameterize chunk write size
                async for chunk in resp.content.iter_chunked(64 * 1024):
                    try:
                        mode = "ab"
                        if resp.status == 200:
                            # Server doesn't support partials
                            mode = "wb"
                        async with aiofiles.open(download.output_file, mode) as f:
                            await f.write(chunk)
                    except Exception:
                        # TODO: Handle Write Errors
                        pass

            download.state = DownloadState.COMPLETED
            await self.events_queue.put(DownloadEvent(
                task_id=download.task_id,
                state= download.state
            ))

            self._tasks.pop(download.task_id, None)
        except asyncio.CancelledError:
            download.state = DownloadState.PAUSED
            await self.events_queue.put(DownloadEvent(
                task_id=download.task_id,
                state= download.state
            ))
        except Exception as err:
            download.state = DownloadState.ERROR
            await self.events_queue.put(DownloadEvent(
                task_id=download.task_id,
                state= download.state,
                error_string=str(err)
            ))

    async def pause_download(self, task_id: int) -> bool:
        """
        Docstring for pause_download
        """
        if task_id not in self._downloads:
            return False
        
        download = self._downloads[task_id]
        if download.state != DownloadState.RUNNING:
            return False

        task = self._tasks.get(task_id)
        if task and not task.done():
            task.cancel()
            # TODO: I don't think we need to await here
            # try:
            #     await task
            # except asyncio.CancelledError:
            #     pass
        download.state = DownloadState.PAUSED
        await self.events_queue.put(
            DownloadEvent(
                task_id=download.id,
                state=download.state
            )
        )
        return True

    # TODO: Implement Resume Download
    # async def resume_download(self, task_id: str) -> bool:
    #     task = self._downloads.get(task_id)
    #     if not task:
    #         return False
    #     if task.state == DownloadState.PAUSED:
    #         worker = asyncio.create_task(self._download_file_coroutine(task))
    #         self._tasks[task_id] = worker
    #     return True

    # TODO: Implement Remove Download and add Optional File Cleanup
    # async def cancel(self, task_id: str) -> bool:
    #     task = self._downloads.get(task_id)
    #     if not task:
    #         return False
    #     task.state = DownloadState.PAUSED
    #     worker = self._tasks.get(task_id)
    #     if worker and not worker.done():
    #         worker.cancel()
    #         try:
    #             await worker
    #         except asyncio.CancelledError:
    #             pass
    #     return True



__all__ = ["DownloadManager", "DownloadMetadata", "DownloadState", "DownloadEvent"]
