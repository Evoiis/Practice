from enum import Enum
from dataclasses import dataclass
from typing import Dict, Optional, Any
from mimetypes import guess_extension
from datetime import datetime, timedelta

import logging
import os
import asyncio
import aiohttp
import aiofiles


# TODO: Support default download folder
# TODO: Save data to file: Persist preferences and download_metadata between restarts

@dataclass
class DownloadEvent:
    task_id: int
    state: DownloadState
    # percent_completed: float    # TODO: Logic
    # download_speed: float   # TODO: Logic
    error_string: Optional[str] = ""
    time: datetime = datetime.now()

class DownloadState(Enum):
    PAUSED = 0
    RUNNING = 1
    COMPLETED = 2
    PENDING = 3
    DELETED = 4
    ERROR = -1

@dataclass
class DownloadMetadata:
    task_id: int
    url: str
    output_file: str
    etag: str = None
    # average_speed: float = 0 # MB/s    # TODO
    downloaded_bytes: int = 0
    file_size_bytes: int = None
    # active_time: timedelta = timedelta() # TODO
    # time_completed: datetime = None # TODO
    time_added: datetime = datetime.now()
    state: DownloadState = DownloadState.PENDING
    server_supports_http_range: bool = False


class DownloadManager:
    """
    Async download manager using asyncio and aiohttp.
    """

    def __init__(self, chunk_write_size: int = 1) -> None:
        """
        :param chunk_write_size: How large of a chunk should the program write each time in MB
        :type chunk_write_size: int
        """

        # TODO Add clean up self._downloads logic
        self._downloads: Dict[int, DownloadMetadata] = {}
        self._next_id = 0
        self.events_queue: asyncio.Queue[Dict[str, Any]] = asyncio.Queue()
        # TODO make sure self._tasks are cleaned up
        self._tasks: Dict[int, asyncio.Task[Any]] = {}
        self.chunk_write_size = chunk_write_size

    def _iterate_and_get_id(self) -> int:
        self._next_id += 1
        return self._next_id

    def shutdown(self):
        for task_id in self._tasks:
            if not self._tasks[task_id].done():
                self._tasks[task_id].cancel()
    
    def get_downloads(self) -> Dict[int, DownloadMetadata]:
        return self._downloads

    async def get_oldest_event(self) -> Optional[DownloadEvent]:
        """
        Pops the oldest event from the queue if the queue is not empty
        """
        if self.events_queue.empty():
            return None
        else:
            return self.events_queue.get_nowait()            

    def add_download(self, url: str, output_file: Optional[str] = "") -> int:
        id = self._iterate_and_get_id()
        # TODO: Add error checks to check for invalid/clashing output file names
        # URL validity check?
        self._downloads[id] = DownloadMetadata(task_id=id, url=url, output_file=output_file)
        return id

    async def start_download(self, task_id: int) -> bool:
        """
        Wraps download coroutine into task to start the download
        """

        if task_id not in self._downloads:
            return False

        if self._downloads[task_id].state != DownloadState.PENDING:
            logging.warning(f"Received request to start, {task_id=}, but task is not pending!")
            return False

        self._tasks[task_id] = asyncio.create_task(self._download_file_coroutine(self._downloads[task_id]))
        return True

    async def resume_download(self, task_id: int) -> bool:
        """
        Wraps download coroutine into task to resume download
        """

        if task_id not in self._downloads:
            logging.warning(f"Received invalid {task_id=} in resume_download")
            return False

        download = self._downloads[task_id]

        if download.state not in [DownloadState.PAUSED, DownloadState.ERROR]:
            return False
        if os.path.exists(download.output_file):
            os.remove(download.output_file)
        
        self._tasks[task_id] = asyncio.create_task(self._download_file_coroutine(self._downloads[task_id], resume=True))

        return True

    async def pause_download(self, task_id: int) -> bool:
        """
        Pauses a download if it is running
        Removes task from _tasks

        :param task_id: Identifies which task to pause
        """
        if task_id not in self._downloads:
            return False
        
        download = self._downloads[task_id]
        if download.state != DownloadState.RUNNING:
            return False

        if task_id not in self._tasks:
            raise Exception("Error: task_id not in DownloadManager task list")
        task = self._tasks[task_id]
        if not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        
        if task_id in self._tasks:
            del self._tasks[task_id]
        download.state = DownloadState.PAUSED
        await self.events_queue.put(
            DownloadEvent(
                task_id=download.task_id,
                state=download.state
            )
        )
        return True

    async def delete_download(self, task_id: int, remove_file: bool = False) -> bool:
        """
        Removes download from _downloads and pauses the download if it is currently running

        :param remove_file: Whether or not to delete the output file
        """
        if task_id not in self._downloads:
            logging.warning(f"Download Manager delete_download called with invalid task_id")
            return False

        if self._downloads[task_id].state == DownloadState.RUNNING:
            if not await self.pause_download(task_id):
                raise Exception("Error: pause_download failed in delete_download")
        
        if task_id in self._tasks:
            del self._tasks[task_id]
        
        if remove_file:
            os.remove(self._downloads[task_id].output_file)
        del self._downloads[task_id]

        await self.events_queue.put(
            DownloadEvent(
                task_id=task_id,
                state=DownloadState.DELETED
            )
        )

        return True

    async def _check_download_headers(self, download: DownloadMetadata, resume: bool=False) -> bool:
        """
        Parses headers from server

        First start:
        - Collects ETag, Content-Length, Content-Type, Accept-Ranges to fill download metadata accordingly

        On Resume:
        - Verfies ETag and Content-Length still match download metadata
        """
        download_is_valid = True # 

        if resume:
            # Header checks on download resume
            async with aiohttp.ClientSession() as session:
                async with session.head(download.url) as resp:                    
                    if "ETag" in resp.headers:
                        if resp.headers["ETag"] != download.etag:
                            logging.info(f"Etag changed for {download.task_id=}, {download.url=}, {download.output_file=}. Restarting download from scratch.")
                            download.etag = resp.headers["ETag"]
                            download_is_valid = False
                    if "Content-Length" in resp.headers:
                        if download.file_size_bytes != int(resp.headers["Content-Length"]):
                            logging.info(f"File size changed for {download.task_id=}, {download.url=}, {download.output_file=}. Restarting download from scratch.")
                            download.file_size_bytes = resp.headers["Content-Length"]
                            download_is_valid = False

        else:
            # Header checks on download first start
            async with aiohttp.ClientSession() as session:
                async with session.head(download.url) as resp:

                    if download.output_file == "":
                        if "ETag" in resp.headers:
                            download.output_file = resp.headers["ETag"] + guess_extension(resp.headers.get("Content-Type", ".file"))
                            download.etag = resp.headers["ETag"]
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
                    
                    download.file_size_bytes = int(resp.headers["Content-Length"]) if "Content-Length" in resp.headers else None
        
        return download_is_valid

    async def _download_file_coroutine(self, download: DownloadMetadata, resume: bool=False) -> None:
        download.downloaded_bytes = os.path.getsize(download.output_file) if os.path.exists(download.output_file) else 0

        # Check if the file has already been completely downloaded
        if download.file_size_bytes is not None:
            if download.downloaded_bytes == download.file_size_bytes:
                download.state = DownloadState.COMPLETED
                await self.events_queue.put(DownloadEvent(
                    task_id=download.task_id,
                    state=download.state
                ))
                del self._tasks[download.task_id]
                return
            elif download.downloaded_bytes > download.file_size_bytes:
                # TODO
                raise Exception("Downloaded bytes > expected file size")

        # Get headers to collect/check metadata
        if await self._check_download_headers(download, resume) is False:
            # Download is no longer valid, restart from scratch
            if os.path.exists(download.output_file):
                os.remove(download.output_file)
                download.downloaded_bytes = 0
        

        # Download the file --------------------------------------------------------------------
        headers = {}
        if download.server_supports_http_range:
            headers["Range"] = f"bytes={download.downloaded_bytes}-"

        download.state = DownloadState.RUNNING
        await self.events_queue.put(DownloadEvent(
            task_id=download.task_id,
            state=download.state
        ))
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(download.url, headers=headers) as resp:
                    async for chunk in resp.content.iter_chunked(self.chunk_write_size * 1024 * 1024):
                        mode = "ab"
                        if resp.status == 200:
                            mode = "wb"
                        async with aiofiles.open(download.output_file, mode) as f:
                            await f.write(chunk)
                            download.downloaded_bytes += len(chunk)
                            
                        # TODO: Update speed

            download.state = DownloadState.COMPLETED
            await self.events_queue.put(DownloadEvent(
                task_id=download.task_id,
                state= download.state
            ))

            del self._tasks[download.task_id]
        except asyncio.CancelledError:
            download.state = DownloadState.PAUSED
            await self.events_queue.put(DownloadEvent(
                task_id=download.task_id,
                state= download.state
            ))
            raise
        except Exception as err:
            download.state = DownloadState.ERROR
            await self.events_queue.put(DownloadEvent(
                task_id=download.task_id,
                state= download.state,
                error_string=str(err)
            ))
        finally:
            if download.task_id in self._tasks:
                del self._tasks[download.task_id]


__all__ = ["DownloadManager", "DownloadMetadata", "DownloadState", "DownloadEvent"]
