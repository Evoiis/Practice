from enum import Enum
from dataclasses import dataclass
from typing import Dict, Optional, Any
from mimetypes import guess_extension
from datetime import datetime, timedelta

import re
import logging
import os
import asyncio
import aiohttp
import aiofiles
import copy

ONE_GibiB = 1073741824

# TODO: Support default download folder
# TODO: Save metadata to file: Persist preferences and download_metadata between restarts

# TODO: Get rid of resume_download and replace with start_downlaod
# TODO: Parallel downloads support resume download

class DownloadState(Enum):
    PAUSED = 0
    RUNNING = 1
    COMPLETED = 2
    PENDING = 3
    DELETED = 4
    ALLOCATING_SPACE = 5
    ERROR = -1


@dataclass
class DownloadEvent:
    task_id: int
    state: DownloadState
    output_file: str
    time: datetime = 0
    error_string: Optional[str] = ""
    download_speed: float = None
    active_time: timedelta = None
    downloaded_bytes: int = None
    download_size_bytes: int = None
    worker_id: int = None


    def __post_init__(self):
        self.time = datetime.now()

@dataclass
class DownloadMetadata:
    task_id: int
    url: str
    output_file: str
    etag: str = None
    downloaded_bytes: int = 0
    file_size_bytes: int = None
    active_time: timedelta = timedelta()
    state: DownloadState = DownloadState.PENDING
    server_supports_http_range: bool = False
    use_parallel_download: bool = False
    error_count: int = 0


class DownloadManager:
    """
    Async download manager using asyncio and aiohttp.
    """

    def __init__(self, chunk_write_size_mb: int = 1, running_event_update_rate_seconds: int = 1, maximum_workers_per_task: int = 5) -> None:
        """
        :param chunk_write_size: How large of a chunk should the program write each time in MB
        :type chunk_write_size: int
        """

        self._downloads: Dict[int, DownloadMetadata] = {}
        self._next_id = 0
        self.events_queue: asyncio.Queue[Dict[str, Any]] = asyncio.Queue()
        self._tasks: Dict[int, asyncio.Task[Any]] = {}
        self._task_pools: Dict[int, list] = {}
        self._data_queues: Dict[int, asyncio.Queue] = {}
        self._session: aiohttp.ClientSession = None

        # TODO update mb -> MiB cause that's technically correct
        self.chunk_write_size_mb = chunk_write_size_mb
        self.running_event_update_rate_seconds = timedelta(seconds=running_event_update_rate_seconds)
        self.maximum_workers_per_task = maximum_workers_per_task

    def _iterate_and_get_id(self) -> int:
        self._next_id += 1
        return self._next_id
    
    async def _log_and_share_error_event(self, download: DownloadMetadata, err: Exception):
        logging.error(f"{repr(err)}, {err}")
        download.state = DownloadState.ERROR
        download.error_count += 1
        await self.events_queue.put(DownloadEvent(
            task_id=download.task_id,
            state= download.state,
            error_string=f"{repr(err)}, {err}",
            output_file=download.output_file
        ))

    async def shutdown(self):
        for task_id in self._tasks:
            if not self._tasks[task_id].done():
                self._tasks[task_id].cancel()
        
        for task_id in self._task_pools:
            for task in self._task_pools[task_id]:
                task.cancel()
        
        if self._session is not None:
            await self._session.close()
                
    
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

        for download in self._downloads.values():
            if download.output_file == output_file:
                output_file = ""
                break

        if os.path.exists(output_file):
            output_file = ""

        output_file = re.sub(r'[\\/:*?"<>|]', "", output_file).rstrip(" .")
        self._downloads[id] = DownloadMetadata(task_id=id, url=url, output_file=output_file)
        return id

    async def start_download(self, task_id: int, use_parallel_download: bool = None) -> bool:
        """
        Wraps download coroutine into task to start the download

        :param task_id: Integer identifier for task
        :param use_parallel_download: Explicit option to disable/enable parallel download
        """

        if not self._session:
            self._session = aiohttp.ClientSession()

        if task_id not in self._downloads:
            return False

        download = self._downloads[task_id]
        if download.state not in [DownloadState.PENDING, DownloadState.ERROR, DownloadState.PAUSED]:
            logging.warning(f"Received invalid request to start, {task_id=}. Task is in invalid state to be started.")
            return False

        if download.state == DownloadState.ERROR:
            if os.path.exists(download.output_file):
                os.remove(download.output_file)

        try:
            await self._check_download_headers(download)
        except Exception as err:
            await self._log_and_share_error_event(download, err)
            return False

        # Use parallel download decision
        if (download.file_size_bytes > ONE_GibiB and use_parallel_download is None) or use_parallel_download is True:
            download.use_parallel_download = True

        if not download.server_supports_http_range or use_parallel_download is False:
            download.use_parallel_download = False
        

        # Initialize async tasks
        if download.use_parallel_download:
            # Pre-allocate file on disk
            async with aiofiles.open(download.output_file, "wb") as f:
                download.state = DownloadState.ALLOCATING_SPACE
                await self.events_queue.put(DownloadEvent(
                    task_id=task_id,
                    state=download.state,
                    output_file=None
                ))
                await f.truncate(download.file_size_bytes)

            try:
                await self._create_task_pool(download)
            except Exception as err:
                await self._log_and_share_error_event(download, err)
        else:
            if await self._check_if_complete_file_on_disk(download):
                return False
            self._tasks[task_id] = asyncio.create_task(self._download_file_coroutine(download))
        return True

    # TODO: get rid of resume_download and just call start
    async def resume_download(self, task_id: int) -> bool:
        """
        Wraps download coroutine into task to resume download
        """
        logging.debug("resume_download called")
        if task_id not in self._downloads:
            logging.warning(f"Received invalid {task_id=} in resume_download")
            return False

        download = self._downloads[task_id]

        if download.state not in [DownloadState.PAUSED, DownloadState.ERROR]:
            return False
        if download.state == DownloadState.ERROR:
            return await self.start_download(task_id)
        else:            
            try:
                await self._check_download_headers(download)
            except Exception as err:
                await self._log_and_share_error_event(download, err)
                return False

            if await self._check_if_complete_file_on_disk(download):
                return False

            self._tasks[task_id] = asyncio.create_task(self._download_file_coroutine(self._downloads[task_id]))

        return True

    async def pause_download(self, task_id: int) -> bool:
        """
        Pauses a download if it is running
        Removes task from _tasks

        :param task_id: Identifies which task to pause
        """
        logging.debug("pause_download called")
        if task_id not in self._downloads:
            logging.warning(f"Pause download called with invalid {task_id=}")
            return False
        
        download = self._downloads[task_id]
        if download.state != DownloadState.RUNNING:
            logging.warning("Pause download called on non-running task")
            return False

        if download.use_parallel_download:
            if task_id not in self._task_pools:
                raise Exception("Error: task_id not in DownloadManager task pool list")
            task_pool = self._task_pools[task_id]
            for task in task_pool:
                if not task.done():
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
            del self._tasks[task_id]

            del self._task_pools[download.task_id]
        else:
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
        return True

    async def delete_download(self, task_id: int, remove_file: bool = False) -> bool:
        """
        Removes download from _downloads and pauses the download if it is currently running

        :param remove_file: Whether or not to delete the output file
        """
        logging.debug("delete_download called")
        if task_id not in self._downloads:
            logging.warning(f"Download Manager delete_download called with invalid {task_id=}")
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
                state=DownloadState.DELETED,
                output_file=None
            )
        )

        return True

    async def _check_download_headers(self, download: DownloadMetadata):
        """
        Parses headers from server

        - Collects ETag, Content-Length, Content-Type, Accept-Ranges to fill download metadata accordingly

        - Verifies ETag and Content-Length still match download metadata
            - Deletes already downloaded data if they no longer match the response from the server
        """

        async with self._session.head(download.url) as resp:
            if "ETag" in resp.headers:
                if download.etag ==None:
                    download.etag = resp.headers["ETag"][1:-1]
                elif resp.headers["ETag"][1:-1] != download.etag:
                    logging.debug(f"Etag changed for {download.task_id=}, {download.url=}, {download.output_file=}. Restarting download from scratch.")
                    download.etag = resp.headers["ETag"][1:-1]
                    if os.path.exists(download.output_file):
                        os.remove(download.output_file)
                        download.downloaded_bytes = 0

            if "Content-Length" in resp.headers:
                if download.file_size_bytes is None:
                    download.file_size_bytes = int(resp.headers["Content-Length"])
                elif download.file_size_bytes != int(resp.headers["Content-Length"]):
                    logging.debug(f"File size changed for {download.task_id=}, {download.url=}, {download.output_file=}. Restarting download from scratch.")
                    download.file_size_bytes = resp.headers["Content-Length"]
                    if os.path.exists(download.output_file):
                        os.remove(download.output_file)
                        download.downloaded_bytes = 0

            if "Accept-Ranges" in resp.headers:
                download.server_supports_http_range = resp.headers["Accept-Ranges"] == "bytes"
            
            if download.output_file == "":
                if "ETag" in resp.headers:
                    download.output_file = resp.headers["ETag"][1:-1] + guess_extension(resp.headers.get("Content-Type", ".file"))
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

        download.downloaded_bytes = os.path.getsize(download.output_file) if os.path.exists(download.output_file) else 0
    
    async def _check_if_complete_file_on_disk(self, download: DownloadMetadata):
        if download.file_size_bytes is not None:
            if download.downloaded_bytes == download.file_size_bytes:
                download.state = DownloadState.COMPLETED
                await self.events_queue.put(DownloadEvent(
                    task_id=download.task_id,
                    state=download.state,
                    output_file=download.output_file
                ))
                del self._tasks[download.task_id]
                return True
            elif download.downloaded_bytes > download.file_size_bytes:
                download.state = DownloadState.ERROR
                await self.events_queue.put(DownloadEvent(
                    task_id=download.task_id,
                    state= download.state,
                    error_string="There is already a downloaded file with the same name that is larger than expected.",
                    output_file=download.output_file
                ))
                raise Exception("Downloaded bytes > expected file size")
        
        return False

    
    async def _create_task_pool(self, download: DownloadMetadata):
        task_id = download.task_id
        self._task_pools[task_id] = []
        self._data_queues[task_id] = asyncio.Queue()

        prev_bytes = None

        if download.file_size_bytes < ONE_GibiB:
            increment = int(download.file_size_bytes // 4)
        else:
            increment = int(download.file_size_bytes // (download.file_size_bytes // (ONE_GibiB/2)))
        
        for end_bytes in range(0, download.file_size_bytes + 1, increment):
            if prev_bytes is not None:
                await self._data_queues[task_id].put((prev_bytes, end_bytes))
            prev_bytes = end_bytes

        n_workers = min(self.maximum_workers_per_task, self._data_queues[task_id].qsize())

        logging.debug(f"Starting {n_workers=}")
        for n in range(n_workers):
            self._task_pools[task_id].append(
                asyncio.create_task(self._parallel_download_coroutine(
                    download,
                    n
                ))
            )
            

    async def _parallel_download_coroutine(self, download: DownloadMetadata, worker_id) -> None:
        logging.debug(f"Task {download.task_id}, Worker {worker_id} initialized.")
        next_write_byte = 0
        end_bytes = 0
        while True:
            try:
                start_bytes, end_bytes = self._data_queues[download.task_id].get_nowait()
                logging.debug(f"Worker {worker_id} picked up download range ({start_bytes}, {end_bytes})")
                next_write_byte = start_bytes
                
                headers = {
                    "Range": f"bytes={start_bytes}-{end_bytes}"
                }

                last_running_update = datetime.now()
                last_active_time_update = datetime.now()

                async with aiofiles.open(download.output_file, "r+b") as f:                    
                    async with self._session.get(download.url, headers=headers) as resp:
                        async for chunk in resp.content.iter_chunked(self.chunk_write_size_mb * 1024 * 1024):
                            chunk_start_time = datetime.now()
                            if resp.status != 206:
                                raise Exception(f"[Parallel] Received unexpected status: {resp.status=}")
                            await f.seek(next_write_byte)
                            await f.write(chunk)

                            next_write_byte += len(chunk)

                            chunk_time_delta = datetime.now() - chunk_start_time
                            download.downloaded_bytes += len(chunk)

                        download.active_time += datetime.now() - last_active_time_update 
                        last_active_time_update = datetime.now()

                        if (datetime.now() - last_running_update) > self.running_event_update_rate_seconds:
                            last_running_update = datetime.now()

                            download.state = DownloadState.RUNNING
                            await self.events_queue.put(DownloadEvent(
                                task_id=download.task_id,
                                state=download.state,
                                output_file=download.output_file,
                                download_speed=len(chunk)/chunk_time_delta.total_seconds(),
                                active_time=download.active_time,
                                downloaded_bytes=download.downloaded_bytes,
                                download_size_bytes=download.file_size_bytes,
                                worker_id=worker_id
                            ))
            
            except asyncio.CancelledError:
                if next_write_byte != end_bytes:
                    self._data_queues[download.task_id].put_nowait((next_write_byte, end_bytes))
                download.state = DownloadState.PAUSED
                await self.events_queue.put(DownloadEvent(
                    task_id=download.task_id,
                    state= download.state,
                    output_file=download.output_file
                ))
                raise
            except asyncio.QueueEmpty:
                logging.debug(f"Worker {worker_id} found no tasks, worker complete.")
                break
            except Exception as err:
                # TODO Count worker errors and handle accordingly
                # TODO Create a new worker and retry?
                if next_write_byte != end_bytes:
                    self._data_queues[download.task_id].put_nowait((next_write_byte, end_bytes))
                
                logging.error(f"Worker {worker_id}, Error: {repr(err)}, {err}")
                download.state = DownloadState.ERROR
                await self.events_queue.put(DownloadEvent(
                    task_id=download.task_id,
                    state= download.state,
                    error_string=f"{repr(err)}, {err}",
                    output_file=download.output_file
                ))
                return

        download.state = DownloadState.COMPLETED
        await self.events_queue.put(DownloadEvent(
            task_id=download.task_id,
            state= download.state,
            output_file=download.output_file,
            download_speed=0,
            active_time=download.active_time,
            downloaded_bytes=download.downloaded_bytes,
            download_size_bytes=download.file_size_bytes,
            worker_id=worker_id
        ))

    async def _download_file_coroutine(self, download: DownloadMetadata) -> None:
        headers = {}
        if download.server_supports_http_range:
            headers["Range"] = f"bytes={download.downloaded_bytes}-"
        
        download.state = DownloadState.RUNNING
        await self.events_queue.put(DownloadEvent(
            task_id=download.task_id,
            state=download.state,
            output_file=download.output_file
        ))

        last_running_update = datetime.now()
        last_active_time_update = datetime.now()
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(download.url, headers=headers) as resp:
                    async for chunk in resp.content.iter_chunked(self.chunk_write_size_mb * 1024 * 1024):
                        chunk_start_time = datetime.now()
                        if resp.status == 206:
                            mode = "ab"
                        elif resp.status == 200:
                            mode = "wb"
                        else:
                            raise Exception(f"Received unexpected status: {resp.status=}")
                        async with aiofiles.open(download.output_file, mode) as f:
                            await f.write(chunk)

                        chunk_time_delta = datetime.now() - chunk_start_time
                        download.downloaded_bytes += len(chunk)

                        download.active_time += datetime.now() - last_active_time_update
                        last_active_time_update = datetime.now()
                        
                        if (datetime.now() - last_running_update) > self.running_event_update_rate_seconds:
                            last_running_update = datetime.now()
                            download.state = DownloadState.RUNNING
                            await self.events_queue.put(DownloadEvent(
                                task_id=download.task_id,
                                state=download.state,
                                output_file=download.output_file,
                                download_speed=len(chunk)/chunk_time_delta.total_seconds(),
                                active_time=download.active_time,
                                downloaded_bytes=download.downloaded_bytes,
                                download_size_bytes=download.file_size_bytes
                            ))

            download.state = DownloadState.COMPLETED
            await self.events_queue.put(DownloadEvent(
                task_id=download.task_id,
                state= download.state,
                output_file=download.output_file,
                download_speed=0,
                active_time=download.active_time,
                downloaded_bytes=download.downloaded_bytes,
                download_size_bytes=download.file_size_bytes
            ))

            del self._tasks[download.task_id]
        except asyncio.CancelledError:
            download.state = DownloadState.PAUSED
            await self.events_queue.put(DownloadEvent(
                task_id=download.task_id,
                state= download.state,
                output_file=download.output_file
            ))
            raise
        except Exception as err:
            await self._log_and_share_error_event(download, err)
        finally:
            if download.task_id in self._tasks:
                del self._tasks[download.task_id]


__all__ = ["DownloadManager", "DownloadMetadata", "DownloadState", "DownloadEvent"]
