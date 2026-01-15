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
import traceback

ONE_GIBIB = 1073741824
ONE_MEBIBYTE = 1048576
KIBIBYTE_256 = 262144

class DownloadState(Enum):
    PAUSED = 0
    RUNNING = 1
    COMPLETED = 2
    PENDING = 3
    DELETED = 4
    ALLOCATING_SPACE = 5
    ERROR = 6


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
    use_parallel_download: bool = None


class DownloadManager:
    """
    Async download manager using asyncio and aiohttp.
    """

    def __init__(self, running_event_update_rate_seconds: int = 1, parallel_running_event_update_rate_seconds: int = 0.5, maximum_workers_per_task: int = 5, minimum_workers_per_task: int = 1) -> None:
        assert minimum_workers_per_task > 0

        self._downloads: Dict[int, DownloadMetadata] = {}
        self._next_id = 0
        self.events_queue: asyncio.Queue[Dict[str, Any]] = asyncio.Queue()
        self._tasks: Dict[int, asyncio.Task[Any]] = {}
        self._task_pools: Dict[int, list] = {}
        self._preallocate_tasks: Dict[int, asyncio.Task[Any]] = {}
        self._data_queues: Dict[int, asyncio.Queue] = {}
        self._session: aiohttp.ClientSession = None

        self._running_event_update_rate_seconds = timedelta(seconds=running_event_update_rate_seconds)
        self._parallel_running_event_update_rate_seconds = timedelta(seconds=parallel_running_event_update_rate_seconds)
        self._maximum_workers_per_task = maximum_workers_per_task
        self._minimum_workers_per_task = minimum_workers_per_task

    def _iterate_and_get_id(self) -> int:
        self._next_id += 1
        return self._next_id
    
    async def _log_and_share_error_event(self, download: DownloadMetadata, err: Exception):
        """
        Log error and emit ERROR Download Event.
        """
        logging.error(f"{repr(err)}, {err}")
        download.state = DownloadState.ERROR
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
        
        for task_id in self._preallocate_tasks:
            if not self._preallocate_tasks[task_id].done():
                self._preallocate_tasks[task_id].cancel()
        
        if self._session is not None:
            await self._session.close()
                
    
    def get_downloads(self) -> Dict[int, DownloadMetadata]:
        return self._downloads

    async def get_oldest_event(self) -> Optional[DownloadEvent]:
        """
        Retrieve and remove the oldest event from the event queue.
        If the queue is empty, this method returns None.
        """
        if self.events_queue.empty():
            return None
        else:
            return self.events_queue.get_nowait()            

    def add_download(self, url: str, output_file: Optional[str] = "") -> int:
        """
        Register a new download task.

        - Generates a unique task ID
        - Sanitizes and validates the output file name
        - Creates initial DownloadMetadata in PENDING state

        Returns:
            int: unique task id
        """
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
        Start or restart a download task.

        - Initializes the aiohttp session if needed
        - Validates task state
        - Fetches and validates HTTP headers
        - Decides between single or parallel download mode
        - Launches the appropriate download coroutine

        Args:
            task_id (int): ID of the download task.
            use_parallel_download (Optional[bool]): Force enable/disable parallel downloads.

        Returns:
            bool: True if the download was started successfully, False otherwise.
        """

        if not self._session:
            self._session = aiohttp.ClientSession()

        if task_id not in self._downloads:
            logging.warning(f"Start Download: {task_id=} not found.")
            return False

        download = self._downloads[task_id]
        if download.state not in [DownloadState.PENDING, DownloadState.ERROR, DownloadState.PAUSED]:
            logging.warning(f"Received invalid request to start, {task_id=}. Task is in invalid state to be started/restarted.")
            return False

        if download.state == DownloadState.ERROR:
            if os.path.exists(download.output_file):
                os.remove(download.output_file)

        try:
            await self._check_download_headers(download)                
        except Exception as err:
            tb = traceback.format_exc()
            logging.error(f"Traceback: {tb}")
            await self._log_and_share_error_event(download, err)
            return False

        # Parallel download decision
        if download.use_parallel_download == None:
            download.use_parallel_download = False
            if (download.file_size_bytes > ONE_GIBIB and use_parallel_download is None) or use_parallel_download is True:
                download.use_parallel_download = True

            # If the server doesn't have htttp range support or didn't provide Content-Length then we can't use parallel download
            if not download.server_supports_http_range or download.file_size_bytes is None or use_parallel_download is False:
                download.use_parallel_download = False

        # Initialize async downloads
        if download.use_parallel_download:
            await self._run_parallel_connection_download(download) 
        else:
            await self._run_single_connection_download(download)
        return True

    async def _run_parallel_connection_download(self, download: DownloadMetadata):
        """
        Start a parallel download using multiple worker tasks.

        - Checks if the file is already complete on disk
        - Preallocates disk space if required
        - Creates a pool of worker tasks to download file ranges

        Args:
            download (DownloadMetadata): The download to process.
        """

        if await self._check_if_complete_file_on_disk(download):
            if download.task_id in self._data_queues and self._data_queues[download.task_id].empty():
                logging.debug("Found file in directory and download queues are empty. Marking download as complete.")
                download.state = DownloadState.COMPLETED
                await self.events_queue.put(DownloadEvent(
                    task_id=download.task_id,
                    state=download.state,
                    output_file=download.output_file
                ))
                del self._tasks[download.task_id]
                return False
        try:
            if not os.path.exists(download.output_file) or (os.path.exists(download.output_file) and os.path.getsize(download.output_file) != download.file_size_bytes):
                self._preallocate_tasks[download.task_id] = asyncio.create_task(self._preallocate_file_space_on_disk(download))
                await self._preallocate_tasks[download.task_id]

                if download.task_id in self._preallocate_tasks:
                    del self._preallocate_tasks[download.task_id]

            await self._create_task_pool(download)
        except asyncio.CancelledError:
            logging.debug(f"{download.task_id=} paused during pre-allocating phase.")
            download.state = DownloadState.PAUSED
            await self.events_queue.put(DownloadEvent(
                task_id=download.task_id,
                state= download.state,
                output_file=download.output_file
            ))
            raise
        except Exception as err:
            tb = traceback.format_exc()
            logging.error(f"Traceback: {tb}")
            await self._log_and_share_error_event(download, err)

    async def _preallocate_file_space_on_disk(self, download: DownloadMetadata):
        """
        Preallocate disk space for a download file.

        - writes zero bytes to disk to ensure the full file size is allocated before parallel downloads begin
        """

        download.state = DownloadState.ALLOCATING_SPACE
        await self.events_queue.put(DownloadEvent(
            task_id=download.task_id,
            state=download.state,
            output_file=None
        ))

        file_size_on_disk = 0
        if os.path.exists(download.output_file):
            file_size_on_disk = os.path.getsize(download.output_file)
            logging.debug(f"Found partially allocated file, resume from {file_size_on_disk=}")
        next_write_byte = file_size_on_disk
        async with aiofiles.open(download.output_file, "wb") as f:
            while next_write_byte < download.file_size_bytes:
                await f.seek(next_write_byte)
                chunk_size = min(KIBIBYTE_256, download.file_size_bytes - next_write_byte)
                await f.write(b"\x00" * chunk_size)
                next_write_byte += chunk_size


    async def _run_single_connection_download(self, download:DownloadMetadata):
        """
        Start a single-connection download.

        This method:
        - Resumes from existing file size if present
        - Checks if the file is already complete
        - Launches a single download coroutine
        """

        download.downloaded_bytes = os.path.getsize(download.output_file) if os.path.exists(download.output_file) else 0
        if await self._check_if_complete_file_on_disk(download):
            download.state = DownloadState.COMPLETED
            await self.events_queue.put(DownloadEvent(
                task_id=download.task_id,
                state=download.state,
                output_file=download.output_file
            ))
            del self._tasks[download.task_id]
            return False
        self._tasks[download.task_id] = asyncio.create_task(self._download_file_coroutine(download))

    async def pause_download(self, task_id: int) -> bool:
        """
        Pause an active download or preallocation task.

        - Cancels active tasks or worker pools
        - Updates download state to PAUSED and emits Paused DownloadState
        """

        try:
            logging.debug("[DM] pause_download called")
            if task_id not in self._downloads:
                logging.warning(f"Pause download called with invalid {task_id=}")
                return False            

            download = self._downloads[task_id]
            if download.state == DownloadState.ALLOCATING_SPACE:
                if task_id in self._preallocate_tasks:
                    task = self._preallocate_tasks[task_id]
                    if not task.done():
                        task.cancel()
                        try: 
                            await task
                        except asyncio.CancelledError:
                            pass
                    if task_id in self._preallocate_tasks:
                        del self._preallocate_tasks[task_id]
                    return True
                else:
                    return False

            if download.state != DownloadState.RUNNING:
                logging.warning("Pause download called on non-running task")
                return False

            logging.debug("[DM] pause_download after initial checks")
            if download.use_parallel_download:
                logging.debug(f"[DM] pause_download in parallel download, {self._task_pools=}")
                if task_id not in self._task_pools:
                    return False
                logging.debug(f"Attempting to stop tasks in {self._task_pools[task_id]}")
                task_pool = self._task_pools[task_id]
                for task in task_pool:
                    if not task.done():
                        task.cancel()
                        try:
                            await task
                        except asyncio.CancelledError:
                            pass

                del self._task_pools[download.task_id]
            else:
                logging.debug("[DM] pause_download in single connection download")
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
        except Exception as err:
            tb = traceback.format_exc()
            logging.error(f"Traceback: {tb}")
            await self._log_and_share_error_event(download, err)

    async def delete_download(self, task_id: int, remove_file: bool = False) -> bool:
        """
        Delete a download task and optionally remove its file.

        This method:
        - Pauses the download if running
        - Removes all internal task references
        - Emits a DELETED event

        Args:
            task_id (int): ID of the download to delete.
            remove_file (bool): Whether to delete the downloaded file.

        Returns:
            bool: True if deletion was successful, False otherwise.
        """

        try:
            logging.debug("delete_download called")
            if task_id not in self._downloads:
                logging.warning(f"Download Manager delete_download called with invalid {task_id=}")
                return False

            download = self._downloads[task_id]
            if self._downloads[task_id].state == DownloadState.RUNNING:
                if not await self.pause_download(task_id):
                    raise Exception("Error: pause_download failed in delete_download")
            
            if task_id in self._tasks:
                del self._tasks[task_id]

            if task_id in self._task_pools:
                del self._task_pools[task_id]
            
            if remove_file:
                if os.path.exists(self._downloads[task_id].output_file):
                    os.remove(self._downloads[task_id].output_file)
            
            if task_id in self._downloads:
                del self._downloads[task_id]

            await self.events_queue.put(
                DownloadEvent(
                    task_id=task_id,
                    state=DownloadState.DELETED,
                    output_file=None
                )
            )

            return True
        except Exception as err:
            tb = traceback.format_exc()
            logging.error(f"Traceback: {tb}")
            self._log_and_share_error_event(download, err)


    async def _check_download_headers(self, download: DownloadMetadata):
        """
        Fetch and validate HTTP headers for a download.

        This method:
        - Retrieves ETag, Content-Length, Accept-Ranges, Content-Type
        - Detects server-side changes to file size or ETag
        - Resets local file state if changes are detected
        - Generates an output filename if none is provided
        """

        async with self._session.head(download.url) as resp:
            if "ETag" in resp.headers:
                if download.etag == None:
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
                download.output_file = datetime.now().strftime("%m_%d_%Y_%H_%M_%S")
                if "Content-Type" in resp.headers:
                    guess_file_extension = guess_extension(resp.headers["Content-Type"])
                    if guess_file_extension is None:
                        download.output_file += ".file"
                    else:
                        download.output_file += guess_file_extension
                else:
                    download.output_file += ".file"
    
    async def _check_if_complete_file_on_disk(self, download: DownloadMetadata):
        """
        Check whether the downloaded file is already complete.

        - Compares file size on disk to expected Content-Length
        - Emits an error if an oversized file exists

        Returns:
            bool: True if the file is complete, False otherwise.

        Raises:
            Exception: If an oversized conflicting file exists.
        """

        output_file_size = os.path.getsize(download.output_file) if os.path.exists(download.output_file) else 0
        if output_file_size != 0:
            if output_file_size == download.file_size_bytes:
                return True
            elif output_file_size > download.file_size_bytes:
                download.state = DownloadState.ERROR
                await self.events_queue.put(DownloadEvent(
                    task_id=download.task_id,
                    state= download.state,
                    error_string="There is already a downloaded file with the same name with a greater size!",
                    output_file=download.output_file
                ))
                # TODO: rework output file naming
                raise Exception("Existing file blocking output file.")
        
        return False

    
    async def _create_task_pool(self, download: DownloadMetadata): 
        """
        Create worker tasks for parallel downloads.

        This method:
        - Splits the file into byte ranges
        - Enqueues ranges into a shared queue
        - Spawns worker tasks to process the ranges

        Args:
            download (DownloadMetadata): The download to parallelize.
        """

        task_id = download.task_id
        self._task_pools[task_id] = []

        if task_id not in self._data_queues:
            self._data_queues[task_id] = asyncio.Queue()
            prev_bytes = None
            if download.file_size_bytes is None or download.file_size_bytes == 0:
                raise Exception("Parallel download requires Content-Length header")
            if download.file_size_bytes < ONE_GIBIB:
                increment = int(download.file_size_bytes // 4)
            else:
                increment = int(download.file_size_bytes // (download.file_size_bytes // (ONE_GIBIB/2)))
            
            for end_bytes in range(0, download.file_size_bytes + 1, increment):
                if prev_bytes is not None:
                    await self._data_queues[task_id].put((prev_bytes, end_bytes))
                prev_bytes = end_bytes

        n_workers = max(min(self._maximum_workers_per_task, self._data_queues[task_id].qsize()), self._minimum_workers_per_task)

        logging.debug(f"{self._data_queues[task_id]=}")

        logging.debug(f"Starting {n_workers=}")
        for n in range(n_workers):
            self._task_pools[task_id].append(
                asyncio.create_task(self._parallel_download_coroutine(
                    download,
                    n
                ))
            )
            

    async def _parallel_download_coroutine(self, download: DownloadMetadata, worker_id) -> None:
        """
        Worker coroutine for parallel downloads.

        - Fetches byte ranges from a shared queue
        - Downloads and writes file chunks
        - Reports progress events
        - Handles cancellation and error recovery

        Args:
            download (DownloadMetadata): The associated download.
            worker_id (int): Worker identifier.
        """

        logging.debug(f"Task {download.task_id}, Worker {worker_id} initialized.")
        next_write_byte = 0
        end_bytes = 0
        active_time = timedelta()
        chunk_time_delta = timedelta()
        while True:
            try:
                start_bytes, end_bytes = self._data_queues[download.task_id].get_nowait()
                logging.debug(f"Task {download.task_id}, Worker {worker_id} picked up download range ({start_bytes}, {end_bytes})")
                next_write_byte = start_bytes
                
                headers = {
                    "Range": f"bytes={start_bytes}-{end_bytes}"
                }

                last_running_update = datetime.now() - self._parallel_running_event_update_rate_seconds
                last_active_time_update = datetime.now()

                async with aiofiles.open(download.output_file, "r+b") as f:
                    async with self._session.get(download.url, headers=headers) as resp:
                        async for chunk in resp.content.iter_chunked(KIBIBYTE_256):
                            chunk_start_time = datetime.now()
                            if resp.status != 206:
                                raise Exception(f"[Parallel] Received unexpected status: {resp.status=}")
                            await f.seek(next_write_byte)
                            await f.write(chunk)

                            next_write_byte += len(chunk)
                            download.downloaded_bytes += len(chunk)

                            chunk_time_delta = datetime.now() - chunk_start_time

                            active_time += datetime.now() - last_active_time_update
                            last_active_time_update = datetime.now()

                            if (datetime.now() - last_running_update) > self._parallel_running_event_update_rate_seconds:
                                last_running_update = datetime.now()

                                chunk_time_seconds = chunk_time_delta.total_seconds()
                                download.state = DownloadState.RUNNING
                                await self.events_queue.put(DownloadEvent(
                                    task_id=download.task_id,
                                    state=download.state,
                                    output_file=download.output_file,
                                    download_speed=len(chunk)/chunk_time_seconds if chunk_time_seconds > 0 else 0,
                                    active_time=active_time,
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
                    output_file=download.output_file,
                    worker_id=worker_id,
                    active_time=active_time
                ))
                raise
            except asyncio.QueueEmpty:
                logging.debug(f"Worker {worker_id} found no tasks, worker complete.")
                download.state = DownloadState.COMPLETED
                await self.events_queue.put(DownloadEvent(
                    task_id=download.task_id,
                    state= download.state,
                    output_file=download.output_file,
                    download_speed=0,
                    active_time=active_time,
                    downloaded_bytes=download.downloaded_bytes,
                    download_size_bytes=download.file_size_bytes,
                    worker_id=worker_id
                ))
                break
            except Exception as err:
                if next_write_byte != end_bytes:
                    self._data_queues[download.task_id].put_nowait((next_write_byte, end_bytes))

                logging.error(f"Worker {worker_id}, Error: {repr(err)}, {err}")
                tb = traceback.format_exc()
                logging.error(f"Traceback: {tb}")
                download.state = DownloadState.ERROR
                await self.events_queue.put(DownloadEvent(
                    task_id=download.task_id,
                    state= download.state,
                    error_string=f"{repr(err)}, {err}",
                    output_file=download.output_file,
                    worker_id=worker_id,
                    active_time=active_time
                ))
                return
        

    async def _download_file_coroutine(self, download: DownloadMetadata) -> None:
        """
        Single-connection download coroutine.

        This method:
        - Performs sequential HTTP downloads
        - Supports resume via HTTP Range headers
        - Emits periodic progress events
        - Handles cancellation and completion events

        Args:
            download (DownloadMetadata): The download to process.
        """

        try:
            headers = {}

            if download.server_supports_http_range:
                headers["Range"] = f"bytes={download.downloaded_bytes}-"
            
            # TODO Get rid of this running event update
            download.state = DownloadState.RUNNING
            await self.events_queue.put(DownloadEvent(
                task_id=download.task_id,
                state=download.state,
                output_file=download.output_file
            ))

            last_running_update = datetime.now() - self._running_event_update_rate_seconds
            last_active_time_update = datetime.now()
            chunk_time_delta = timedelta()

            async with self._session.get(download.url, headers=headers) as resp:
                async for chunk in resp.content.iter_chunked(KIBIBYTE_256):
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

                    if (datetime.now() - last_running_update) > self._running_event_update_rate_seconds:
                        last_running_update = datetime.now()
                        download.state = DownloadState.RUNNING
                        chunk_time_seconds = chunk_time_delta.total_seconds()
                        await self.events_queue.put(DownloadEvent(
                            task_id=download.task_id,
                            state=download.state,
                            output_file=download.output_file,
                            download_speed=len(chunk)/chunk_time_seconds if chunk_time_seconds > 0 else 0,
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
            tb = traceback.format_exc()
            logging.error(f"Traceback: {tb}")
            await self._log_and_share_error_event(download, err)

__all__ = ["DownloadManager", "DownloadMetadata", "DownloadState", "DownloadEvent"]
