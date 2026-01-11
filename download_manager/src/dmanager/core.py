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

ONE_GibiB = 1073741824

# TODO: Support default download folder
# TODO: Support parallel downloads
# TODO: Save metadata to file: Persist preferences and download_metadata between restarts

class DownloadState(Enum):
    PAUSED = 0
    RUNNING = 1
    COMPLETED = 2
    PENDING = 3
    DELETED = 4
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
    time_completed: datetime = None
    time_added: datetime = datetime.now()
    state: DownloadState = DownloadState.PENDING
    server_supports_http_range: bool = False
    use_parallel_download: bool = False


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

        # TODO update mb -> MiB cause that's technically correct
        self.chunk_write_size_mb = chunk_write_size_mb
        self.running_event_update_rate_seconds = timedelta(seconds=running_event_update_rate_seconds)
        self.maximum_workers_per_task = maximum_workers_per_task

    def _iterate_and_get_id(self) -> int:
        self._next_id += 1
        return self._next_id

    def shutdown(self):
        for task_id in self._tasks:
            if not self._tasks[task_id].done():
                self._tasks[task_id].cancel()
        
        for task_id in self._task_pools:
            for task in self._task_pools[task_id]:
                task.cancel()
    
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
        logging.info("start_download called")
        if task_id not in self._downloads:
            return False

        download = self._downloads[task_id]
        if download.state not in [DownloadState.PENDING, DownloadState.ERROR]:
            logging.warning(f"Received request to start, {task_id=}, but task is not pending!")
            return False

        if download.state == DownloadState.ERROR:
            if os.path.exists(download.output_file):
                os.remove(download.output_file)

        if await self._check_download_headers(download) is False:
            return False

        # TODO: double check later but should be ok
        if download.file_size_bytes > ONE_GibiB and use_parallel_download is None:
            download.use_parallel_download = True
        elif use_parallel_download is True:
            download.use_parallel_download = True

        if not download.server_supports_http_range or use_parallel_download is False:
            download.use_parallel_download = False
        
        logging.warning(f"{download.file_size_bytes > ONE_GibiB}=")
        logging.warning(f"{download.server_supports_http_range=}")
        logging.warning(f"{download.use_parallel_download=}") # TODO clean up
        if download.use_parallel_download:
            async with aiofiles.open(download.output_file, "wb") as f:
                await f.truncate(download.file_size_bytes)
            
            logging.info(f"File allocated now creating task pool")
            logging.info(f"{self._task_pools=}")
            await self._create_task_pool(download)
        else:
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
            if await self._check_download_headers(download) is False:
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

    async def _check_download_headers(self, download: DownloadMetadata) -> bool:
        """
        Parses headers from server

        - Collects ETag, Content-Length, Content-Type, Accept-Ranges to fill download metadata accordingly

        - Verifies ETag and Content-Length still match download metadata
            - Deletes already downloaded data if they no longer match the response from the server
        """

        try:
            async with aiohttp.ClientSession() as session:
                async with session.head(download.url) as resp:
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
        except Exception as err:
            logging.error(err)
            download.state = DownloadState.ERROR
            await self.events_queue.put(DownloadEvent(
                task_id=download.task_id,
                state=download.state,
                output_file=download.output_file,
                error_string=str(err)
            ))
            return False

        download.downloaded_bytes = os.path.getsize(download.output_file) if os.path.exists(download.output_file) else 0

        # Check if the file has already been completely downloaded
        # TODO doesn't work with parallel downloads and truncate
        # if download.file_size_bytes is not None:
        #     if download.downloaded_bytes == download.file_size_bytes:
        #         download.state = DownloadState.COMPLETED
        #         await self.events_queue.put(DownloadEvent(
        #             task_id=download.task_id,
        #             state=download.state,
        #             output_file=download.output_file
        #         ))
        #         del self._tasks[download.task_id]
        #         return False
        #     elif download.downloaded_bytes > download.file_size_bytes:
        #         download.state = DownloadState.ERROR
        #         await self.events_queue.put(DownloadEvent(
        #             task_id=download.task_id,
        #             state= download.state,
        #             error_string="There is already a downloaded file with the same name that is larger than expected.",
        #             output_file=download.output_file
        #         ))
        #         raise Exception("Downloaded bytes > expected file size")

        return True
    
    async def _create_task_pool(self, download: DownloadMetadata):
        logging.info("Creating Task Pool")
        self._task_pools[download.task_id] = []
        data_queue = asyncio.Queue()

        prev_bytes = None
        logging.info(f"{download.file_size_bytes=}")

        if download.file_size_bytes < ONE_GibiB:
            increment = int(download.file_size_bytes // 4)
        else:
            increment = int(download.file_size_bytes // (download.file_size_bytes // (ONE_GibiB/2)))
        
        logging.info(f"{increment=}")
        for end_bytes in range(0, download.file_size_bytes + 1, increment):
            if prev_bytes is not None:
                await data_queue.put((prev_bytes, end_bytes))
            prev_bytes = end_bytes
        
        logging.info(download.file_size_bytes)
        logging.info(data_queue)

        n_workers = min(self.maximum_workers_per_task, data_queue.qsize())

        logging.info(f"Starting {n_workers=}")

        try:
            for n in range(n_workers):
                self._task_pools[download.task_id].append(
                    asyncio.create_task(self._parallel_download_coroutine(
                        download,
                        data_queue,
                        n
                    ))
                )
        except Exception as err:
            logging.error(err)
            download.state = DownloadState.ERROR
            await self.events_queue.put(DownloadEvent(
                task_id=download.task_id,
                state= download.state,
                error_string=str(err),
                output_file=download.output_file
            ))

        logging.info("End of create task pool")

    async def _parallel_download_coroutine(self, download: DownloadMetadata, data_queue: asyncio.Queue, worker_id) -> None:
        logging.warning(f"Hello from worker {worker_id}")
        while True:
            try:
                logging.warning(f"Worker {worker_id}, top")
                # if data_queue.empty():
                #     logging.warning(f"Worker {worker_id} found no work left, quitting")
                #     break
                # data = await data_queue.get()
                try:
                    start_bytes, end_bytes = data_queue.get_nowait()
                except asyncio.QueueEmpty:
                    logging.debug(f"Worker {worker_id} found no tasks, ending.")
                    break
                # start_bytes: int = data[0]
                # end_bytes: int = data[1]
                headers = {
                    "Range": f"bytes={start_bytes}-{end_bytes-1}"
                }

                last_running_update = datetime.now()
                # last_active_time_update = datetime.now()

                async with aiofiles.open(download.output_file, "r+b") as f:
                    async with aiohttp.ClientSession() as session:
                        async with session.get(download.url, headers=headers) as resp:
                            async for chunk in resp.content.iter_chunked(self.chunk_write_size_mb * 1024 * 1024):
                            # chunk_start_time = datetime.now()
                                if resp.status != 206:
                                    raise Exception(f"[Parallel] Received unexpected status: {resp.status=}")
                                await f.seek(start_bytes)
                                await f.write(chunk)

                                start_bytes += len(chunk)

                                # chunk_time_delta = datetime.now() - chunk_start_time
                                download.downloaded_bytes += len(chunk)

                            # logging.warning(f"Worker {worker_id}, finished writing chunk, {download.downloaded_bytes=}")

                            # download.active_time += datetime.now() - last_active_time_update 
                            # last_active_time_update = datetime.now()
                            
                            # TODO
                            # if (datetime.now() - last_running_update) > self.running_event_update_rate_seconds:
                            # last_running_update = datetime.now()
                            download.state = DownloadState.RUNNING
                            await self.events_queue.put(DownloadEvent(
                                task_id=download.task_id,
                                state=download.state,
                                output_file=download.output_file,
                                # download_speed=len(chunk)/chunk_time_delta.total_seconds(),
                                # active_time=download.active_time,
                                downloaded_bytes=download.downloaded_bytes,
                                download_size_bytes=download.file_size_bytes,
                                worker_id=worker_id
                            ))
                logging.warning(f"Worker {worker_id}, Bot")
            except asyncio.CancelledError:
                download.state = DownloadState.PAUSED
                await self.events_queue.put(DownloadEvent(
                    task_id=download.task_id,
                    state= download.state,
                    output_file=download.output_file
                ))
                raise
            except Exception as err:
                logging.error(f"Worker {worker_id} Error: {err}")
                download.state = DownloadState.ERROR
                await self.events_queue.put(DownloadEvent(
                    task_id=download.task_id,
                    state= download.state,
                    error_string=str(err),
                    output_file=download.output_file
                ))

        download.time_completed = datetime.now()
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
        # TODO: cleanup task pool when all tasks are done
        # TODO: add to download event to specify which worker is done
            


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

            download.time_completed = datetime.now()
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
            logging.error(err)
            download.state = DownloadState.ERROR
            await self.events_queue.put(DownloadEvent(
                task_id=download.task_id,
                state= download.state,
                error_string=str(err),
                output_file=download.output_file
            ))
        finally:
            if download.task_id in self._tasks:
                del self._tasks[download.task_id]


__all__ = ["DownloadManager", "DownloadMetadata", "DownloadState", "DownloadEvent"]
