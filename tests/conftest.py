import asyncio
import pytest
import os
import logging
import copy
import time

from typing import Dict

from dmanager.asyncio_thread import AsyncioEventLoopThread
from dmanager.core import DownloadManager
from dmanager.constants import SEGMENT_SIZE

class MockResponse:
    def __init__(self, status, headers=None):
        self.status = status
        self.headers = headers
        self.content = self
        self.queue = asyncio.Queue()
        self.stop = False
        self.exception = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def iter_chunked(self, chunk_size_limit):
        while not self.stop or not self.queue.empty():
            if self.exception is not None:
                raise self.exception

            if not self.queue.empty():
                chunk = await self.queue.get()
                yield chunk
            else:
                await asyncio.sleep(0.5)
    
    async def insert_chunk(self, chunk):
        await self.queue.put(chunk)

    def end_response(self):
        self.stop = True

    async def empty_queue(self):
        while not self.queue.empty():
            await self.queue.get()

    def set_exception(self, exception:Exception):
        self.exception = exception


class MockSession:
    def __init__(self, responses):
        self._responses = responses
        self.closed = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        pass

    def get(self, url, headers=None, timeout=None):
        return self._responses[url]

    def head(self, url, timeout):
        return self._responses[url]
    
    async def close(self):
        self.closed = True
        return


class MockParallelResponse():
    def __init__(self, status, request_queue: asyncio.Queue, headers: dict, range_ends: list, data: dict):
        self.status = status
        self.headers = headers
        self.content = self
        self.request_queue = request_queue
        self.data = copy.deepcopy(data)
        self.send_n_letters: Dict[str, int] = {x:0 for x in range_ends}
        self.done = {x: False for x in range_ends}
        self.exception = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def iter_chunked(self, _):
        if self.data is None:
            return
        data_range_request = await self.request_queue.get()
        range_end = data_range_request.split("-")[-1]

        while True:            
            if self.exception is not None:
                raise self.exception

            if range_end not in self.send_n_letters:
                raise Exception(f"Got unexpected {range_end=}")
            

            if self.send_n_letters[range_end] > 0:
                if len(self.data[range_end]) > 0:
                    slice_size = min(1024, self.send_n_letters[range_end])
                    yield bytes(self.data[range_end][:slice_size])
                    self.data[range_end] = self.data[range_end][slice_size:]
                    self.send_n_letters[range_end] -= slice_size
                else:
                    # No more data to send
                    break
            else:
                await asyncio.sleep(1)
            if self.done[range_end] and self.send_n_letters[range_end] == 0:
                break
    
    def set_range_end_n_send(self, range_end, n):
        self.send_n_letters[range_end] = n
    
    def set_range_end_done(self, range_end):
        self.done[range_end] = True

    def end_response(self):
        self.stop = True

    def set_exception(self, exception:Exception):
        self.exception = exception

class MockParallelSession:
    def __init__(self, responses, request_queues):
        self._responses = responses
        self.request_queues = request_queues
        self.closed = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        pass

    def get(self, url, headers=None, timeout=None):
        self.request_queues[url].put_nowait(headers["Range"])
        return self._responses[url]

    def head(self, url, timeout):
        return self._responses[url]

    async def close(self):
        self.closed = True
        return

@pytest.fixture
def async_thread_runner(request):
    runner = AsyncioEventLoopThread()
    
    def cleanup():
        logging.debug("Async thread fixture shutting down.")
        runner.shutdown()

    request.addfinalizer(cleanup)
    return runner

@pytest.fixture
def create_mock_response_and_set_mock_session(monkeypatch):

    def factory(return_status, headers, mock_url):
        mock_res = MockResponse(return_status, headers)
        monkeypatch.setattr("aiohttp.ClientSession", lambda: MockSession({mock_url: mock_res}))
        return mock_res

    return factory

@pytest.fixture
def create_parallel_mock_response_and_set_mock_session(monkeypatch):

    def factory(return_status, headers, mock_url, range_ends, data):
        request_queue = asyncio.Queue()
        mock_response = MockParallelResponse(return_status, request_queue, headers, range_ends, data)
        monkeypatch.setattr("aiohttp.ClientSession", lambda: MockParallelSession({mock_url: mock_response}, {mock_url: request_queue}))
        return mock_response
    
    return factory

@pytest.fixture
def create_multiple_parallel_mock_response_and_mock_sessions(monkeypatch):

    def factory(mock_response_args: dict):
        mock_url_to_mock_responses = dict()
        mock_url_to_request_queue = dict()

        for url in mock_response_args:
            mock_url_to_request_queue[url] = asyncio.Queue()
            new_mock_response = MockParallelResponse(request_queue=mock_url_to_request_queue[url], **mock_response_args[url])
            mock_url_to_mock_responses[url] = new_mock_response
        
        mps = MockParallelSession(mock_url_to_mock_responses, mock_url_to_request_queue)

        monkeypatch.setattr("aiohttp.ClientSession", lambda: mps)
        return mock_url_to_mock_responses
    
    return factory

@pytest.fixture
def test_file_setup_and_cleanup(request):
    test_file_name = ""

    def setup(file_name):
        nonlocal test_file_name
        test_file_name = file_name
        if os.path.exists(test_file_name):
            os.remove(test_file_name)

    def cleanup():
        if test_file_name != "" and os.path.exists(test_file_name):
            logging.debug(f"Cleaning up {test_file_name=}")
            os.remove(test_file_name)
    
    request.addfinalizer(cleanup)
    yield setup


@pytest.fixture
def test_multiple_file_setup_and_cleanup(request):
    test_files = []

    def setup(files):
        nonlocal test_files
        test_files = files
        for file in test_files:
            if os.path.exists(file):
                os.remove(file)

    def cleanup():
        for file in test_files:
            if os.path.exists(file):
                logging.debug(f"Cleaning up {file=}")
                os.remove(file)
                
    
    request.addfinalizer(cleanup)
    yield setup

@pytest.fixture
def download_manager_fixture(request):
    download_manager = None
    runner = None

    def setup(
        async_runner: AsyncioEventLoopThread,
        running_event_update_rate_seconds: int = 1, 
        parallel_running_event_update_rate_seconds: int = 1, 
        maximum_workers_per_task: int = 5, 
        request_connect_timeout: float= 120.,
        request_read_timeout: float= 60.,
        request_total_timeout: float = None,
        parallel_download_segment_size: int= SEGMENT_SIZE,
        continue_on_error: bool= True,
        stop_continue_on_n_errors: int | None=5
    ):
        nonlocal runner
        nonlocal download_manager
        runner = async_runner
        download_manager = DownloadManager(
            running_event_update_rate_seconds=running_event_update_rate_seconds,
            parallel_running_event_update_rate_seconds=parallel_running_event_update_rate_seconds,
            maximum_workers_per_task=maximum_workers_per_task,
            request_connect_timeout=request_connect_timeout,
            request_read_timeout=request_read_timeout,
            request_total_timeout=request_total_timeout,
            parallel_download_segment_size=parallel_download_segment_size,
            continue_on_error=continue_on_error,
            stop_continue_on_n_errors=stop_continue_on_n_errors
        )
        return download_manager

    def cleanup():
        logging.debug("Download Manager fixture cleanup.")
        if download_manager:
            future = runner.submit(download_manager.shutdown())
            future.result(timeout=15)
    
    request.addfinalizer(cleanup)
    yield setup
