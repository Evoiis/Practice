import asyncio
import pytest
import os
import logging
import copy

from typing import Dict
from dmanager.asyncio_thread import AsyncioEventLoopThread


class MockResponse:
    def __init__(self, status, headers={}):
        self.status = status
        self.headers = headers
        self.content = self
        self.queue = asyncio.Queue()
        self.stop = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def iter_chunked(self, chunk_size_limit):
        while not self.stop or not self.queue.empty():
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


class MockSession:
    def __init__(self, responses):
        self._responses = responses

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        pass

    def get(self, url, headers=None, timeout=None):
        return self._responses[url]

    def head(self, url, timeout):
        return self._responses[url]
    
    async def close(self):
        return


class MockParallelResponse():
    def __init__(self, status, request_queue: asyncio.Queue, headers: dict, range_ends: list, data: dict):
        self.status = status
        self.headers = headers
        self.content = self
        self.request_queue = request_queue
        self.data = copy.deepcopy(data)
        self.send_next_letter: Dict[str: int] = {x:0 for x in range_ends}
        self.done = {x: False for x in range_ends}

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
            if range_end not in self.send_next_letter:
                    raise Exception(f"Got unexpected {range_end=}")
            if self.send_next_letter[range_end] > 0:
                if len(self.data[range_end]) > 0:
                    slice_size = min(1024*256, len(self.data[range_end]))
                    yield bytes(self.data[range_end][:slice_size])
                    self.data[range_end] = self.data[range_end][slice_size:]
                    self.send_next_letter[range_end] -= slice_size
                else:
                    # No more data to send
                    break
            else:
                await asyncio.sleep(1)
            if self.done[range_end] and self.send_next_letter[range_end] == 0:
                break
    
    def set_range_end_n_send(self, range_end, n):
        self.send_next_letter[range_end] = n
    
    def set_range_end_done(self, range_end):
        self.done[range_end] = True

    def end_response(self):
        self.stop = True

    async def empty_queue(self):
        while not self.queue.empty():
            await self.queue.get()


class MockParallelSession:
    def __init__(self, responses, request_queues):
        self._responses = responses
        self.request_queues = request_queues

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
        return


@pytest.fixture
def async_thread_runner():
    runner = AsyncioEventLoopThread()
    yield runner
    runner.shutdown()

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
        if os.path.exists(test_file_name):
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

