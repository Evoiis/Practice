import asyncio
import pytest
import os
import inspect

from dmanager.core import DownloadManager, DownloadState
from tests.helpers import wait_for_state, verify_file, wait_for_file_to_be_created


class MockRangeResponse:
    """Mock response with configurable range behavior"""
    def __init__(self, status, headers=None, ignore_range=False, wrong_range=False):
        self.status = status
        self.headers = headers or {}
        self.content = self
        self.queue = asyncio.Queue()
        self.stop = False
        self.ignore_range = ignore_range
        self.wrong_range = wrong_range
        self.last_request_headers = {}

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


class MockRangeSession:
    def __init__(self, response):
        self._response = response
        self.closed = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        pass

    def get(self, url, headers=None, timeout=None):
        if headers:
            self._response.last_request_headers = headers
            
            # Simulate server ignoring range request
            if self._response.ignore_range and "Range" in headers:
                self._response.status = 200  # Return 200 instead of 206
        
        return self._response

    def head(self, url, timeout):
        return self._response
    
    async def close(self):
        self.closed = True
        return


@pytest.mark.asyncio
async def test_server_returns_wrong_content_length_in_range(monkeypatch, async_thread_runner, test_file_setup_and_cleanup):
    """Test when server returns different amount of data than Content-Length indicates"""
    mock_url = "https://example.com/file.bin"
    mock_file_name = f"{inspect.currentframe().f_code.co_name}.txt"
    test_file_setup_and_cleanup(mock_file_name)

    # Server says 9 bytes but we'll send more
    mock_response = MockRangeResponse(
        206,
        {
            "Content-Length": "9",
            "Accept-Ranges": "bytes"
        }
    )
    
    monkeypatch.setattr("aiohttp.ClientSession", lambda: MockRangeSession(mock_response))

    dm = DownloadManager()
    task_id = dm.add_download(mock_url, mock_file_name)
    async_thread_runner.submit(dm.start_download(task_id))

    await wait_for_state(dm, task_id, DownloadState.RUNNING)
    
    # Send more data than Content-Length indicates
    await mock_response.insert_chunk(b"abcdefghijklmnop")  # 16 bytes instead of 9
    mock_response.end_response()
    
    # Download should complete with what was sent
    await wait_for_state(dm, task_id, DownloadState.COMPLETED)
    
    download = dm.get_downloads()[task_id]
    # Downloaded more than expected
    assert download.downloaded_bytes >= 9

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)


@pytest.mark.asyncio
async def test_resume_with_corrupted_partial_file(async_thread_runner, create_mock_response_and_set_mock_session, test_file_setup_and_cleanup):
    """Test when partial file exists but is larger than expected"""
    chunks = [b"abc", b"def", b"ghi"]
    mock_url = "https://example.com/file.bin"
    mock_file_name = f"{inspect.currentframe().f_code.co_name}.txt"
    test_file_setup_and_cleanup(mock_file_name)

    # Create a partial file that's TOO LARGE
    with open(mock_file_name, 'wb') as f:
        f.write(b"x" * 1000)  # Much larger than expected 9 bytes

    mock_response = create_mock_response_and_set_mock_session(
        206,
        {
            "Content-Length": str(sum(len(c) for c in chunks)),  # Only 9 bytes
            "Accept-Ranges": "bytes"
        },
        mock_url
    )

    dm = DownloadManager()
    task_id = dm.add_download(mock_url, mock_file_name)
    future = async_thread_runner.submit(dm.start_download(task_id))
    future.result(timeout=15)
    
    download = dm.get_downloads()[task_id]
    if download.output_file == mock_file_name:
        result = future.result(timeout=15)
        assert result is False
        await wait_for_state(dm, task_id, DownloadState.ERROR)
    else:
        await asyncio.sleep(1)
        assert os.path.exists(download.output_file)

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)

    if os.path.exists(download.output_file):
        os.remove(download.output_file)

@pytest.mark.asyncio
async def test_server_no_accept_ranges_header(async_thread_runner, create_mock_response_and_set_mock_session, test_file_setup_and_cleanup):
    """Test server that doesn't support range requests"""
    chunks = [b"abc", b"def", b"ghi"]
    mock_url = "https://example.com/file.bin"
    mock_file_name = f"{inspect.currentframe().f_code.co_name}.txt"
    test_file_setup_and_cleanup(mock_file_name)

    # No Accept-Ranges header
    mock_response = create_mock_response_and_set_mock_session(
        200,
        {
            "Content-Length": str(sum(len(c) for c in chunks)),
        },
        mock_url
    )

    dm = DownloadManager()
    task_id = dm.add_download(mock_url, mock_file_name)
    async_thread_runner.submit(dm.start_download(task_id))

    await wait_for_state(dm, task_id, DownloadState.RUNNING)
    
    download = dm.get_downloads()[task_id]
    # Should not use parallel download
    assert download.server_supports_http_range is False
    assert download.use_parallel_download is False
    
    for chunk in chunks:
        await mock_response.insert_chunk(chunk)
    mock_response.end_response()
    
    await wait_for_state(dm, task_id, DownloadState.COMPLETED)
    
    verify_file(mock_file_name, "abcdefghi")

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)


@pytest.mark.asyncio
async def test_server_accept_ranges_none(async_thread_runner, create_mock_response_and_set_mock_session, test_file_setup_and_cleanup):
    """Test server that explicitly doesn't support ranges (Accept-Ranges: none)"""
    chunks = [b"abc", b"def", b"ghi"]
    mock_url = "https://example.com/file.bin"
    mock_file_name = f"{inspect.currentframe().f_code.co_name}.txt"
    test_file_setup_and_cleanup(mock_file_name)

    mock_response = create_mock_response_and_set_mock_session(
        200,
        {
            "Content-Length": str(sum(len(c) for c in chunks)),
            "Accept-Ranges": "none"  # Explicitly doesn't support ranges
        },
        mock_url
    )

    dm = DownloadManager()
    task_id = dm.add_download(mock_url, mock_file_name)
    async_thread_runner.submit(dm.start_download(task_id))

    await wait_for_state(dm, task_id, DownloadState.RUNNING)
    
    download = dm.get_downloads()[task_id]
    assert download.server_supports_http_range is False
    assert download.use_parallel_download is False
    
    for chunk in chunks:
        await mock_response.insert_chunk(chunk)
    mock_response.end_response()
    
    await wait_for_state(dm, task_id, DownloadState.COMPLETED)

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)


@pytest.mark.asyncio
async def test_parallel_range_header_format(monkeypatch, async_thread_runner, test_file_setup_and_cleanup):
    """Test that parallel workers send correctly formatted Range headers"""
    
    mock_url = "https://example.com/file.txt"
    mock_file_name = f"{inspect.currentframe().f_code.co_name}.txt"
    test_file_setup_and_cleanup(mock_file_name)

    class MockParallelRangeResponse:
        def __init__(self):
            self.status = 206
            self.headers = {
                "Content-Length": str(2 * 1024),
                "Accept-Ranges": "bytes"
            }
            self.content = self
            self.received_ranges = []
        
        async def __aenter__(self):
            return self
        
        async def __aexit__(self, *args):
            return False
        
        async def iter_chunked(self, _):
            # Just return empty to complete quickly
            return
            yield
    
    class MockParallelRangeSession:
        def __init__(self, response):
            self.response = response
            self.closed = False
        
        async def __aenter__(self):
            return self
        
        async def __aexit__(self, *args):
            pass
        
        def get(self, url, headers=None, timeout=None):
            if headers and "Range" in headers:
                self.response.received_ranges.append(headers["Range"])
            return self.response
        
        def head(self, url, timeout):
            return self.response
        
        async def close(self):
            self.closed = True
            pass
    
    mock_response = MockParallelRangeResponse()
    monkeypatch.setattr("aiohttp.ClientSession", lambda: MockParallelRangeSession(mock_response))

    dm = DownloadManager(maximum_workers_per_task=2)
    task_id = dm.add_download(mock_url, mock_file_name)
    async_thread_runner.submit(dm.start_download(task_id, use_parallel_download=True))

    await wait_for_state(dm, task_id, DownloadState.ALLOCATING_SPACE, timeout_sec=60)
    
    # Give workers time to send requests
    await asyncio.sleep(2)
    
    # Should have received properly formatted range headers
    assert len(mock_response.received_ranges) > 0
    for range_header in mock_response.received_ranges:
        assert range_header.startswith("bytes=")
        assert "-" in range_header
    
    # Clean up
    async_thread_runner.submit(dm.pause_download(task_id))
    
    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)


@pytest.mark.asyncio
async def test_304_not_modified_response(monkeypatch, async_thread_runner, test_file_setup_and_cleanup):
    """Test handling of 304 Not Modified response"""
    mock_url = "https://example.com/file.bin"
    mock_file_name = f"{inspect.currentframe().f_code.co_name}.txt"
    test_file_setup_and_cleanup(mock_file_name)

    mock_response = MockRangeResponse(
        304,  # Not Modified
        {
            "ETag": '"same-etag"'
        }
    )
    
    monkeypatch.setattr("aiohttp.ClientSession", lambda: MockRangeSession(mock_response))

    dm = DownloadManager()
    task_id = dm.add_download(mock_url, mock_file_name)
    future = async_thread_runner.submit(dm.start_download(task_id))
    
    result = future.result(timeout=15)
    assert result is False
    
    # Should error on 304
    await wait_for_state(dm, task_id, DownloadState.ERROR)

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)


@pytest.mark.asyncio
async def test_416_range_not_satisfiable(monkeypatch, async_thread_runner, test_file_setup_and_cleanup):
    """Test handling of 416 Range Not Satisfiable response"""
    mock_url = "https://example.com/file.bin"
    mock_file_name = f"{inspect.currentframe().f_code.co_name}.txt"
    test_file_setup_and_cleanup(mock_file_name)

    # Create file that's already complete
    with open(mock_file_name, 'wb') as f:
        f.write(b"complete")

    mock_response = MockRangeResponse(
        416,  # Range Not Satisfiable
        {
            "Content-Length": "8",
            "Accept-Ranges": "bytes"
        }
    )
    
    monkeypatch.setattr("aiohttp.ClientSession", lambda: MockRangeSession(mock_response))

    dm = DownloadManager()
    task_id = dm.add_download(mock_url, mock_file_name)
    future = async_thread_runner.submit(dm.start_download(task_id))
    
    result = future.result(timeout=15)
    
    # Should error on 416
    await wait_for_state(dm, task_id, DownloadState.ERROR)

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)
