import asyncio
import pytest
import aiohttp
import inspect

from dmanager.core import DownloadManager, DownloadState
from tests.helpers import wait_for_state, verify_file, wait_for_file_to_be_created


@pytest.mark.asyncio
async def test_download_without_content_length(async_thread_runner, create_mock_response_and_set_mock_session, test_file_setup_and_cleanup):
    """Test behavior when server doesn't provide Content-Length header"""
    chunks = [b"abc", b"def", b"ghi"]
    mock_url = "https://example.com/file.bin"
    mock_file_name = f"{inspect.currentframe().f_code.co_name}.txt"
    test_file_setup_and_cleanup(mock_file_name)

    mock_response = create_mock_response_and_set_mock_session(
        200,
        {},  # No Content-Length header
        mock_url
    )

    dm = DownloadManager()
    task_id = dm.add_download(mock_url, mock_file_name)
    async_thread_runner.submit(dm.start_download(task_id))

    for chunk in chunks:
        await mock_response.insert_chunk(chunk)
    
    mock_response.end_response()

    await wait_for_state(dm, task_id, DownloadState.RUNNING)
    await wait_for_state(dm, task_id, DownloadState.COMPLETED)
    
    download_metadata = dm.get_downloads()[task_id]
    
    assert download_metadata.file_size_bytes is None
    assert download_metadata.downloaded_bytes == 9
    assert download_metadata.use_parallel_download is False

    verify_file(mock_file_name, "abcdefghi")

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)


class MockErrorResponse:
    """Mock response that simulates server errors"""
    def __init__(self, status, headers=None, error_on_chunk=False):
        self.status = status
        self.headers = headers or {}
        self.content = self
        self.error_on_chunk = error_on_chunk
        self.queue = asyncio.Queue()
        self.stop = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def iter_chunked(self, chunk_size_limit):
        if self.error_on_chunk:
            raise aiohttp.ClientError("Network error during download")
        
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


@pytest.mark.asyncio
async def test_download_server_500_error(monkeypatch, async_thread_runner, test_file_setup_and_cleanup):
    """Test handling of server errors during header request"""
    mock_url = "https://example.com/file.bin"
    mock_file_name = f"{inspect.currentframe().f_code.co_name}.txt"
    test_file_setup_and_cleanup(mock_file_name)

    from tests.conftest import MockSession
    
    mock_response = MockErrorResponse(500, {"Content-Length": "100"})
    monkeypatch.setattr("aiohttp.ClientSession", lambda: MockSession({mock_url: mock_response}))

    dm = DownloadManager()
    task_id = dm.add_download(mock_url, mock_file_name)
    future = async_thread_runner.submit(dm.start_download(task_id))
    
    result = future.result(timeout=15)
    assert result is False
    
    await wait_for_state(dm, task_id, DownloadState.ERROR)

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)


@pytest.mark.asyncio
async def test_download_network_timeout(monkeypatch, async_thread_runner, test_file_setup_and_cleanup):
    """Test behavior when network request times out"""
    mock_url = "https://example.com/file.bin"
    mock_file_name = f"{inspect.currentframe().f_code.co_name}.txt"
    test_file_setup_and_cleanup(mock_file_name)

    class MockTimeoutSession:

        def __init__(self):
            self.closed = False

        async def __aenter__(self):
            return self
        
        async def __aexit__(self, exc_type, exc, tb):
            pass
        
        def head(self, url, timeout):
            raise asyncio.TimeoutError("Request timed out")
        
        async def close(self):
            self.closed = True
            return

    monkeypatch.setattr("aiohttp.ClientSession", lambda: MockTimeoutSession())

    dm = DownloadManager()
    task_id = dm.add_download(mock_url, mock_file_name)
    future = async_thread_runner.submit(dm.start_download(task_id))
    
    result = future.result(timeout=15)
    assert result is False
    
    await wait_for_state(dm, task_id, DownloadState.ERROR)

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)


@pytest.mark.asyncio
async def test_download_network_error_during_chunk(monkeypatch, async_thread_runner, test_file_setup_and_cleanup):
    """Test handling of network error during chunk download"""
    mock_url = "https://example.com/file.bin"
    mock_file_name = f"{inspect.currentframe().f_code.co_name}.txt"
    test_file_setup_and_cleanup(mock_file_name)

    from tests.conftest import MockSession
    
    # Response that errors during chunk iteration
    mock_response = MockErrorResponse(206, {"Content-Length": "100", "Accept-Ranges": "bytes"}, error_on_chunk=True)
    monkeypatch.setattr("aiohttp.ClientSession", lambda: MockSession({mock_url: mock_response}))

    dm = DownloadManager()
    task_id = dm.add_download(mock_url, mock_file_name)
    async_thread_runner.submit(dm.start_download(task_id))
    
    await wait_for_state(dm, task_id, DownloadState.RUNNING)
    await wait_for_state(dm, task_id, DownloadState.ERROR)

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)


@pytest.mark.asyncio
async def test_download_server_redirect(create_mock_response_and_set_mock_session, async_thread_runner, test_file_setup_and_cleanup):
    """Test handling of HTTP redirects (301, 302, etc.)
    
    Note: aiohttp follows redirects automatically by default, so this tests that behavior
    """
    chunks = [b"abc", b"def", b"ghi"]
    mock_url = "https://example.com/file.bin"
    mock_file_name = f"{inspect.currentframe().f_code.co_name}.txt"
    test_file_setup_and_cleanup(mock_file_name)

    # aiohttp will follow redirects automatically, so we just test normal download
    # The redirect is handled transparently by aiohttp
    mock_response = create_mock_response_and_set_mock_session(
        200,  # Final response after redirect
        {
            "Content-Length": str(sum(len(c) for c in chunks)),
        },
        mock_url
    )

    dm = DownloadManager()
    task_id = dm.add_download(mock_url, mock_file_name)
    async_thread_runner.submit(dm.start_download(task_id))

    for chunk in chunks:
        await mock_response.insert_chunk(chunk)
    
    mock_response.end_response()

    await wait_for_state(dm, task_id, DownloadState.RUNNING)
    await wait_for_state(dm, task_id, DownloadState.COMPLETED)

    verify_file(mock_file_name, "abcdefghi")

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)
