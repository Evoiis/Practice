import asyncio
import pytest
import inspect

from dmanager.core import DownloadManager, DownloadState
from tests.helpers import wait_for_state, verify_file, wait_for_file_to_be_created


class MockETagResponse:
    """Mock response with configurable ETag"""
    def __init__(self, status, headers=None):
        self.status = status
        self.headers = headers or {}
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
    
    def update_etag(self, new_etag):
        """Update the ETag header"""
        self.headers["ETag"] = f'"{new_etag}"'
    
    def update_content_length(self, new_length):
        """Update the Content-Length header"""
        self.headers["Content-Length"] = str(new_length)


class MockETagSession:
    def __init__(self, response):
        self._response = response
        self.closed = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        pass

    def get(self, url, headers=None, timeout=None):
        return self._response

    def head(self, url, timeout):
        return self._response
    
    async def close(self):
        self.closed = True
        return


@pytest.mark.asyncio
async def test_etag_change_during_pause(monkeypatch, async_thread_runner, test_file_setup_and_cleanup):
    """Test detection when ETag changes between pause and resume"""
    chunks = [b"abc", b"def", b"ghi"]
    mock_url = "https://example.com/file.bin"
    mock_file_name = f"{inspect.currentframe().f_code.co_name}.txt"
    test_file_setup_and_cleanup(mock_file_name)

    mock_response = MockETagResponse(
        206,
        {
            "Content-Length": str(sum(len(c) for c in chunks)),
            "Accept-Ranges": "bytes",
            "ETag": '"original-etag"'
        }
    )
    
    monkeypatch.setattr("aiohttp.ClientSession", lambda: MockETagSession(mock_response))

    dm = DownloadManager()
    task_id = dm.add_download(mock_url, mock_file_name)
    async_thread_runner.submit(dm.start_download(task_id))

    await wait_for_state(dm, task_id, DownloadState.RUNNING)
    
    await mock_response.insert_chunk(chunks[0])
    wait_for_file_to_be_created(mock_file_name)
    
    # Pause download
    async_thread_runner.submit(dm.pause_download(task_id))
    await wait_for_state(dm, task_id, DownloadState.PAUSED)
    
    # Verify first chunk was written
    verify_file(mock_file_name, "abc")
    
    # Change ETag (simulating file changed on server)
    mock_response.update_etag("new-etag")
    
    # Resume download - should detect ETag change and restart
    async_thread_runner.submit(dm.start_download(task_id))
    
    await wait_for_state(dm, task_id, DownloadState.RUNNING)
    
    # File should be restarted from beginning
    download_metadata = dm.get_downloads()[task_id]
    assert download_metadata.etag == "new-etag"
    
    # Send all chunks for complete download
    for chunk in chunks:
        await mock_response.insert_chunk(chunk)
    mock_response.end_response()
    
    await wait_for_state(dm, task_id, DownloadState.COMPLETED)
    
    verify_file(mock_file_name, "abcdefghi")

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)


@pytest.mark.asyncio
async def test_etag_missing_then_appears(monkeypatch, async_thread_runner, test_file_setup_and_cleanup):
    """Test behavior when ETag is initially missing but appears on retry"""
    chunks = [b"abc", b"def", b"ghi"]
    mock_url = "https://example.com/file.bin"
    mock_file_name = f"{inspect.currentframe().f_code.co_name}.txt"
    test_file_setup_and_cleanup(mock_file_name)

    mock_response = MockETagResponse(
        206,
        {
            "Content-Length": str(sum(len(c) for c in chunks)),
            "Accept-Ranges": "bytes",
            # No ETag initially
        }
    )
    
    monkeypatch.setattr("aiohttp.ClientSession", lambda: MockETagSession(mock_response))

    dm = DownloadManager()
    task_id = dm.add_download(mock_url, mock_file_name)
    async_thread_runner.submit(dm.start_download(task_id))

    await wait_for_state(dm, task_id, DownloadState.RUNNING)
    
    download_metadata = dm.get_downloads()[task_id]
    assert download_metadata.etag is None
    
    await mock_response.insert_chunk(chunks[0])
    wait_for_file_to_be_created(mock_file_name)
    
    # Pause download
    async_thread_runner.submit(dm.pause_download(task_id))
    await wait_for_state(dm, task_id, DownloadState.PAUSED)
    
    # Add ETag (server now provides it)
    mock_response.update_etag("new-etag")
    
    # Resume download - should accept the new ETag
    async_thread_runner.submit(dm.start_download(task_id))
    await wait_for_state(dm, task_id, DownloadState.RUNNING)
    
    download_metadata = dm.get_downloads()[task_id]
    assert download_metadata.etag == "new-etag"
    
    for chunk in chunks[1:]:
        await mock_response.insert_chunk(chunk)
    mock_response.end_response()
    
    await wait_for_state(dm, task_id, DownloadState.COMPLETED)

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)


@pytest.mark.asyncio
async def test_file_size_change_detection(monkeypatch, async_thread_runner, test_file_setup_and_cleanup):
    """Test detection when Content-Length changes between requests"""
    chunks_v1 = [b"abc", b"def", b"ghi"]
    chunks_v2 = [b"1234", b"5678", b"9012", b"3456"]
    
    mock_url = "https://example.com/file.bin"
    mock_file_name = f"{inspect.currentframe().f_code.co_name}.txt"
    test_file_setup_and_cleanup(mock_file_name)

    mock_response = MockETagResponse(
        206,
        {
            "Content-Length": str(sum(len(c) for c in chunks_v1)),
            "Accept-Ranges": "bytes",
            "ETag": '"version-1"'
        }
    )
    
    monkeypatch.setattr("aiohttp.ClientSession", lambda: MockETagSession(mock_response))

    dm = DownloadManager()
    task_id = dm.add_download(mock_url, mock_file_name)
    async_thread_runner.submit(dm.start_download(task_id))

    await wait_for_state(dm, task_id, DownloadState.RUNNING)
    
    await mock_response.insert_chunk(chunks_v1[0])
    wait_for_file_to_be_created(mock_file_name)
    
    # Pause download
    async_thread_runner.submit(dm.pause_download(task_id))
    await wait_for_state(dm, task_id, DownloadState.PAUSED)
    
    # File size changes on server
    mock_response.update_content_length(sum(len(c) for c in chunks_v2))
    mock_response.update_etag("version-2")
    
    # Resume download - should detect size change and restart
    async_thread_runner.submit(dm.start_download(task_id))
    await wait_for_state(dm, task_id, DownloadState.RUNNING)
    
    download_metadata = dm.get_downloads()[task_id]
    assert download_metadata.file_size_bytes == sum(len(c) for c in chunks_v2)
    assert download_metadata.downloaded_bytes == 0  # Should restart
    
    # Send new version chunks
    for chunk in chunks_v2:
        await mock_response.insert_chunk(chunk)
    mock_response.end_response()
    
    await wait_for_state(dm, task_id, DownloadState.COMPLETED)
    
    verify_file(mock_file_name, b"".join(chunks_v2).decode())

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)


@pytest.mark.asyncio
async def test_etag_with_quotes_stripping(monkeypatch, async_thread_runner, test_file_setup_and_cleanup):
    """Test that ETag quotes are properly stripped"""
    chunks = [b"test"]
    mock_url = "https://example.com/file.bin"
    mock_file_name = f"{inspect.currentframe().f_code.co_name}.txt"
    test_file_setup_and_cleanup(mock_file_name)

    mock_response = MockETagResponse(
        206,
        {
            "Content-Length": str(sum(len(c) for c in chunks)),
            "Accept-Ranges": "bytes",
            "ETag": '"etag-with-quotes"'  # Quotes included
        }
    )
    
    monkeypatch.setattr("aiohttp.ClientSession", lambda: MockETagSession(mock_response))

    dm = DownloadManager()
    task_id = dm.add_download(mock_url, mock_file_name)
    async_thread_runner.submit(dm.start_download(task_id))

    await wait_for_state(dm, task_id, DownloadState.RUNNING)
    
    download_metadata = dm.get_downloads()[task_id]
    # ETag should have quotes stripped
    assert download_metadata.etag == "etag-with-quotes"
    assert '"' not in download_metadata.etag
    
    await mock_response.insert_chunk(chunks[0])
    mock_response.end_response()
    
    await wait_for_state(dm, task_id, DownloadState.COMPLETED)

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)
