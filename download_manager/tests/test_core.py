import asyncio
import pytest
import os
import logging


class MockResponse:
    def __init__(self, chunks, status=200, headers=None, chunk_iter_wait=0.5):
        self._chunks = chunks
        self.status = status
        self.headers = headers or {}
        self.content = self
        self.chunk_iter_wait = chunk_iter_wait

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def iter_chunked(self, n):
        # TODO: Use queue instead of sleep for more concrete control
        for c in self._chunks:
            await asyncio.sleep(self.chunk_iter_wait)
            yield c


class MockSession:
    def __init__(self, responses):
        self._responses = responses

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        pass

    def get(self, url, headers=None):
        return self._responses[url]

    def head(self, url):
        return self._responses[url]


@pytest.mark.asyncio
async def test_add_and_start_download(monkeypatch, async_thread_runner):
    from dmanager.core import DownloadManager, DownloadState

    # Prepare mock session
    chunks = [b"abc", b"def", b"ghi"]
    mock_url = "https://example.com/file.bin"
    mock_file_name = "file.bin"
    mock_resp = MockResponse(
        chunks=chunks, 
        status=206, 
        headers={
            "Content-Length": str(sum(len(c) for c in chunks)),
            "Accept-Ranges": "bytes"
        })
    mock_session = MockSession({mock_url: mock_resp})

    # Patch aiohttp.ClientSession() to return our mock session
    monkeypatch.setattr("aiohttp.ClientSession", lambda: mock_session)

    logging.debug("Add download to dmanager")
    dm = DownloadManager()
    future = async_thread_runner.submit(dm.add_and_start_download(mock_url, mock_file_name))

    task_id = future.result(1)
    logging.debug(f"{task_id=}")

    received_running_event = False

    logging.debug("Consume Events")
    counter = 0
    while True:
        await asyncio.sleep(1)
        counter += 1
        event = await dm.get_latest_event()
        logging.debug(f"{event=}")

        if event is None:
            continue

        if event.state == DownloadState.RUNNING and event.task_id == task_id:
            received_running_event = True
        
        if event.state == DownloadState.COMPLETED and event.task_id == task_id:
            assert(received_running_event, "Expected to receive download running event before completed event")
            break

        assert(counter < 20, "Download Manager took too long to respond!")
    
    download_metadata = dm.get_downloads()[1]

    logging.debug(download_metadata)

    assert(download_metadata.url == mock_url)
    assert(download_metadata.output_file == mock_file_name)
    assert(download_metadata.file_size_bytes == 9)
    assert(download_metadata.downloaded_bytes == 9)

    expected_text = "abcdefghi"
    with open(mock_file_name) as f:
        file_text = f.read()
        assert(file_text == expected_text, f"Downloaded file text did not match expected.\nDownloaded: {file_text}\nExpected: {expected_text}")

    if os.path.exists(mock_file_name):
        os.remove(mock_file_name)


def test_add_download():
    pass

@pytest.mark.asyncio
async def test_pause_download(monkeypatch, async_thread_runner):
    # Set up mocks with slow task
    MockResponse(
        chunks=[b"abc", b"def", b"ghi"], 
        status=206, 
        headers={
            "Content-Length": "9",
            "Accept-Ranges": "bytes"
        },
        chunk_iter_wait=2.0
    )

    # Add and start download

    # Call pause

    # Check task is paused
    pass

# TODO: Test dmanager._tasks is cleaned up properly

@pytest.mark.asyncio
async def test_resume_download(monkeypatch, async_thread_runner):

    # Set up mocks

    # 

    pass


@pytest.mark.asyncio
async def test_cancel_download(monkeypatch, async_thread_runner):
    pass

@pytest.mark.asyncio
async def test_header_etag_change(monkeypatch, async_thread_runner):
    pass

@pytest.mark.asyncio
async def test_header_file_size_change(monkeypatch, async_thread_runner):
    pass

@pytest.mark.asyncio
async def test_size_change(monkeypatch, async_thread_runner):
    pass

@pytest.mark.asyncio
async def test_no_http_range_support(monkeypatch, async_thread_runner):
    pass

@pytest.mark.asyncio
async def test_no_output_file_input(monkeypatch, async_thread_runner):
    pass

# NEGATIVE TESTS --------------------------------------------------------

@pytest.mark.asyncio
async def test_output_file_with_invalid_characters(monkeypatch, async_thread_runner):
    pass

@pytest.mark.asyncio
async def test_invalid_test_id(async_thread_runner):
    pass

@pytest.mark.asyncio
async def test_start_running_download(monkeypatch, async_thread_runner):
    pass

@pytest.mark.asyncio
async def test_resume_running_download(monkeypatch, async_thread_runner):
    pass

@pytest.mark.asyncio
async def test_cancel_completed_download(monkeypatch, async_thread_runner):
    pass

@pytest.mark.asyncio
async def test_start_completed_download(monkeypatch, async_thread_runner):
    pass

@pytest.mark.asyncio
async def test_pause_completed_download(monkeypatch, async_thread_runner):
    pass

@pytest.mark.asyncio
async def test_cancel_paused_download(monkeypatch, async_thread_runner):
    pass

@pytest.mark.asyncio
async def test_start_paused_download(monkeypatch, async_thread_runner):
    pass

@pytest.mark.asyncio
async def test_pause_paused_download(monkeypatch, async_thread_runner):
    pass

@pytest.mark.asyncio
async def test_input_invalid_url(monkeypatch, async_thread_runner):
    pass

@pytest.mark.asyncio
async def test_input_already_used_output_file(monkeypatch, async_thread_runner):
    pass

@pytest.mark.asyncio
async def test_resume_on_download_with_no_http_range_support(monkeypatch, async_thread_runner):
    pass



