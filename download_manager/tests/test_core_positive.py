import asyncio
import pytest
import os
import logging

from dmanager.core import DownloadManager, DownloadState
from tests.helpers import wait_for_state, verify_file

@pytest.mark.asyncio
async def test_add_and_start_download(async_thread_runner, create_mock_response_and_set_mock_session):

    logging.debug("Prepare mock session and response")
    chunks = [b"abc", b"def", b"ghi"]
    mock_url = "https://example.com/file.bin"

    mock_file_name = "test_file.bin"
    if os.path.exists(mock_file_name):
        os.remove(mock_file_name)

    mock_response = create_mock_response_and_set_mock_session(
        206,
        {
            "Content-Length": str(sum(len(c) for c in chunks)),
            "Accept-Ranges": "bytes"
        },
        mock_url
    )    

    logging.debug("Add and start download")
    dm = DownloadManager()
    task_id = dm.add_download(mock_url, mock_file_name)
    async_thread_runner.submit(dm.start_download(task_id))

    logging.debug(f"{task_id=}")

    received_running_event = False

    for chunk in chunks:
        await mock_response.insert_chunk(chunk)
    
    mock_response.end_response()

    logging.debug("Consume Events")
    counter = 0
    while True:
        await asyncio.sleep(1)
        counter += 1
        event = await dm.get_oldest_event()
        logging.debug(f"{event=}")

        if event is None:
            assert(counter < 20, "Download Manager took too long to respond!")
            continue

        if event.state == DownloadState.RUNNING and event.task_id == task_id:
            received_running_event = True
        
        if event.state == DownloadState.COMPLETED and event.task_id == task_id:
            assert(received_running_event, "Expected to receive download running event before completed event")
            break
    
    download_metadata = dm.get_downloads()[1]

    logging.debug(download_metadata)

    assert(download_metadata.url == mock_url)
    assert(download_metadata.output_file == mock_file_name)
    assert(download_metadata.file_size_bytes == 9)
    assert(download_metadata.downloaded_bytes == 9)

    verify_file(mock_file_name, "abcdefghi")

    logging.debug("Cleanup")
    if os.path.exists(mock_file_name):
        os.remove(mock_file_name)


@pytest.mark.asyncio
async def test_pause_download(async_thread_runner, create_mock_response_and_set_mock_session):
    chunks = [b"abc", b"def", b"ghi"]
    mock_url = "https://example.com/file.bin"
    mock_file_name = "test_file.bin"
    if os.path.exists(mock_file_name):
        os.remove(mock_file_name)
    mock_response = create_mock_response_and_set_mock_session(
        206,
        {
            "Content-Length": str(sum(len(c) for c in chunks)),
            "Accept-Ranges": "bytes"
        },
        mock_url
    )

    logging.debug("Add and start download")
    dm = DownloadManager()
    task_id = dm.add_download(mock_url, mock_file_name)
    async_thread_runner.submit(dm.start_download(task_id))

    logging.debug("Wait for dm to emit download running state")
    await wait_for_state(dm, task_id, DownloadState.RUNNING)

    logging.debug("Give dm a chunk to write")
    await mock_response.insert_chunk(chunks[0])

    logging.debug("Wait for dm to write the chunk")
    count = 0
    while not os.path.exists(mock_file_name):
        await asyncio.sleep(1)
        count += 1
        assert(count < 20, "Timeout exceeded while waiting for file write")

    logging.debug("Pause download")
    async_thread_runner.submit(dm.pause_download(task_id))
    
    logging.debug("Wait for dm to emit download pause state")
    await wait_for_state(dm, task_id, DownloadState.PAUSED)

    # Task should be paused so we will give a chunk and wait some time to make sure pause stops file write
    await mock_response.insert_chunk(chunks[1])
    await asyncio.sleep(5)
    
    # Check _tasks is cleaned up and download state
    assert(task_id not in dm._tasks)
    assert(dm.get_downloads()[task_id].state == DownloadState.PAUSED)

    logging.debug("Verifying only the first chunk was written to file")
    verify_file(mock_file_name, "abc")

    logging.debug("Cleanup")
    if os.path.exists(mock_file_name):
        os.remove(mock_file_name)


@pytest.mark.asyncio
async def test_resume_download(async_thread_runner, create_mock_response_and_set_mock_session):
    chunks = [b"abc", b"def", b"ghi"]
    mock_url = "https://example.com/file.bin"
    mock_file_name = "test_file.bin"
    if os.path.exists(mock_file_name):
        os.remove(mock_file_name)
    mock_response = create_mock_response_and_set_mock_session(
        206,
        {
            "Content-Length": str(sum(len(c) for c in chunks)),
            "Accept-Ranges": "bytes"
        },
        mock_url
    )

    logging.debug("Add and start download")
    dm = DownloadManager()
    task_id = dm.add_download(mock_url, mock_file_name)
    async_thread_runner.submit(dm.start_download(task_id))

    logging.debug("Wait for dm to emit download running state")
    await wait_for_state(dm, task_id, DownloadState.RUNNING)

    logging.debug("Give dm a chunk to write")
    await mock_response.insert_chunk(chunks[0])

    logging.debug("Wait for dm to write the chunk")
    count = 0
    while not os.path.exists(mock_file_name):
        await asyncio.sleep(1)
        count += 1
        assert(count < 20, "Timeout exceeded while waiting for file write")

    logging.debug("Pause download")
    async_thread_runner.submit(dm.pause_download(task_id))

    logging.debug("Wait for dm to emit download pause state")
    await wait_for_state(dm, task_id, DownloadState.PAUSED)

    # Task should be paused so we will give a chunk and wait some time to make sure pause stops file write
    await mock_response.insert_chunk(chunks[1])
    await asyncio.sleep(5)

    logging.debug("Resume Download")
    async_thread_runner.submit(dm.resume_download(task_id))
    
    await wait_for_state(dm, task_id, DownloadState.RUNNING)

    await mock_response.insert_chunk(chunks[2])

    # Verify
    verify_file(mock_file_name, "abcghi")


    # Cleanup
    logging.debug("Cleanup")
    if os.path.exists(mock_file_name):
        os.remove(mock_file_name)


@pytest.mark.asyncio
async def test_cancel_download(async_thread_runner):
    pass

@pytest.mark.asyncio
async def test_download_with_no_http_range_support():
    pass



# TODO: Error path tests
# Chunk write Failure
    # Fail Gracefully if can't get file?
# Asyncio/Aiohttp errors
    # Network disconnect
    # Server Errors 500

# TODO: Concurrency tests
"""
Put tests like:

Tests asserting event ordering

Tests asserting _tasks cleanup

Pause/resume timing correctness

Partial-write verification

Race-condition-related tests
"""


