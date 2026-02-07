import pytest
import logging
import inspect

from dmanager.core import DownloadManager, DownloadState
from tests.helpers import wait_for_state, verify_file, wait_for_file_to_be_created


@pytest.mark.asyncio
async def test_start_in_running_state(async_thread_runner, test_file_setup_and_cleanup, create_mock_response_and_set_mock_session):
    chunks = [b"abc", b"def", b"ghi"]
    mock_url = "https://example.com/file.bin"
    mock_file_name = f"{inspect.currentframe().f_code.co_name}.txt"
    test_file_setup_and_cleanup(mock_file_name)

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

    logging.debug("Wait for dm to create the file")
    wait_for_file_to_be_created(mock_file_name)

    future = async_thread_runner.submit(dm.start_download(task_id))

    assert future.result(timeout=15) is False, "start_download should have returned False"
    mock_response.end_response()
    await wait_for_state(dm, task_id, DownloadState.COMPLETED)

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)

@pytest.mark.asyncio
async def test_start_in_completed_state(async_thread_runner, test_file_setup_and_cleanup, create_mock_response_and_set_mock_session):
    logging.debug("Prepare mock session and response")
    chunks = [b"abc", b"def", b"ghi"]
    mock_url = "https://example.com/file.bin"

    mock_file_name = f"{inspect.currentframe().f_code.co_name}.txt"
    test_file_setup_and_cleanup(mock_file_name)

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

    for chunk in chunks:
        await mock_response.insert_chunk(chunk)
    
    mock_response.end_response()

    logging.debug("Wait for events.")
    await wait_for_state(dm, task_id, DownloadState.RUNNING)
    await wait_for_state(dm, task_id, DownloadState.COMPLETED)

    future = async_thread_runner.submit(dm.start_download(task_id))
    assert future.result(timeout=15) is False, "start_download should have returned False"

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)

@pytest.mark.asyncio
async def test_start_in_paused_state(async_thread_runner, test_file_setup_and_cleanup, create_mock_response_and_set_mock_session):
    chunks = [b"abc", b"def", b"ghi"]
    mock_url = "https://example.com/file.bin"
    mock_file_name = f"{inspect.currentframe().f_code.co_name}.txt"
    test_file_setup_and_cleanup(mock_file_name)

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

    logging.debug("Wait for dm to create the file")
    wait_for_file_to_be_created(mock_file_name)

    logging.debug("Pause download")
    async_thread_runner.submit(dm.pause_download(task_id))
    
    logging.debug("Wait for dm to emit download pause state")
    await wait_for_state(dm, task_id, DownloadState.PAUSED)

    future = async_thread_runner.submit(dm.start_download(task_id))

    assert future.result(timeout=15)

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)
# TEST_RESUME ------------------------------------------------------------

@pytest.mark.asyncio
async def test_resume_in_running_state(async_thread_runner, test_file_setup_and_cleanup, create_mock_response_and_set_mock_session):
    chunks = [b"abc", b"def", b"ghi"]
    mock_url = "https://example.com/file.bin"
    mock_file_name = f"{inspect.currentframe().f_code.co_name}.txt"
    test_file_setup_and_cleanup(mock_file_name)

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

    logging.debug("Wait for dm to create the file")
    wait_for_file_to_be_created(mock_file_name)

    future = async_thread_runner.submit(dm.start_download(task_id))    
    assert future.result(timeout=15) is False, "start_download should have returned False"

    mock_response.end_response()
    await wait_for_state(dm, task_id, DownloadState.COMPLETED)
    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)

@pytest.mark.asyncio
async def test_resume_completed_download(async_thread_runner, test_file_setup_and_cleanup, create_mock_response_and_set_mock_session):
    logging.debug("Prepare mock session and response")
    chunks = [b"abc", b"def", b"ghi"]
    mock_url = "https://example.com/file.bin"

    mock_file_name = f"{inspect.currentframe().f_code.co_name}.txt"
    test_file_setup_and_cleanup(mock_file_name)

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

    for chunk in chunks:
        await mock_response.insert_chunk(chunk)
    
    mock_response.end_response()

    logging.debug("Wait for events.")
    await wait_for_state(dm, task_id, DownloadState.RUNNING)
    await wait_for_state(dm, task_id, DownloadState.COMPLETED)

    future = async_thread_runner.submit(dm.start_download(task_id))    
    assert future.result(timeout=15) is False, "start_download should have returned False"

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)

# TEST_PAUSE ------------------------------------------------------------

@pytest.mark.asyncio
async def test_pause_completed_download(async_thread_runner, test_file_setup_and_cleanup, create_mock_response_and_set_mock_session):
    logging.debug("Prepare mock session and response")
    chunks = [b"abc", b"def", b"ghi"]
    mock_url = "https://example.com/file.bin"

    mock_file_name = f"{inspect.currentframe().f_code.co_name}.txt"
    test_file_setup_and_cleanup(mock_file_name)

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

    for chunk in chunks:
        await mock_response.insert_chunk(chunk)
    
    mock_response.end_response()

    logging.debug("Wait for events.")
    await wait_for_state(dm, task_id, DownloadState.RUNNING)
    await wait_for_state(dm, task_id, DownloadState.COMPLETED)

    future = async_thread_runner.submit(dm.pause_download(task_id))
    assert future.result(timeout=15) is False, "pause_download should have returned False"

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)

@pytest.mark.asyncio
async def test_pause_in_paused_state(async_thread_runner, test_file_setup_and_cleanup, create_mock_response_and_set_mock_session):
    chunks = [b"abc", b"def", b"ghi"]
    mock_url = "https://example.com/file.bin"
    mock_file_name = f"{inspect.currentframe().f_code.co_name}.txt"
    test_file_setup_and_cleanup(mock_file_name)

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

    logging.debug("Wait for dm to create the file")
    wait_for_file_to_be_created(mock_file_name)

    logging.debug("Pause download")
    async_thread_runner.submit(dm.pause_download(task_id))
    
    logging.debug("Wait for dm to emit download pause state")
    await wait_for_state(dm, task_id, DownloadState.PAUSED)

    future = async_thread_runner.submit(dm.pause_download(task_id))
    assert future.result(timeout=15) is False, "pause_download should have returned False"

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)

@pytest.mark.asyncio
async def test_pause_in_pending_state(async_thread_runner, test_file_setup_and_cleanup, create_mock_response_and_set_mock_session):
    logging.debug("Prepare mock session and response")
    chunks = [b"abc", b"def", b"ghi"]
    mock_url = "https://example.com/file.bin"

    mock_file_name = f"{inspect.currentframe().f_code.co_name}.txt"
    test_file_setup_and_cleanup(mock_file_name)

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

    future = async_thread_runner.submit(dm.pause_download(task_id))
    assert future.result(timeout=15) is False, "pause_download should have returned False"

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)

@pytest.mark.asyncio
async def test_pause_in_error_state(async_thread_runner, create_mock_response_and_set_mock_session, test_file_setup_and_cleanup):
    chunks = ["invalid chunk because a bytes-like object is required :)"]
    mock_url = "https://example.com/file.bin"
    mock_file_name = f"{inspect.currentframe().f_code.co_name}.txt"
    test_file_setup_and_cleanup(mock_file_name)

    mock_response = create_mock_response_and_set_mock_session(
        206,
        {
            "Content-Length": str(sum(len(c) for c in chunks)),
            "Accept-Ranges": "bytes"
        },
        mock_url
    )

    dm = DownloadManager()
    task_id = dm.add_download(mock_url, mock_file_name)
    async_thread_runner.submit(dm.start_download(task_id))

    await mock_response.insert_chunk(chunks[0])
    
    await wait_for_state(dm, task_id, DownloadState.RUNNING)
    await wait_for_state(dm, task_id, DownloadState.ERROR)

    logging.debug("Download is now in error state, now running pause_download")

    future = async_thread_runner.submit(dm.pause_download(task_id))
    assert future.result(timeout=15) is True, "pause_download should have returned True on pausing a task in error"

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)
