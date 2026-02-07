import pytest
import logging
import inspect

from dmanager.core import DownloadManager, DownloadState
from tests.helpers import wait_for_state, verify_file

@pytest.mark.asyncio
async def test_header_etag_change(async_thread_runner, test_file_setup_and_cleanup, create_mock_response_and_set_mock_session):
    chunks = [b"abc", b"def", b"ghi"]
    mock_url = "https://example.com/file.bin"
    mock_file_name = f"{inspect.currentframe().f_code.co_name}.txt"
    test_file_setup_and_cleanup(mock_file_name)

    mock_response = create_mock_response_and_set_mock_session(
        206,
        {
            "Content-Length": 9,
            "Accept-Ranges": "bytes",
            "ETag": '"1"'
        },
        mock_url
    )

    logging.debug("Add and start download")
    dm = DownloadManager()
    task_id = dm.add_download(mock_url, mock_file_name)
    async_thread_runner.submit(dm.start_download(task_id))

    logging.debug("Wait for dm to emit download running state")
    await wait_for_state(dm, task_id, DownloadState.RUNNING)

    await mock_response.insert_chunk(chunks[0])
    await wait_for_state(dm, task_id, DownloadState.RUNNING)

    logging.debug("Pause download")
    async_thread_runner.submit(dm.pause_download(task_id))
    
    logging.debug("Wait for dm to emit download pause state")
    await wait_for_state(dm, task_id, DownloadState.PAUSED)

    mock_response.headers["ETag"] = '"2"'

    async_thread_runner.submit(dm.start_download(task_id))

    await mock_response.insert_chunk(chunks[1])
    await mock_response.insert_chunk(chunks[2])
    mock_response.end_response()

    await wait_for_state(dm, task_id, DownloadState.COMPLETED)

    verify_file(mock_file_name, "defghi")

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)


@pytest.mark.asyncio
async def test_resume_on_header_content_length_change(async_thread_runner, test_file_setup_and_cleanup, create_mock_response_and_set_mock_session):
    chunks = [b"abc", b"def", b"ghi"]
    mock_url = "https://example.com/file.bin"
    mock_file_name = f"{inspect.currentframe().f_code.co_name}.txt"
    test_file_setup_and_cleanup(mock_file_name)

    mock_response = create_mock_response_and_set_mock_session(
        206,
        {
            "Content-Length": 9,
            "Accept-Ranges": "bytes",
            "ETag": "1"
        },
        mock_url
    )

    logging.debug("Add and start download")
    dm = DownloadManager()
    task_id = dm.add_download(mock_url, mock_file_name)
    async_thread_runner.submit(dm.start_download(task_id))

    logging.debug("Wait for dm to emit download running state")
    await wait_for_state(dm, task_id, DownloadState.RUNNING)

    await mock_response.insert_chunk(chunks[0])
    await wait_for_state(dm, task_id, DownloadState.RUNNING)

    logging.debug("Pause download")
    async_thread_runner.submit(dm.pause_download(task_id))
    
    logging.debug("Wait for dm to emit download pause state")
    await wait_for_state(dm, task_id, DownloadState.PAUSED)

    mock_response.headers["Content-Length"] = 3

    async_thread_runner.submit(dm.start_download(task_id))

    await mock_response.insert_chunk(chunks[1])
    await mock_response.insert_chunk(chunks[2])
    mock_response.end_response()

    await wait_for_state(dm, task_id, DownloadState.COMPLETED)

    verify_file(mock_file_name, "defghi")

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)

@pytest.mark.asyncio
async def test_download_with_no_http_range_support(async_thread_runner, test_file_setup_and_cleanup, create_mock_response_and_set_mock_session):
    mock_url = "https://example.com/file.bin"
    mock_file_name = f"{inspect.currentframe().f_code.co_name}.txt"
    test_file_setup_and_cleanup(mock_file_name)

    mock_response = create_mock_response_and_set_mock_session(
        200,
        {
            "Content-Length": 9,
            "Etag": "1"
        },
        mock_url
    )

    logging.debug("Add and start download")
    dm = DownloadManager()
    task_id = dm.add_download(mock_url, mock_file_name)
    async_thread_runner.submit(dm.start_download(task_id))

    logging.debug("Wait for dm to emit download running state")
    await wait_for_state(dm, task_id, DownloadState.RUNNING)

    await mock_response.insert_chunk(b"abcdefghi")
    mock_response.end_response()
    await wait_for_state(dm, task_id, DownloadState.COMPLETED)

    verify_file(mock_file_name, "abcdefghi")

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)
