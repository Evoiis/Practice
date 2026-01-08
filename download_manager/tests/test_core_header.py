import asyncio
import pytest
import os
import logging


from dmanager.core import DownloadManager, DownloadState
from tests.helpers import wait_for_state, verify_file

@pytest.mark.asyncio
async def test_header_etag_change(async_thread_runner, test_file_setup_and_cleanup, create_mock_response_and_set_mock_session):
    chunks = [b"abc", b"def", b"ghi"]
    mock_url = "https://example.com/file.bin"
    mock_file_name = "test_file.bin"
    test_file_setup_and_cleanup(mock_file_name)

    mock_response = create_mock_response_and_set_mock_session(
        206,
        {
            "Content-Length": 9,
            "Accept-Ranges": "bytes",
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

    await mock_response.insert_chunk(chunks[0])

    logging.debug("Pause download")
    async_thread_runner.submit(dm.pause_download(task_id))
    
    logging.debug("Wait for dm to emit download pause state")
    await wait_for_state(dm, task_id, DownloadState.PAUSED)

    mock_response = create_mock_response_and_set_mock_session(
        206,
        {
            "Content-Length": 9,
            "Accept-Ranges": "bytes",
            "Etag": "2"
        },
        mock_url
    )

    async_thread_runner.submit(dm.resume_download(task_id))

    await mock_response.insert_chunk(chunks[1])
    await mock_response.insert_chunk(chunks[2])
    mock_response.end_response()

    await wait_for_state(dm, task_id, DownloadState.COMPLETED)

    verify_file(mock_file_name, "defghi")

    dm.shutdown()


@pytest.mark.asyncio
async def test_resume_on_header_content_length_change(async_thread_runner, test_file_setup_and_cleanup, create_mock_response_and_set_mock_session):
    chunks = [b"abc", b"def", b"ghi"]
    mock_url = "https://example.com/file.bin"
    mock_file_name = "test_file.bin"
    test_file_setup_and_cleanup(mock_file_name)

    mock_response = create_mock_response_and_set_mock_session(
        206,
        {
            "Content-Length": 9,
            "Accept-Ranges": "bytes",
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

    await mock_response.insert_chunk(chunks[0])

    logging.debug("Pause download")
    async_thread_runner.submit(dm.pause_download(task_id))
    
    logging.debug("Wait for dm to emit download pause state")
    await wait_for_state(dm, task_id, DownloadState.PAUSED)

    mock_response = create_mock_response_and_set_mock_session(
        206,
        {
            "Content-Length": 3,
            "Accept-Ranges": "bytes",
            "Etag": "1"
        },
        mock_url
    )

    async_thread_runner.submit(dm.resume_download(task_id))

    await mock_response.insert_chunk(chunks[1])
    await mock_response.insert_chunk(chunks[2])
    mock_response.end_response()

    await wait_for_state(dm, task_id, DownloadState.COMPLETED)

    verify_file(mock_file_name, "defghi")

    dm.shutdown()


@pytest.mark.asyncio
async def test_no_http_range_support(async_thread_runner, test_file_setup_and_cleanup, create_mock_response_and_set_mock_session):
    mock_url = "https://example.com/file.bin"
    mock_file_name = "test_file.bin"
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

    dm.shutdown()

@pytest.mark.asyncio
async def test_resume_on_download_with_no_http_range_support(async_thread_runner, test_file_setup_and_cleanup, create_mock_response_and_set_mock_session):
    chunks = [b"abcdefghi"]
    mock_url = "https://example.com/file.bin"
    mock_file_name = "test_file.bin"
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

    await mock_response.insert_chunk(chunks[0])

    logging.debug("Pause download")
    async_thread_runner.submit(dm.pause_download(task_id))
    
    logging.debug("Wait for dm to emit download pause state")
    await wait_for_state(dm, task_id, DownloadState.PAUSED)
    
    async_thread_runner.submit(dm.resume_download(task_id))

    await mock_response.insert_chunk(chunks[0])
    mock_response.end_response()

    await wait_for_state(dm, task_id, DownloadState.COMPLETED)

    verify_file(mock_file_name, "abcdefghi")

    dm.shutdown()
