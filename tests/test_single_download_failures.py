import inspect
import pytest
import logging
import os
import asyncio

from dmanager.core import DownloadManager, DownloadState
from tests.helpers import wait_for_state, verify_file, wait_for_file_to_be_created

@pytest.mark.asyncio
async def test_resume_in_error_state(async_thread_runner, test_file_setup_and_cleanup, create_mock_response_and_set_mock_session):
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

    dm = DownloadManager(continue_on_error=False)
    task_id = dm.add_download(mock_url, mock_file_name)
    async_thread_runner.submit(dm.start_download(task_id))

    await mock_response.insert_chunk("invalid chunk because a bytes-like object is required :)")
    
    await wait_for_state(dm, task_id, DownloadState.RUNNING)
    await wait_for_state(dm, task_id, DownloadState.ERROR)

    logging.debug("Download is now in error state, now running start_download")

    future = async_thread_runner.submit(dm.start_download(task_id))
    assert future.result(timeout=15)

    await wait_for_state(dm, task_id, DownloadState.RUNNING)

    await mock_response.insert_chunk(b"This is a valid chunk!")
    mock_response.end_response()

    await wait_for_state(dm, task_id, DownloadState.COMPLETED)

    verify_file(mock_file_name, "This is a valid chunk!")

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)


@pytest.mark.asyncio
async def test_start_in_error_state(async_thread_runner, test_file_setup_and_cleanup, create_mock_response_and_set_mock_session):
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

    dm = DownloadManager(continue_on_error=False)
    task_id = dm.add_download(mock_url, mock_file_name)
    async_thread_runner.submit(dm.start_download(task_id))

    await mock_response.insert_chunk(chunks[0])
    
    await wait_for_state(dm, task_id, DownloadState.RUNNING)
    await wait_for_state(dm, task_id, DownloadState.ERROR)

    logging.debug("Download is now in error state, now running start_download")
    future = async_thread_runner.submit(dm.start_download(task_id))

    assert future.result(timeout=15)
    await wait_for_state(dm, task_id, DownloadState.RUNNING)

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)



@pytest.mark.asyncio
async def test_delete_from_error_state(async_thread_runner, test_file_setup_and_cleanup, create_mock_response_and_set_mock_session):
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

    logging.debug("Download is now in error state, now run delete_download")
    await dm.delete_download(task_id, remove_file=False)

    await wait_for_state(dm, task_id, DownloadState.DELETED)

    assert task_id not in dm.get_downloads()
    assert task_id not in dm._tasks
    assert os.path.exists(mock_file_name)

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)


@pytest.mark.asyncio
async def test_single_download_continue_on_error(async_thread_runner, test_file_setup_and_cleanup, create_mock_response_and_set_mock_session):
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

    dm = DownloadManager(continue_on_error=True)
    task_id = dm.add_download(mock_url, mock_file_name)
    async_thread_runner.submit(dm.start_download(task_id))

    await mock_response.insert_chunk(chunks[0])
    
    await wait_for_state(dm, task_id, DownloadState.RUNNING)

    mock_response.set_exception(Exception("Mock Exception: Fake bad news."))

    await wait_for_state(dm, task_id, DownloadState.ERROR)

    mock_response.set_exception(None)

    await mock_response.insert_chunk(chunks[1])
    await mock_response.insert_chunk(chunks[2])

    mock_response.end_response()

    await wait_for_state(dm, task_id, DownloadState.COMPLETED)
    verify_file(mock_file_name, "abcdefghi")

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)


@pytest.mark.asyncio
async def test_single_download_do_not_continue_on_error(async_thread_runner, test_file_setup_and_cleanup, create_mock_response_and_set_mock_session):
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

    dm = DownloadManager(continue_on_error=False)
    task_id = dm.add_download(mock_url, mock_file_name)
    async_thread_runner.submit(dm.start_download(task_id))

    await mock_response.insert_chunk(chunks[0])
    
    await wait_for_state(dm, task_id, DownloadState.RUNNING)

    exception = Exception("Mock Exception: Fake bad news.")
    mock_response.set_exception(exception)

    await wait_for_state(dm, task_id, DownloadState.ERROR)
    
    assert dm.get_downloads()[task_id].state == DownloadState.ERROR

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)




@pytest.mark.asyncio
async def test_single_download_stop_on_5_errors(async_thread_runner, test_file_setup_and_cleanup, create_mock_response_and_set_mock_session):
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

    dm = DownloadManager(continue_on_error=True, stop_continue_on_n_errors=5)
    task_id = dm.add_download(mock_url, mock_file_name)
    async_thread_runner.submit(dm.start_download(task_id))

    await mock_response.insert_chunk(chunks[0])
    
    await wait_for_state(dm, task_id, DownloadState.RUNNING)

    exception = Exception("Mock Exception: Fake bad news.")
    mock_response.set_exception(exception)

    n = 5
    for i in range(n):
        logging.debug(f"Waiting for error event {i}/{n}.")
        await wait_for_state(dm, task_id, DownloadState.ERROR)

    if task_id in dm._tasks:
        await asyncio.sleep(1)

    assert dm.get_downloads()[task_id].state == DownloadState.ERROR

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)

