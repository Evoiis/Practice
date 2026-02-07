import asyncio
import pytest
import os
import logging
import inspect

from dmanager.core import DownloadManager, DownloadState
from tests.helpers import wait_for_state, verify_file, wait_for_file_to_be_created

@pytest.mark.asyncio
async def test_add_and_start_download(async_thread_runner, create_mock_response_and_set_mock_session, test_file_setup_and_cleanup):
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
    
    download_metadata = dm.get_downloads()[task_id]

    logging.debug(download_metadata)
    assert download_metadata.url == mock_url
    assert download_metadata.output_file == mock_file_name
    assert download_metadata.file_size_bytes == 9
    assert download_metadata.downloaded_bytes == 9

    verify_file(mock_file_name, "abcdefghi")

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)


@pytest.mark.asyncio
async def test_pause_download(async_thread_runner, create_mock_response_and_set_mock_session, test_file_setup_and_cleanup):
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

    # Task should be paused so we will give a chunk and wait some time to make sure pause stops file write
    await mock_response.insert_chunk(chunks[1])
    await asyncio.sleep(5)
    
    # Check _tasks is cleaned up and download state
    assert task_id not in dm._tasks
    assert dm.get_downloads()[task_id].state == DownloadState.PAUSED

    logging.debug("Verifying only the first chunk was written to file")
    verify_file(mock_file_name, "abc")

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)


@pytest.mark.asyncio
async def test_resume_download(async_thread_runner, create_mock_response_and_set_mock_session, test_file_setup_and_cleanup):
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

    logging.debug("Wait for dm to write the chunk")    
    wait_for_file_to_be_created(mock_file_name)

    logging.debug("Pause download")
    async_thread_runner.submit(dm.pause_download(task_id))

    logging.debug("Wait for dm to emit download pause state")
    await wait_for_state(dm, task_id, DownloadState.PAUSED)

    # Task should be paused so we will give a chunk and wait some time to make sure pause stops file write
    await mock_response.insert_chunk(chunks[1])
    await asyncio.sleep(5)
    await mock_response.empty_queue()

    logging.debug("Resume Download")
    async_thread_runner.submit(dm.start_download(task_id))
    
    await wait_for_state(dm, task_id, DownloadState.RUNNING)

    await mock_response.insert_chunk(chunks[2])
    mock_response.end_response()

    await wait_for_state(dm, task_id, DownloadState.COMPLETED)

    # Verify
    verify_file(mock_file_name, "abcghi")

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)


@pytest.mark.asyncio
async def test_delete_from_pending_state():
    mock_url = "https://example.com/file.bin"
    mock_file_name = f"{inspect.currentframe().f_code.co_name}.txt"
    dm = DownloadManager()
    task_id = dm.add_download(mock_url, mock_file_name)

    assert task_id in dm.get_downloads()

    await dm.delete_download(task_id, remove_file=False)

    assert task_id not in dm.get_downloads()    

@pytest.mark.asyncio
async def test_delete_from_running_state(async_thread_runner, test_file_setup_and_cleanup, create_mock_response_and_set_mock_session):
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

    dm = DownloadManager()
    task_id = dm.add_download(mock_url, mock_file_name)
    async_thread_runner.submit(dm.start_download(task_id))

    await mock_response.insert_chunk(chunks[0])
    await wait_for_state(dm, task_id, DownloadState.RUNNING)
    wait_for_file_to_be_created(mock_file_name)

    logging.debug("Download is running, now run delete_download")
    future = async_thread_runner.submit(dm.delete_download(task_id, remove_file=False))
    future.result(timeout=15)

    await wait_for_state(dm, task_id, DownloadState.DELETED)

    assert task_id not in dm.get_downloads()
    assert task_id not in dm._tasks
    assert os.path.exists(mock_file_name)

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)

@pytest.mark.asyncio
async def test_delete_from_paused_state(async_thread_runner, test_file_setup_and_cleanup, create_mock_response_and_set_mock_session):
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

    dm = DownloadManager()
    task_id = dm.add_download(mock_url, mock_file_name)
    async_thread_runner.submit(dm.start_download(task_id))

    await mock_response.insert_chunk(chunks[0])
    await wait_for_state(dm, task_id, DownloadState.RUNNING)
    wait_for_file_to_be_created(mock_file_name)

    logging.debug("Pause download")
    async_thread_runner.submit(dm.pause_download(task_id))

    await wait_for_state(dm, task_id, DownloadState.PAUSED)

    logging.debug("Download is paused, now run delete_download")
    await dm.delete_download(task_id, remove_file=False)

    await wait_for_state(dm, task_id, DownloadState.DELETED)

    assert task_id not in dm.get_downloads()
    assert task_id not in dm._tasks
    assert os.path.exists(mock_file_name)

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)
    
@pytest.mark.asyncio
async def test_delete_from_completed_state(async_thread_runner, test_file_setup_and_cleanup, create_mock_response_and_set_mock_session):
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

    dm = DownloadManager()
    task_id = dm.add_download(mock_url, mock_file_name)
    download_future = async_thread_runner.submit(dm.start_download(task_id))

    await mock_response.insert_chunk(chunks[0])
    mock_response.end_response()
    
    await wait_for_state(dm, task_id, DownloadState.RUNNING)
    wait_for_file_to_be_created(mock_file_name)

    await wait_for_state(dm, task_id, DownloadState.COMPLETED)
    download_future.result(timeout=15)

    logging.debug("Download is completed, now run delete_download")
    await dm.delete_download(task_id, remove_file=False)

    await wait_for_state(dm, task_id, DownloadState.DELETED)

    assert task_id not in dm.get_downloads()
    assert task_id not in dm._tasks
    assert os.path.exists(mock_file_name)

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)


@pytest.mark.asyncio
async def test_two_mib_download(async_thread_runner, test_file_setup_and_cleanup, create_mock_response_and_set_mock_session):
    two_mib = b"a" * (2 * 1024 * 1024)
    mock_url = "https://example.com/file.bin"
    mock_file_name = f"{inspect.currentframe().f_code.co_name}.txt"
    test_file_setup_and_cleanup(mock_file_name)

    mock_response = create_mock_response_and_set_mock_session(
        206,
        {
            "Content-Length": len(two_mib),
            "Accept-Ranges": "bytes"
        },
        mock_url
    )

    dm = DownloadManager()
    task_id = dm.add_download(mock_url, mock_file_name)
    async_thread_runner.submit(dm.start_download(task_id))

    await mock_response.insert_chunk(two_mib)
    mock_response.end_response()
    
    await wait_for_state(dm, task_id, DownloadState.RUNNING)
    wait_for_file_to_be_created(mock_file_name)
    await wait_for_state(dm, task_id, DownloadState.COMPLETED)

    verify_file(mock_file_name, "a" * (2 * 1024 * 1024))

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)


@pytest.mark.asyncio
async def test_two_mib_download_no_http_ranges(async_thread_runner, test_file_setup_and_cleanup, create_mock_response_and_set_mock_session):
    two_mib = b"a" * (2 * 1024 * 1024)
    mock_url = "https://example.com/file.bin"
    mock_file_name = f"{inspect.currentframe().f_code.co_name}.txt"
    test_file_setup_and_cleanup(mock_file_name)

    mock_response = create_mock_response_and_set_mock_session(
        200,
        {
            "Content-Length": len(two_mib),
        },
        mock_url
    )

    dm = DownloadManager()
    task_id = dm.add_download(mock_url, mock_file_name)
    async_thread_runner.submit(dm.start_download(task_id))

    await mock_response.insert_chunk(two_mib)
    mock_response.end_response()
    
    await wait_for_state(dm, task_id, DownloadState.RUNNING)
    wait_for_file_to_be_created(mock_file_name)
    await wait_for_state(dm, task_id, DownloadState.COMPLETED)

    verify_file(mock_file_name, "a" * (2 * 1024 * 1024))

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)


@pytest.mark.asyncio
async def test_delete_and_redownload_same_file(
    async_thread_runner,
    test_file_setup_and_cleanup,
    create_mock_response_and_set_mock_session,
):
    # --- Setup ---
    chunks = [b"abc", b"def", b"ghi"]
    expected_content = b"".join(chunks)

    mock_url = "https://example.com/file.bin"
    mock_file_name = f"{inspect.currentframe().f_code.co_name}.txt"
    test_file_setup_and_cleanup(mock_file_name)

    # First mock response
    mock_response = create_mock_response_and_set_mock_session(
        206,
        {
            "Content-Length": str(len(expected_content)),
            "Accept-Ranges": "bytes",
        },
        mock_url,
    )

    dm = DownloadManager()

    # --- First download ---
    task_id = dm.add_download(mock_url, mock_file_name)
    async_thread_runner.submit(dm.start_download(task_id))

    for chunk in chunks:
        await mock_response.insert_chunk(chunk)
    mock_response.end_response()

    await wait_for_state(dm, task_id, DownloadState.RUNNING)
    await wait_for_state(dm, task_id, DownloadState.COMPLETED)

    
    wait_for_file_to_be_created(mock_file_name)
    verify_file(mock_file_name, expected_content.decode())

    await dm.delete_download(task_id, remove_file=True)

    await wait_for_state(dm, task_id, DownloadState.DELETED)
    assert task_id not in dm._tasks
    assert task_id not in dm._downloads
    assert not os.path.exists(mock_file_name)

    # --- Re-add and re-download ---
    task_id_2 = dm.add_download(mock_url, mock_file_name)
    async_thread_runner.submit(dm.start_download(task_id_2))

    for chunk in chunks:
        await mock_response.insert_chunk(chunk)
    mock_response.end_response()

    await wait_for_state(dm, task_id_2, DownloadState.RUNNING)
    await wait_for_state(dm, task_id_2, DownloadState.COMPLETED)

    # --- Verify again ---
    wait_for_file_to_be_created(mock_file_name)
    verify_file(mock_file_name, expected_content.decode())

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)
