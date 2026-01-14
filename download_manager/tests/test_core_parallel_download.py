import asyncio
import pytest
import logging

from dmanager.core import DownloadManager, DownloadState
from tests.helpers import wait_for_state, verify_file, wait_for_file_to_be_created


@pytest.mark.parametrize(
    "n_workers",
    [1, 4]
)
@pytest.mark.asyncio
async def test_n_worker_parallel_download_coroutine(async_thread_runner, create_parallel_mock_response_and_set_mock_session, test_file_setup_and_cleanup, n_workers):
    logging.info(f"Running with {n_workers=}")
    dm = DownloadManager(maximum_workers_per_task=n_workers)

    mock_url = "https://example.com/file.txt"
    mock_file_name = "test_file.txt"
    test_file_setup_and_cleanup(mock_file_name)

    request_queue = asyncio.Queue()
    data = {
        "25": list(b"abcdeabcdeabcdeabcdeabcde"),
        "50": list(b"ghijkghijkghijkghijkghijk"),
        "75": list(b"mnopqmnopqmnopqmnopqmnopq"),
        "100": list(b"asdfeasdfeasdfeasdfeasdfe")
    }

    mock_response = create_parallel_mock_response_and_set_mock_session(
        206,
        {
            "Content-Length": 100,
            "Accept-Ranges": "bytes"
        },
        mock_url,
        request_queue,
        list(data.keys()),
        data
    )

    for key in data:
        mock_response.set_range_end_n_send(key, 25)
        mock_response.set_range_end_done(key)
    
    task_id = dm.add_download(mock_url, mock_file_name)

    async_thread_runner.submit(dm.start_download(task_id, use_parallel_download=True)) 
    
    await wait_for_state(dm, task_id, DownloadState.ALLOCATING_SPACE)

    for _ in range(n_workers):
        await wait_for_state(dm, task_id, DownloadState.COMPLETED)

    verify_file(
        mock_file_name,
        "".join(bytes(x).decode('ascii') for x in data.values())
    )
    
    await dm.shutdown()


@pytest.mark.asyncio
async def test_parallel_download_pause(async_thread_runner, create_parallel_mock_response_and_set_mock_session, test_file_setup_and_cleanup):
    n_workers = 4
    dm = DownloadManager(maximum_workers_per_task=n_workers, minimum_workers_per_task=n_workers)

    mock_url = "https://example.com/file.txt"
    mock_file_name = "test_file.txt"
    test_file_setup_and_cleanup(mock_file_name)

    request_queue = asyncio.Queue()
    data = {
        "25":  list(b"abcdeabcdeabcdeabcdeabcde"),
        "50":  list(b"ghijkghijkghijkghijkghijk"),
        "75":  list(b"mnopqmnopqmnopqmnopqmnopq"),
        "100": list(b"asdfeasdfeasdfeasdfeasdfe")
    }

    mock_response = create_parallel_mock_response_and_set_mock_session(
        206,
        {
            "Content-Length": 100,
            "Accept-Ranges": "bytes"
        },
        mock_url,
        request_queue,
        list(data.keys()),
        data
    )

    for key in data:
        mock_response.set_range_end_n_send(key, 12)

    task_id = dm.add_download(mock_url, mock_file_name)

    async_thread_runner.submit(dm.start_download(task_id, use_parallel_download=True)) 
    
    await wait_for_state(dm, task_id, DownloadState.ALLOCATING_SPACE)
    await wait_for_state(dm, task_id, DownloadState.RUNNING)

    async_thread_runner.submit(dm.pause_download(task_id)) 

    for _ in range(n_workers):
        await wait_for_state(dm, task_id, DownloadState.PAUSED)
    
    await dm.shutdown()


@pytest.mark.asyncio
async def test_parallel_download_resume(async_thread_runner, create_parallel_mock_response_and_set_mock_session, test_file_setup_and_cleanup):
    n_workers = 4
    dm = DownloadManager(maximum_workers_per_task=n_workers, minimum_workers_per_task=n_workers)

    mock_url = "https://example.com/file.txt"
    mock_file_name = "test_file.txt"
    test_file_setup_and_cleanup(mock_file_name)

    request_queue = asyncio.Queue()
    data = {
        "25":  list(b"abcdeabcdeabcdeabcdeabcde"),
        "50":  list(b"ghijkghijkghijkghijkghijk"),
        "75":  list(b"mnopqmnopqmnopqmnopqmnopq"),
        "100": list(b"asdfeasdfeasdfeasdfeasdfe")
    }

    mock_response = create_parallel_mock_response_and_set_mock_session(
        206,
        {
            "Content-Length": 100,
            "Accept-Ranges": "bytes"
        },
        mock_url,
        request_queue,
        list(data.keys()),
        data
    )

    for key in data:
        mock_response.set_range_end_n_send(key, 12)
    
    task_id = dm.add_download(mock_url, mock_file_name)

    async_thread_runner.submit(dm.start_download(task_id, use_parallel_download=True)) 
    
    await wait_for_state(dm, task_id, DownloadState.ALLOCATING_SPACE)
    await wait_for_state(dm, task_id, DownloadState.RUNNING)

    async_thread_runner.submit(dm.pause_download(task_id)) 

    for _ in range(n_workers):
        await wait_for_state(dm, task_id, DownloadState.PAUSED)

    
    async_thread_runner.submit(dm.start_download(task_id, use_parallel_download=True))

    for key in data:
        mock_response.set_range_end_n_send(key, 25)
        mock_response.set_range_end_done(key)

    for _ in range(n_workers):
        await wait_for_state(dm, task_id, DownloadState.COMPLETED)
    
    await dm.shutdown()



# TODO more parallel tests
# Multiple different downloads at the same time
