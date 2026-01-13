import asyncio
import pytest
import os
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
        "bytes=0-25": b"abcdeabcdeabcdeabcdeabcde",
        "bytes=25-50": b"ghijkghijkghijkghijkghijk",
        "bytes=50-75": b"mnopqmnopqmnopqmnopqmnopq",
        "bytes=75-100": b"asdfeasdfeasdfeasdfeasdfe"
    }

    create_parallel_mock_response_and_set_mock_session(
        206,
        {
            "Content-Length": 100,
            "Accept-Ranges": "bytes"
        },
        mock_url,
        request_queue,
        data
    )
    
    task_id = dm.add_download(mock_url, mock_file_name)

    async_thread_runner.submit(dm.start_download(task_id, use_parallel_download=True)) 
    
    await wait_for_state(dm, task_id, DownloadState.ALLOCATING_SPACE)

    for _ in range(n_workers):
        await wait_for_state(dm, task_id, DownloadState.COMPLETED)

    verify_file(
        mock_file_name,
        "".join(x.decode('ascii') for x in data.values())
    )
    
    await dm.shutdown()


# TODO more parallel tests
# Delete running
# Resume parallel download
# Multiple different downloads at the same time
