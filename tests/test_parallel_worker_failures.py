import asyncio
import pytest
import logging
import aiohttp

from dmanager.core import DownloadManager, DownloadState
from tests.helpers import wait_for_state, verify_file, wait_for_multiple_states


@pytest.mark.asyncio
async def test_parallel_download_worker_error(async_thread_runner, create_parallel_mock_response_and_set_mock_session, test_file_setup_and_cleanup):
    """Test that other workers continue when one fails."""
    n_workers = 4

    segment_size = 1024
    dm = DownloadManager(maximum_workers_per_task=n_workers, parallel_download_segment_size=segment_size)

    mock_url = "https://example.com/file.txt"
    mock_file_name = "test_file.txt"
    test_file_setup_and_cleanup(mock_file_name)

    data = {
        str(segment_size - 1):  ["INVALIDSTRING"],
        str((segment_size * 2) - 1):  list(b"b" * segment_size),
        str((segment_size * 3) - 1):  list(b"c" * segment_size),
        str((segment_size * 4) - 1): list(b"d" * segment_size)
    }

    mock_response = create_parallel_mock_response_and_set_mock_session(
        206,
        {
            "Content-Length": 4 * segment_size,
            "Accept-Ranges": "bytes"
        },
        mock_url,
        list(data.keys()),
        data
    )

    logging.debug(f"{str(segment_size - 1)=}, {data[str(segment_size - 1)]=}")

    
    for key in data:
        mock_response.set_range_end_n_send(key, 1)
        if key != str(segment_size - 1):
            mock_response.set_range_end_done(key)

    task_id = dm.add_download(mock_url, mock_file_name)
    async_thread_runner.submit(dm.start_download(task_id, use_parallel_download=True))
    
    await wait_for_state(dm, task_id, DownloadState.ALLOCATING_SPACE)
    await wait_for_state(dm, task_id, DownloadState.RUNNING)
    
    await wait_for_state(dm, task_id, DownloadState.ERROR)
    
    download_metadata = dm.get_downloads()[task_id]
    assert download_metadata.state == DownloadState.ERROR
    assert download_metadata.parallel_metadata is not None
    
    # Check that worker states are tracked
    if download_metadata.parallel_metadata.worker_states:
        # At least one worker should be in ERROR state
        worker_states = download_metadata.parallel_metadata.worker_states.values()
        assert DownloadState.ERROR in worker_states

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)


@pytest.mark.asyncio
async def test_parallel_download_fake_timeout(async_thread_runner, create_parallel_mock_response_and_set_mock_session, test_file_setup_and_cleanup):
    
    n_workers = 4
    segment_size = 1024
    dm = DownloadManager(maximum_workers_per_task=n_workers, parallel_download_segment_size=segment_size)

    mock_url = "https://example.com/file.txt"
    mock_file_name = "test_file.txt"
    test_file_setup_and_cleanup(mock_file_name)

    data = {
        str(segment_size - 1):  list(b"a" * segment_size),
        str((segment_size * 2) - 1):  list(b"b" * segment_size),
        str((segment_size * 3) - 1):  list(b"c" * segment_size),
        str((segment_size * 4) - 1): list(b"d" * segment_size)
    }

    mock_response = create_parallel_mock_response_and_set_mock_session(
        206,
        {
            "Content-Length": 4 * segment_size,
            "Accept-Ranges": "bytes"
        },
        mock_url,
        list(data.keys()),
        data
    )

    for key in list(data.keys())[:3]:
        mock_response.set_range_end_n_send(key, segment_size)
        mock_response.set_range_end_done(key)
    
    mock_response.set_range_end_n_send(str((segment_size * 4) - 1), 1)
    
    task_id = dm.add_download(mock_url, mock_file_name)
    async_thread_runner.submit(dm.start_download(task_id, use_parallel_download=True))
    
    await wait_for_state(dm, task_id, DownloadState.ALLOCATING_SPACE)
    await wait_for_state(dm, task_id, DownloadState.RUNNING)
    
    mock_response.set_exception(aiohttp.ServerTimeoutError("Fake Server Timeout Error"))


    await wait_for_state(dm, task_id, DownloadState.ERROR)
    
    download_metadata = dm.get_downloads()[task_id]
    assert download_metadata.state == DownloadState.ERROR
    assert download_metadata.parallel_metadata is not None
    
    # Check that worker states are tracked
    if download_metadata.parallel_metadata.worker_states:
        # At least one worker should be in ERROR state
        worker_states = download_metadata.parallel_metadata.worker_states.values()
        assert DownloadState.ERROR in worker_states
    
    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)

@pytest.mark.asyncio
async def test_parallel_worker_continue_on_failure(async_thread_runner, create_parallel_mock_response_and_set_mock_session, test_file_setup_and_cleanup):
    """Test parallel workers still continue after encountering a failure."""

    n_workers = 1
    segment_size = 49
    dm = DownloadManager(maximum_workers_per_task=n_workers, parallel_download_segment_size=segment_size, continue_on_error=True)

    mock_url = "https://example.com/file.txt"
    mock_file_name = "test_file.txt"
    test_file_setup_and_cleanup(mock_file_name)

    data = {
        str(segment_size - 1):  list(b"a" * segment_size),
        str((segment_size * 2) - 1):  list(b"b" * segment_size),
        str((segment_size * 3) - 1):  list(b"c" * segment_size),
        str((segment_size * 4) - 1): list(b"d" * segment_size)
    }

    mock_response = create_parallel_mock_response_and_set_mock_session(
        206,
        {
            "Content-Length": 4 * segment_size,
            "Accept-Ranges": "bytes"
        },
        mock_url,
        list(data.keys()),
        data
    )

    # Send each worker half of their chunks
    for key in list(data.keys()):
        mock_response.set_range_end_n_send(key, segment_size//2)

    task_id = dm.add_download(mock_url, mock_file_name)
    async_thread_runner.submit(dm.start_download(task_id, use_parallel_download=True))

    await wait_for_state(dm, task_id, DownloadState.ALLOCATING_SPACE)
    await wait_for_state(dm, task_id, DownloadState.RUNNING)

    mock_response.set_exception(Exception("Fake: Something bad happened"))

    
    await wait_for_state(dm, task_id, DownloadState.ERROR)
    mock_response.set_exception(None)

    # Send the rest of the file
    for key in data:
        mock_response.set_range_end_n_send(key, segment_size)
        mock_response.set_range_end_done(key)

    await wait_for_state(dm, task_id, DownloadState.RUNNING)
    
    for _ in range(2):
        await wait_for_state(dm, task_id, DownloadState.COMPLETED)

    verify_file(
        mock_file_name,
        "".join(bytes(x).decode('ascii') for x in data.values())
    )

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)


@pytest.mark.asyncio
async def test_parallel_worker_stop_on_5_errors(async_thread_runner, create_parallel_mock_response_and_set_mock_session, test_file_setup_and_cleanup):
    """Test parallel workers still continue after encountering a failure."""

    n_workers = 1
    segment_size = 49
    dm = DownloadManager(maximum_workers_per_task=n_workers, parallel_download_segment_size=segment_size, continue_on_error=True, stop_continue_on_n_errors=5)

    mock_url = "https://example.com/file.txt"
    mock_file_name = "test_file.txt"
    # test_file_setup_and_cleanup(mock_file_name)

    data = {
        str(segment_size - 1):  list(b"a" * segment_size),
        str((segment_size * 2) - 1):  list(b"b" * segment_size),
        str((segment_size * 3) - 1):  list(b"c" * segment_size),
        str((segment_size * 4) - 1): list(b"d" * segment_size)
    }

    mock_response = create_parallel_mock_response_and_set_mock_session(
        206,
        {
            "Content-Length": 4 * segment_size,
            "Accept-Ranges": "bytes"
        },
        mock_url,
        list(data.keys()),
        data
    )

    exception = Exception("Fake: Something bad happened")
    mock_response.set_exception(exception)

    task_id = dm.add_download(mock_url, mock_file_name)
    async_thread_runner.submit(dm.start_download(task_id, use_parallel_download=True))

    await wait_for_state(dm, task_id, DownloadState.ALLOCATING_SPACE)
    
    for _ in range(5):
        await wait_for_state(dm, task_id, DownloadState.ERROR)

    if task_id in dm._task_pools:
        task = dm._task_pools[task_id][0]
        try:
            await asyncio.wait_for(task, timeout=10)
        except Exception as err:
            assert err == exception
    
    await wait_for_state(dm, task_id, DownloadState.PAUSED)

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)

@pytest.mark.parametrize(
    "continue_on_error",
    [True, False]
)
@pytest.mark.asyncio
async def test_parallel_worker_pause_after_failure(async_thread_runner, create_parallel_mock_response_and_set_mock_session, test_file_setup_and_cleanup, continue_on_error):
    """Test that pause works correctly even after worker failure"""
    n_workers = 4
    segment_size = 1024
    dm = DownloadManager(
        maximum_workers_per_task=n_workers, 
        parallel_download_segment_size=segment_size,
        continue_on_error=continue_on_error
    )

    mock_url = "https://example.com/file.txt"
    mock_file_name = "test_file.txt"
    test_file_setup_and_cleanup(mock_file_name)

    data = {
        str(segment_size - 1):  list(b"a" * segment_size),
        str((segment_size * 2) - 1):  list(b"b" * segment_size),
        str((segment_size * 3) - 1):  list(b"c" * segment_size),
        str((segment_size * 4) - 1): list(b"d" * segment_size)
    }

    mock_response = create_parallel_mock_response_and_set_mock_session(
        206,
        {
            "Content-Length": 4 * segment_size,
            "Accept-Ranges": "bytes"
        },
        mock_url,
        list(data.keys()),
        data
    )
    
    exception = Exception("Fake: Something bad happened")
    mock_response.set_exception(exception)

    task_id = dm.add_download(mock_url, mock_file_name)
    async_thread_runner.submit(dm.start_download(task_id, use_parallel_download=True))
    
    await wait_for_state(dm, task_id, DownloadState.ALLOCATING_SPACE)

    await wait_for_state(dm, task_id, DownloadState.ERROR)
    mock_response.set_exception(None)


    if continue_on_error:
        async_thread_runner.submit(dm.pause_download(task_id))
        for _ in range(n_workers + 1):
            await wait_for_state(dm, task_id, DownloadState.PAUSED)
    else:
        await wait_for_state(dm, task_id, DownloadState.PAUSED)
    
    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)

@pytest.mark.parametrize(
    "continue_on_error",
    [True, False]
)
@pytest.mark.asyncio
async def test_parallel_worker_delete_after_failure(async_thread_runner, create_parallel_mock_response_and_set_mock_session, test_file_setup_and_cleanup, continue_on_error):
    """Test that delete works correctly even after worker failure"""

    n_workers = 4
    segment_size = 1024
    dm = DownloadManager(
        maximum_workers_per_task=n_workers, 
        parallel_download_segment_size=segment_size,
        continue_on_error=continue_on_error
    )

    mock_url = "https://example.com/file.txt"
    mock_file_name = "test_file.txt"
    test_file_setup_and_cleanup(mock_file_name)

    data = {
        str(segment_size - 1):  list(b"a" * segment_size),
        str((segment_size * 2) - 1):  list(b"b" * segment_size),
        str((segment_size * 3) - 1):  list(b"c" * segment_size),
        str((segment_size * 4) - 1): list(b"d" * segment_size)
    }

    mock_response = create_parallel_mock_response_and_set_mock_session(
        206,
        {
            "Content-Length": 4 * segment_size,
            "Accept-Ranges": "bytes"
        },
        mock_url,
        list(data.keys()),
        data
    )
    
    exception = Exception("Fake: Something bad happened")
    mock_response.set_exception(exception)

    task_id = dm.add_download(mock_url, mock_file_name)
    async_thread_runner.submit(dm.start_download(task_id, use_parallel_download=True))
    
    await wait_for_state(dm, task_id, DownloadState.ALLOCATING_SPACE)

    await wait_for_state(dm, task_id, DownloadState.ERROR)
    
    async_thread_runner.submit(dm.delete_download(task_id))
    
    await wait_for_state(dm, task_id, DownloadState.DELETED)
    
    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)


@pytest.mark.parametrize(
    "continue_on_error",
    [True, False]
)
@pytest.mark.asyncio
async def test_parallel_worker_start_after_failure(async_thread_runner, create_parallel_mock_response_and_set_mock_session, test_file_setup_and_cleanup, continue_on_error):
    """Test that start works correctly after worker failure"""

    n_workers = 4
    segment_size = 1024
    dm = DownloadManager(
        maximum_workers_per_task=n_workers, 
        parallel_download_segment_size=segment_size,
        continue_on_error=continue_on_error
    )

    mock_url = "https://example.com/file.txt"
    mock_file_name = "test_file.txt"
    test_file_setup_and_cleanup(mock_file_name)

    data = {
        str(segment_size - 1):  list(b"a" * segment_size),
        str((segment_size * 2) - 1):  list(b"b" * segment_size),
        str((segment_size * 3) - 1):  list(b"c" * segment_size),
        str((segment_size * 4) - 1): list(b"d" * segment_size)
    }

    mock_response = create_parallel_mock_response_and_set_mock_session(
        206,
        {
            "Content-Length": 4 * segment_size,
            "Accept-Ranges": "bytes"
        },
        mock_url,
        list(data.keys()),
        data
    )
    
    exception = Exception("Fake: Something bad happened")
    mock_response.set_exception(exception)

    task_id = dm.add_download(mock_url, mock_file_name)
    async_thread_runner.submit(dm.start_download(task_id, use_parallel_download=True))
    
    await wait_for_state(dm, task_id, DownloadState.ALLOCATING_SPACE)

    await wait_for_state(dm, task_id, DownloadState.ERROR)
    
    mock_response.set_exception(None)
    
    for key in data:
        mock_response.set_range_end_n_send(key, segment_size)
        mock_response.set_range_end_done(key)
    async_thread_runner.submit(dm.start_download(task_id))

    if continue_on_error:
        pass
    else:
        await wait_for_state(dm, task_id, DownloadState.RUNNING)
    
    await wait_for_state(dm, task_id, DownloadState.COMPLETED)
    verify_file(
        mock_file_name,
        "".join(bytes(x).decode('ascii') for x in data.values())
    )

    
    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)
