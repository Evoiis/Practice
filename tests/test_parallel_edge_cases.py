import pytest
import inspect

from dmanager.core import DownloadManager, DownloadState
from tests.helpers import wait_for_state, verify_file, wait_for_file_to_be_created, wait_for_multiple_states

@pytest.mark.asyncio
async def test_zero_workers_input():
    """Test behavior with invalid worker count"""
    try:
        DownloadManager(maximum_workers_per_task=0)
        assert False
    except Exception as err:
        str(err) == "Download Manager parameter, maximum_workers_per_task, must be an integer greater than zero"

@pytest.mark.asyncio
async def test_parallel_download_single_byte_file(async_thread_runner, create_parallel_mock_response_and_set_mock_session, test_file_setup_and_cleanup):
    """Test parallel download with file smaller than segment size"""
    n_workers = 4
    segment_size = 1024
    dm = DownloadManager(maximum_workers_per_task=n_workers, parallel_download_segment_size=segment_size)

    mock_url = "https://example.com/file.txt"
    mock_file_name = f"{inspect.currentframe().f_code.co_name}.txt"
    test_file_setup_and_cleanup(mock_file_name)

    # Single byte file
    data = {
        "1": [ord("X")]
    }

    mock_response = create_parallel_mock_response_and_set_mock_session(
        206,
        {
            "Content-Length": 1,
            "Accept-Ranges": "bytes"
        },
        mock_url,
        list(data.keys()),
        data
    )

    mock_response.set_range_end_n_send("1", 1)
    mock_response.set_range_end_done("1")
    
    task_id = dm.add_download(mock_url, mock_file_name)
    async_thread_runner.submit(dm.start_download(task_id, use_parallel_download=True))
    
    await wait_for_state(dm, task_id, DownloadState.ALLOCATING_SPACE)
    
    await wait_for_multiple_states(
        dm,
        {(task_id, DownloadState.COMPLETED): n_workers}
    )
    
    verify_file(mock_file_name, "X")

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)


@pytest.mark.asyncio
async def test_parallel_download_exact_segment_boundary(async_thread_runner, create_parallel_mock_response_and_set_mock_session, test_file_setup_and_cleanup):
    """Test file size that's exactly N * segment_size"""
    n_workers = 2
    segment_size = 1024
    dm = DownloadManager(maximum_workers_per_task=n_workers, parallel_download_segment_size=segment_size)

    mock_url = "https://example.com/file.txt"
    mock_file_name = f"{inspect.currentframe().f_code.co_name}.txt"
    test_file_setup_and_cleanup(mock_file_name)

    # Exactly 2 segments
    data = {
        str(segment_size - 1): list(b"a" * segment_size),
        str((segment_size * 2) - 1): list(b"b" * segment_size),
    }

    mock_response = create_parallel_mock_response_and_set_mock_session(
        206,
        {
            "Content-Length": 2 * segment_size,
            "Accept-Ranges": "bytes"
        },
        mock_url,
        list(data.keys()),
        data
    )

    for key in data:
        mock_response.set_range_end_n_send(key, segment_size)
        mock_response.set_range_end_done(key)
    
    task_id = dm.add_download(mock_url, mock_file_name)
    async_thread_runner.submit(dm.start_download(task_id, use_parallel_download=True))
    
    await wait_for_state(dm, task_id, DownloadState.ALLOCATING_SPACE)
    
    for _ in range(n_workers):
        await wait_for_state(dm, task_id, DownloadState.COMPLETED, timeout_sec=60)
    
    verify_file(
        mock_file_name,
        "".join(bytes(x).decode('ascii') for x in data.values())
    )

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)


@pytest.mark.asyncio
async def test_parallel_download_pause_during_different_worker_states(async_thread_runner, create_parallel_mock_response_and_set_mock_session, test_file_setup_and_cleanup):
    """Pause when workers are in mixed states (some running, some pending)"""
    n_workers = 4
    segment_size = 1024
    dm = DownloadManager(maximum_workers_per_task=n_workers, parallel_download_segment_size=segment_size)

    mock_url = "https://example.com/file.txt"
    mock_file_name = f"{inspect.currentframe().f_code.co_name}.txt"
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

    # Only send data for first 2 segments, leaving 2 workers potentially pending
    for i, key in enumerate(list(data.keys())[:2]):
        mock_response.set_range_end_n_send(key, segment_size // 2)
    
    task_id = dm.add_download(mock_url, mock_file_name)
    async_thread_runner.submit(dm.start_download(task_id, use_parallel_download=True))
    
    await wait_for_state(dm, task_id, DownloadState.ALLOCATING_SPACE)
    await wait_for_state(dm, task_id, DownloadState.RUNNING)
    
    # Pause while workers are in different states
    async_thread_runner.submit(dm.pause_download(task_id))
    
    # All workers should eventually pause
    for _ in range(n_workers):
        await wait_for_state(dm, task_id, DownloadState.PAUSED)

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)


@pytest.mark.asyncio
async def test_parallel_resume_with_partial_segments(async_thread_runner, create_parallel_mock_response_and_set_mock_session, test_file_setup_and_cleanup):
    """Test leftover_segments queue properly handles partial work"""
    n_workers = 4
    segment_size = 1024
    dm = DownloadManager(maximum_workers_per_task=n_workers, parallel_download_segment_size=segment_size)

    mock_url = "https://example.com/file.txt"
    mock_file_name = f"{inspect.currentframe().f_code.co_name}.txt"
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

    # Send partial data for each segment
    for key in data:
        mock_response.set_range_end_n_send(key, segment_size // 3)
    
    task_id = dm.add_download(mock_url, mock_file_name)
    async_thread_runner.submit(dm.start_download(task_id, use_parallel_download=True))
    
    await wait_for_state(dm, task_id, DownloadState.ALLOCATING_SPACE)
    await wait_for_state(dm, task_id, DownloadState.RUNNING)
    
    # Pause - should save partial segments to leftover queue
    async_thread_runner.submit(dm.pause_download(task_id))
    
    for _ in range(n_workers):
        await wait_for_state(dm, task_id, DownloadState.PAUSED)
    
    # Resume and complete download
    async_thread_runner.submit(dm.start_download(task_id, use_parallel_download=True))
    
    for key in data:
        mock_response.set_range_end_n_send(key, segment_size)
        mock_response.set_range_end_done(key)
    
    for _ in range(n_workers):
        await wait_for_state(dm, task_id, DownloadState.COMPLETED, timeout_sec=60)
    
    verify_file(
        mock_file_name,
        "".join(bytes(x).decode('ascii') for x in data.values())
    )

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)


@pytest.mark.asyncio
async def test_parallel_download_more_workers_than_segments(async_thread_runner, create_parallel_mock_response_and_set_mock_session, test_file_setup_and_cleanup):
    """Test more workers than file can be divided into"""
    n_workers = 10  # More workers than segments
    segment_size = 1024
    dm = DownloadManager(maximum_workers_per_task=n_workers, parallel_download_segment_size=segment_size)

    mock_url = "https://example.com/file.txt"
    mock_file_name = f"{inspect.currentframe().f_code.co_name}.txt"
    test_file_setup_and_cleanup(mock_file_name)

    # Only 2 segments
    data = {
        str(segment_size - 1): list(b"a" * segment_size),
        str((segment_size * 2) - 1): list(b"b" * segment_size),
    }

    mock_response = create_parallel_mock_response_and_set_mock_session(
        206,
        {
            "Content-Length": 2 * segment_size,
            "Accept-Ranges": "bytes"
        },
        mock_url,
        list(data.keys()),
        data
    )

    for key in data:
        mock_response.set_range_end_n_send(key, segment_size)
        mock_response.set_range_end_done(key)
    
    task_id = dm.add_download(mock_url, mock_file_name)
    async_thread_runner.submit(dm.start_download(task_id, use_parallel_download=True))
    
    await wait_for_state(dm, task_id, DownloadState.ALLOCATING_SPACE)
    
    for _ in range(n_workers):
        await wait_for_state(dm, task_id, DownloadState.COMPLETED, timeout_sec=60)
    
    verify_file(
        mock_file_name,
        "".join(bytes(x).decode('ascii') for x in data.values())
    )

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)


@pytest.mark.asyncio
async def test_parallel_iterator_exhaustion_race(async_thread_runner, create_parallel_mock_response_and_set_mock_session, test_file_setup_and_cleanup):
    """Test multiple workers hitting empty iterator simultaneously"""
    n_workers = 8
    segment_size = 1024
    dm = DownloadManager(maximum_workers_per_task=n_workers, parallel_download_segment_size=segment_size)

    mock_url = "https://example.com/file.txt"
    mock_file_name = f"{inspect.currentframe().f_code.co_name}.txt"
    test_file_setup_and_cleanup(mock_file_name)

    # Small file so iterator exhausts quickly
    data = {
        str(segment_size - 1): list(b"a" * segment_size),
    }

    mock_response = create_parallel_mock_response_and_set_mock_session(
        206,
        {
            "Content-Length": segment_size,
            "Accept-Ranges": "bytes"
        },
        mock_url,
        list(data.keys()),
        data
    )

    for key in data:
        mock_response.set_range_end_n_send(key, segment_size)
        mock_response.set_range_end_done(key)
    
    task_id = dm.add_download(mock_url, mock_file_name)
    async_thread_runner.submit(dm.start_download(task_id, use_parallel_download=True))
    
    await wait_for_state(dm, task_id, DownloadState.ALLOCATING_SPACE)
    
    # All workers should complete gracefully
    for _ in range(n_workers):
        await wait_for_state(dm, task_id, DownloadState.COMPLETED, timeout_sec=60)
    
    verify_file(mock_file_name, "a" * segment_size)

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)


@pytest.mark.asyncio
async def test_parallel_download_all_workers_complete_simultaneously(async_thread_runner, create_parallel_mock_response_and_set_mock_session, test_file_setup_and_cleanup):
    """Test all workers finishing at exact same time"""
    n_workers = 4
    segment_size = 1024
    dm = DownloadManager(maximum_workers_per_task=n_workers, parallel_download_segment_size=segment_size)

    mock_url = "https://example.com/file.txt"
    mock_file_name = f"{inspect.currentframe().f_code.co_name}.txt"
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

    # Send all data at once so workers complete simultaneously
    for key in data:
        mock_response.set_range_end_n_send(key, segment_size)
        mock_response.set_range_end_done(key)
    
    task_id = dm.add_download(mock_url, mock_file_name)
    async_thread_runner.submit(dm.start_download(task_id, use_parallel_download=True))
    
    await wait_for_state(dm, task_id, DownloadState.ALLOCATING_SPACE)
    
    for _ in range(n_workers):
        await wait_for_state(dm, task_id, DownloadState.COMPLETED, timeout_sec=60)
    
    verify_file(
        mock_file_name,
        "".join(bytes(x).decode('ascii') for x in data.values())
    )

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)
