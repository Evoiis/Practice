import asyncio
import pytest
import logging
import inspect

from dmanager.core import DownloadManager, DownloadState
from tests.helpers import wait_for_state, wait_for_file_to_be_created


@pytest.mark.asyncio
async def test_delete_during_pause_operation(async_thread_runner, create_mock_response_and_set_mock_session, test_file_setup_and_cleanup):
    """Test race condition: delete while pause is in progress"""
    chunks = [b"a" * 1024 for _ in range(10)]
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

    await wait_for_state(dm, task_id, DownloadState.RUNNING)
    
    for chunk in chunks[:3]:
        await mock_response.insert_chunk(chunk)
    
    # Initiate pause (don't wait)
    pause_future = async_thread_runner.submit(dm.pause_download(task_id))
    
    # Immediately try to delete
    await asyncio.sleep(0.1)  # Give pause a tiny head start
    delete_future = async_thread_runner.submit(dm.delete_download(task_id, remove_file=False))
    
    # Both should complete without error
    pause_result = pause_future.result(timeout=15)
    delete_result = delete_future.result(timeout=15)
    
    # Either pause succeeded then delete, or delete succeeded directly
    assert delete_result is True
    
    # Task should be deleted
    assert task_id not in dm.get_downloads()

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)


@pytest.mark.asyncio
async def test_pause_during_delete_operation(async_thread_runner, create_mock_response_and_set_mock_session, test_file_setup_and_cleanup):
    """Test race condition: pause while delete is in progress"""
    chunks = [b"a" * 1024 for _ in range(10)]
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

    await wait_for_state(dm, task_id, DownloadState.RUNNING)
    
    for chunk in chunks[:3]:
        await mock_response.insert_chunk(chunk)
    
    # Initiate delete (don't wait)
    delete_future = async_thread_runner.submit(dm.delete_download(task_id, remove_file=False))
    
    # Immediately try to pause
    await asyncio.sleep(0.1)
    pause_future = async_thread_runner.submit(dm.pause_download(task_id))
    
    # Delete should succeed
    delete_result = delete_future.result(timeout=15)
    assert delete_result is True
    
    # Pause should fail (task no longer exists)
    pause_result = pause_future.result(timeout=15)
    assert pause_result is False
    
    # Task should be deleted
    assert task_id not in dm.get_downloads()

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)


@pytest.mark.asyncio
async def test_multiple_downloads_same_url_different_files(async_thread_runner, create_multiple_parallel_mock_response_and_mock_sessions, test_multiple_file_setup_and_cleanup):
    """Test multiple simultaneous downloads from same URL to different files"""
    mock_url = "https://example.com/file.bin"
    mock_file_1 = "output1.bin"
    mock_file_2 = "output2.bin"
    test_multiple_file_setup_and_cleanup([mock_file_1, mock_file_2])

    segment_size = 100
    
    data = {
        str(segment_size - 1): list(b"a" * segment_size),
    }

    mock_responses = create_multiple_parallel_mock_response_and_mock_sessions({
        mock_url: {
            "status": 206,
            "headers": {"Content-Length": segment_size, "Accept-Ranges": "bytes"},
            "range_ends": list(data.keys()),
            "data": data
        },
    })

    for key in data:
        mock_responses[mock_url].set_range_end_n_send(key, segment_size)
        mock_responses[mock_url].set_range_end_done(key)

    dm = DownloadManager(parallel_download_segment_size=segment_size)
    
    # Add two downloads with same URL but different output files
    task_id_1 = dm.add_download(mock_url, mock_file_1)
    task_id_2 = dm.add_download(mock_url, mock_file_2)
    
    # Verify they have different task IDs
    assert task_id_1 != task_id_2
    
    # Verify output files are different
    download_1 = dm.get_downloads()[task_id_1]
    download_2 = dm.get_downloads()[task_id_2]
    assert download_1.output_file != download_2.output_file
    
    # Start both downloads
    async_thread_runner.submit(dm.start_download(task_id_1, use_parallel_download=True))
    async_thread_runner.submit(dm.start_download(task_id_2, use_parallel_download=True))
    
    # Both should complete
    await wait_for_state(dm, task_id_1, DownloadState.COMPLETED, timeout_sec=60)
    await wait_for_state(dm, task_id_2, DownloadState.COMPLETED, timeout_sec=60)

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)


@pytest.mark.asyncio
async def test_multiple_downloads_different_url_same_file():
    """Test duplicate filename detection for different URLs"""
    dm = DownloadManager()
    
    same_filename = "output.bin"
    
    task_id_1 = dm.add_download("https://example.com/file1.bin", same_filename)
    task_id_2 = dm.add_download("https://example.com/file2.bin", same_filename)
    
    download_1 = dm.get_downloads()[task_id_1]
    download_2 = dm.get_downloads()[task_id_2]
    
    # Second download should have empty filename to avoid collision
    assert download_1.output_file == same_filename
    assert download_2.output_file == ""


@pytest.mark.asyncio
async def test_concurrent_start_operations(async_thread_runner, create_mock_response_and_set_mock_session, test_file_setup_and_cleanup):
    """Test multiple simultaneous start_download calls on same task"""
    chunks = [b"test"]
    mock_url = "https://example.com/file.bin"
    mock_file_name = f"{inspect.currentframe().f_code.co_name}.txt"
    test_file_setup_and_cleanup(mock_file_name)

    mock_response = create_mock_response_and_set_mock_session(
        206,
        {
            "Content-Length": str(len(chunks[0])),
            "Accept-Ranges": "bytes"
        },
        mock_url
    )

    dm = DownloadManager()
    task_id = dm.add_download(mock_url, mock_file_name)
    
    # Try to start same download multiple times concurrently
    future1 = async_thread_runner.submit(dm.start_download(task_id))
    future2 = async_thread_runner.submit(dm.start_download(task_id))
    future3 = async_thread_runner.submit(dm.start_download(task_id))
    
    result1 = future1.result(timeout=15)
    result2 = future2.result(timeout=15)
    result3 = future3.result(timeout=15)
    
    # First should succeed, others should fail
    results = [result1, result2, result3]
    assert results.count(True) == 1  # Only one should succeed
    assert results.count(False) == 2  # Others should fail
    
    await wait_for_state(dm, task_id, DownloadState.RUNNING)

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)


@pytest.mark.asyncio
async def test_concurrent_pause_operations(async_thread_runner, create_mock_response_and_set_mock_session, test_file_setup_and_cleanup):
    """Test multiple simultaneous pause_download calls"""
    chunks = [b"a" * 1024]
    mock_url = "https://example.com/file.bin"
    mock_file_name = f"{inspect.currentframe().f_code.co_name}.txt"
    test_file_setup_and_cleanup(mock_file_name)

    mock_response = create_mock_response_and_set_mock_session(
        206,
        {
            "Content-Length": str(len(chunks[0])),
            "Accept-Ranges": "bytes"
        },
        mock_url
    )

    dm = DownloadManager()
    task_id = dm.add_download(mock_url, mock_file_name)
    async_thread_runner.submit(dm.start_download(task_id))

    await wait_for_state(dm, task_id, DownloadState.RUNNING)
    await mock_response.insert_chunk(chunks[0])
    
    # Try to pause multiple times concurrently
    future1 = async_thread_runner.submit(dm.pause_download(task_id))
    future2 = async_thread_runner.submit(dm.pause_download(task_id))
    future3 = async_thread_runner.submit(dm.pause_download(task_id))
    
    result1 = future1.result(timeout=15)
    result2 = future2.result(timeout=15)
    result3 = future3.result(timeout=15)
    
    # At least one should succeed
    results = [result1, result2, result3]
    assert True in results
    
    await wait_for_state(dm, task_id, DownloadState.PAUSED)

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)



@pytest.mark.asyncio
async def test_concurrent_delete_operations(async_thread_runner, create_mock_response_and_set_mock_session, test_file_setup_and_cleanup):
    """Test multiple simultaneous pause_download calls"""
    chunks = [b"a" * 1024]
    mock_url = "https://example.com/file.bin"
    mock_file_name = f"{inspect.currentframe().f_code.co_name}.txt"
    test_file_setup_and_cleanup(mock_file_name)

    mock_response = create_mock_response_and_set_mock_session(
        206,
        {
            "Content-Length": str(len(chunks[0])),
            "Accept-Ranges": "bytes"
        },
        mock_url
    )

    dm = DownloadManager()
    task_id = dm.add_download(mock_url, mock_file_name)
    async_thread_runner.submit(dm.start_download(task_id))

    await wait_for_state(dm, task_id, DownloadState.RUNNING)
    await mock_response.insert_chunk(chunks[0])
    
    # Try to pause multiple times concurrently
    future1 = async_thread_runner.submit(dm.delete_download(task_id))
    future2 = async_thread_runner.submit(dm.delete_download(task_id))
    future3 = async_thread_runner.submit(dm.delete_download(task_id))
    
    result1 = future1.result(timeout=15)
    result2 = future2.result(timeout=15)
    result3 = future3.result(timeout=15)
    
    # At least one should succeed
    results = [result1, result2, result3]
    assert True in results
    
    await wait_for_state(dm, task_id, DownloadState.DELETED)

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)


@pytest.mark.asyncio
async def test_start_and_pause_race(async_thread_runner, create_mock_response_and_set_mock_session, test_file_setup_and_cleanup):
    """Test race between start and pause operations"""
    chunks = [b"a" * 1024]
    mock_url = "https://example.com/file.bin"
    mock_file_name = f"{inspect.currentframe().f_code.co_name}.txt"
    test_file_setup_and_cleanup(mock_file_name)

    mock_response = create_mock_response_and_set_mock_session(
        206,
        {
            "Content-Length": str(len(chunks[0])),
            "Accept-Ranges": "bytes"
        },
        mock_url
    )

    dm = DownloadManager()
    task_id = dm.add_download(mock_url, mock_file_name)
    
    # Start and immediately pause
    start_future = async_thread_runner.submit(dm.start_download(task_id))
    pause_future = async_thread_runner.submit(dm.pause_download(task_id))
    
    start_result = start_future.result(timeout=15)
    pause_result = pause_future.result(timeout=15)
    
    # One should succeed
    assert start_result is True or pause_result is True
    
    # Wait for final state
    await asyncio.sleep(2)
    
    download = dm.get_downloads()[task_id]
    # Should be in a valid state
    assert download.state in [DownloadState.RUNNING, DownloadState.PAUSED, DownloadState.PENDING]

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)


@pytest.mark.asyncio
async def test_parallel_worker_concurrent_segment_access(async_thread_runner, create_parallel_mock_response_and_set_mock_session, test_file_setup_and_cleanup):
    """Test that workers don't pick up duplicate segments"""
    n_workers = 8
    segment_size = 1024
    dm = DownloadManager(maximum_workers_per_task=n_workers, parallel_download_segment_size=segment_size)

    mock_url = "https://example.com/file.txt"
    mock_file_name = f"{inspect.currentframe().f_code.co_name}.txt"
    test_file_setup_and_cleanup(mock_file_name)

    # Only 4 segments
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

    for key in data:
        mock_response.set_range_end_n_send(key, segment_size)
        mock_response.set_range_end_done(key)
    
    task_id = dm.add_download(mock_url, mock_file_name)
    async_thread_runner.submit(dm.start_download(task_id, use_parallel_download=True))
    
    await wait_for_state(dm, task_id, DownloadState.ALLOCATING_SPACE)
    
    # All workers should complete without duplicating work
    for _ in range(n_workers):
        await wait_for_state(dm, task_id, DownloadState.COMPLETED, timeout_sec=60)
    
    # File should be correct
    from tests.helpers import verify_file
    verify_file(
        mock_file_name,
        "".join(bytes(x).decode('ascii') for x in data.values())
    )

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)


@pytest.mark.asyncio
async def test_add_download_id_uniqueness():
    """Test that task IDs are always unique even with rapid additions"""
    dm = DownloadManager()
    
    task_ids = set()
    
    # Add many downloads rapidly
    for i in range(1000):
        task_id = dm.add_download(f"https://example.com/file{i}.bin", f"output{i}.bin")
        task_ids.add(task_id)
    
    # All IDs should be unique
    assert len(task_ids) == 1000


@pytest.mark.asyncio
async def test_parallel_multiple_pauses_during_different_segments(async_thread_runner, create_parallel_mock_response_and_set_mock_session, test_file_setup_and_cleanup):
    """Test pausing and resuming at different points in parallel download"""
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
        str((segment_size * 4) - 1): list(b"d" * segment_size),
        str((segment_size * 5) - 1): list(b"e" * segment_size),
        str((segment_size * 6) - 1): list(b"f" * segment_size),
    }

    mock_response = create_parallel_mock_response_and_set_mock_session(
        206,
        {
            "Content-Length": 6 * segment_size,
            "Accept-Ranges": "bytes"
        },
        mock_url,
        list(data.keys()),
        data
    )

    # Send partial data for first 2 segments
    for key in list(data.keys())[:2]:
        mock_response.set_range_end_n_send(key, segment_size // 3)
    
    task_id = dm.add_download(mock_url, mock_file_name)
    async_thread_runner.submit(dm.start_download(task_id, use_parallel_download=True))
    
    await wait_for_state(dm, task_id, DownloadState.ALLOCATING_SPACE)
    await wait_for_state(dm, task_id, DownloadState.RUNNING)
    
    # First pause
    async_thread_runner.submit(dm.pause_download(task_id))
    for _ in range(n_workers):
        await wait_for_state(dm, task_id, DownloadState.PAUSED)
    
    # Resume and send more partial data
    async_thread_runner.submit(dm.start_download(task_id, use_parallel_download=True))
    
    for key in list(data.keys())[2:4]:
        mock_response.set_range_end_n_send(key, segment_size // 2)
    
    await wait_for_state(dm, task_id, DownloadState.RUNNING)
    
    # Second pause
    async_thread_runner.submit(dm.pause_download(task_id))
    for _ in range(n_workers):
        await wait_for_state(dm, task_id, DownloadState.PAUSED)
    
    # Final resume and complete
    async_thread_runner.submit(dm.start_download(task_id, use_parallel_download=True))
    
    for key in data:
        mock_response.set_range_end_n_send(key, segment_size)
        mock_response.set_range_end_done(key)
    
    for _ in range(n_workers):
        await wait_for_state(dm, task_id, DownloadState.COMPLETED, timeout_sec=60)
    
    from tests.helpers import verify_file
    verify_file(
        mock_file_name,
        "".join(bytes(x).decode('ascii') for x in data.values())
    )

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)


@pytest.mark.asyncio
async def test_rapid_delete_and_read(async_thread_runner, create_mock_response_and_set_mock_session, test_file_setup_and_cleanup):
    """Test rapidly deleting and re-adding same download"""
    mock_url = "https://example.com/file.bin"
    mock_file_name = f"{inspect.currentframe().f_code.co_name}.txt"
    test_file_setup_and_cleanup(mock_file_name)

    chunks = [b"test"]
    
    mock_response = create_mock_response_and_set_mock_session(
        206,
        {
            "Content-Length": str(len(chunks[0])),
            "Accept-Ranges": "bytes"
        },
        mock_url
    )

    dm = DownloadManager()
    
    for i in range(5):
        logging.debug(f"Loop Number: {i}")
        # Add
        task_id = dm.add_download(mock_url, mock_file_name)
        
        # Start
        async_thread_runner.submit(dm.start_download(task_id))
        await wait_for_state(dm, task_id, DownloadState.RUNNING)
        
        # Delete
        async_thread_runner.submit(dm.delete_download(task_id, remove_file=True))
        await wait_for_state(dm, task_id, DownloadState.DELETED)
        
        assert task_id not in dm.get_downloads()
    
    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)
