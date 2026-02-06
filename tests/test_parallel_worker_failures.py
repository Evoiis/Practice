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
    
    for _ in range(2):
        # 1 for worker state, 1 for download state
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
    
    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)

# @pytest.mark.asyncio
# async def test_parallel_worker_continue_on_failure(async_thread_runner, create_parallel_mock_response_and_set_mock_session, test_file_setup_and_cleanup):
#     """Test parallel workers still continue after encountering a failure."""

#     n_workers = 4
#     segment_size = 1024
#     dm = DownloadManager(maximum_workers_per_task=n_workers, parallel_download_segment_size=segment_size)

#     mock_url = "https://example.com/file.txt"
#     mock_file_name = "test_file.txt"
#     test_file_setup_and_cleanup(mock_file_name)

#     data = {
#         str(segment_size - 1):  list(b"a" * segment_size),
#         str((segment_size * 2) - 1):  list(b"b" * segment_size),
#         str((segment_size * 3) - 1):  list(b"c" * segment_size),
#         str((segment_size * 4) - 1): list(b"d" * segment_size)
#     }

#     mock_response = create_parallel_mock_response_and_set_mock_session(
#         206,
#         {
#             "Content-Length": 4 * segment_size,
#             "Accept-Ranges": "bytes"
#         },
#         mock_url,
#         list(data.keys()),
#         data
#     )

#     # Send each worker half of their chunks
#     for key in list(data.keys()):
#         mock_response.set_range_end_n_send(key, segment_size/2)
    
#     task_id = dm.add_download(mock_url, mock_file_name)
#     async_thread_runner.submit(dm.start_download(task_id, use_parallel_download=True))

#     await wait_for_state(dm, task_id, DownloadState.ALLOCATING_SPACE)
#     await wait_for_state(dm, task_id, DownloadState.RUNNING)

#     mock_response.set_exception(Exception("Fake: Something bad happened"))
#     await wait_for_state(dm, task_id, DownloadState.ERROR)
#     mock_response.set_exception(None)

#     # Send the rest of the file
#     for key in data:
#         mock_response.set_range_end_n_send(key, segment_size)
#         mock_response.set_range_end_done(key)

#     verify_file(
#         mock_file_name,
#         "".join(bytes(x).decode('ascii') for x in data.values())
#     )

#     future = async_thread_runner.submit(dm.shutdown())
#     future.result(timeout=15)

# @pytest.mark.asyncio
# async def test_parallel_worker_failure_pause_still_works(async_thread_runner, create_parallel_mock_response_and_set_mock_session, test_file_setup_and_cleanup):
#     """Test that pause/delete works correctly even after worker failure
    
#     CRITICAL: Tests issue #14 - pause/delete should work after error
#     """
#     n_workers = 4
#     segment_size = 1024
#     dm = DownloadManager(maximum_workers_per_task=n_workers, parallel_download_segment_size=segment_size)

#     mock_url = "https://example.com/file.txt"
#     mock_file_name = "test_file.txt"
#     test_file_setup_and_cleanup(mock_file_name)

#     data = {
#         str(segment_size - 1):  list(b"a" * segment_size),
#         str((segment_size * 2) - 1):  list(b"b" * segment_size),
#         str((segment_size * 3) - 1):  list(b"c" * segment_size),
#         str((segment_size * 4) - 1): list(b"d" * segment_size)
#     }

#     mock_response = create_parallel_mock_response_and_set_mock_session(
#         206,
#         {
#             "Content-Length": 4 * segment_size,
#             "Accept-Ranges": "bytes"
#         },
#         mock_url,
#         list(data.keys()),
#         data
#     )

#     # Set up segments to cause error
#     for key in list(data.keys())[:2]:
#         mock_response.set_range_end_n_send(key, segment_size)
#         mock_response.set_range_end_done(key)
    
#     task_id = dm.add_download(mock_url, mock_file_name)
#     async_thread_runner.submit(dm.start_download(task_id, use_parallel_download=True))
    
#     await wait_for_state(dm, task_id, DownloadState.ALLOCATING_SPACE)
#     await wait_for_state(dm, task_id, DownloadState.RUNNING)
    
#     # Cause error
#     data[str((segment_size * 3) - 1)] = "invalid"
#     mock_response.set_range_end_n_send(str((segment_size * 3) - 1), 100)
    
#     await wait_for_state(dm, task_id, DownloadState.ERROR, timeout_sec=30)
    
#     # Now try to delete - should work despite error
#     delete_future = async_thread_runner.submit(dm.delete_download(task_id, remove_file=True))
#     result = delete_future.result(timeout=15)
    
#     assert result is True
    
#     # Verify task is deleted
#     await wait_for_state(dm, task_id, DownloadState.DELETED, timeout_sec=15)
#     assert task_id not in dm.get_downloads()
#     assert task_id not in dm._task_pools
    
#     future = async_thread_runner.submit(dm.shutdown())
#     future.result(timeout=15)


# @pytest.mark.asyncio
# async def test_parallel_worker_failure_then_restart(async_thread_runner, create_parallel_mock_response_and_set_mock_session, test_file_setup_and_cleanup):
#     """Test restarting download after worker failure
    
#     CRITICAL: Tests issue #13 - restart after failure should work
#     """
#     n_workers = 4
#     segment_size = 1024
#     dm = DownloadManager(maximum_workers_per_task=n_workers, parallel_download_segment_size=segment_size)

#     mock_url = "https://example.com/file.txt"
#     mock_file_name = "test_file.txt"
#     test_file_setup_and_cleanup(mock_file_name)

#     data = {
#         str(segment_size - 1):  list(b"a" * segment_size),
#         str((segment_size * 2) - 1):  list(b"b" * segment_size),
#         str((segment_size * 3) - 1):  list(b"c" * segment_size),
#         str((segment_size * 4) - 1): list(b"d" * segment_size)
#     }

#     mock_response = create_parallel_mock_response_and_set_mock_session(
#         206,
#         {
#             "Content-Length": 4 * segment_size,
#             "Accept-Ranges": "bytes"
#         },
#         mock_url,
#         list(data.keys()),
#         data
#     )

#     # First attempt - will fail
#     for key in list(data.keys())[:2]:
#         mock_response.set_range_end_n_send(key, segment_size // 2)
    
#     task_id = dm.add_download(mock_url, mock_file_name)
#     async_thread_runner.submit(dm.start_download(task_id, use_parallel_download=True))
    
#     await wait_for_state(dm, task_id, DownloadState.ALLOCATING_SPACE)
#     await wait_for_state(dm, task_id, DownloadState.RUNNING)
    
#     # Cause error
#     data[str((segment_size * 3) - 1)] = "invalid"
#     mock_response.set_range_end_n_send(str((segment_size * 3) - 1), 100)
    
#     await wait_for_state(dm, task_id, DownloadState.ERROR, timeout_sec=30)
    
#     download = dm.get_downloads()[task_id]
#     assert download.state == DownloadState.ERROR
    
#     # Now fix the data and restart
#     data[str((segment_size * 3) - 1)] = list(b"c" * segment_size)
#     data[str((segment_size * 4) - 1)] = list(b"d" * segment_size)
    
#     # Clear and reset all segments
#     for key in data.keys():
#         mock_response.set_range_end_n_send(key, segment_size)
#         mock_response.set_range_end_done(key)
    
#     # Restart download
#     restart_future = async_thread_runner.submit(dm.start_download(task_id, use_parallel_download=True))
#     result = restart_future.result(timeout=15)
    
#     assert result is True  # Should allow restart from ERROR state
    
#     # Should eventually complete
#     await wait_for_state(dm, task_id, DownloadState.ALLOCATING_SPACE, timeout_sec=30)
    
#     for _ in range(n_workers):
#         await wait_for_state(dm, task_id, DownloadState.COMPLETED, timeout_sec=60)
    
#     # Verify file integrity
#     verify_file(
#         mock_file_name,
#         "".join(bytes(x).decode('ascii') for x in data.values())
#     )
    
#     future = async_thread_runner.submit(dm.shutdown())
#     future.result(timeout=15)


# @pytest.mark.asyncio
# async def test_parallel_multiple_workers_fail_simultaneously(async_thread_runner, create_parallel_mock_response_and_set_mock_session, test_file_setup_and_cleanup):
#     """Test state when multiple workers fail at same time"""
#     n_workers = 4
#     segment_size = 1024
#     dm = DownloadManager(maximum_workers_per_task=n_workers, parallel_download_segment_size=segment_size)

#     mock_url = "https://example.com/file.txt"
#     mock_file_name = "test_file.txt"
#     test_file_setup_and_cleanup(mock_file_name)

#     data = {
#         str(segment_size - 1):  list(b"a" * segment_size),
#         str((segment_size * 2) - 1):  list(b"b" * segment_size),
#         str((segment_size * 3) - 1):  list(b"c" * segment_size),
#         str((segment_size * 4) - 1): list(b"d" * segment_size)
#     }

#     mock_response = create_parallel_mock_response_and_set_mock_session(
#         206,
#         {
#             "Content-Length": 4 * segment_size,
#             "Accept-Ranges": "bytes"
#         },
#         mock_url,
#         list(data.keys()),
#         data
#     )

#     # All segments will cause errors
#     for i, key in enumerate(data.keys()):
#         data[key] = f"invalid{i}"  # String instead of bytes list
#         mock_response.set_range_end_n_send(key, 100)
    
#     task_id = dm.add_download(mock_url, mock_file_name)
#     async_thread_runner.submit(dm.start_download(task_id, use_parallel_download=True))
    
#     await wait_for_state(dm, task_id, DownloadState.ALLOCATING_SPACE)
#     await wait_for_state(dm, task_id, DownloadState.RUNNING)
    
#     # Should transition to ERROR
#     await wait_for_state(dm, task_id, DownloadState.ERROR, timeout_sec=30)
    
#     download = dm.get_downloads()[task_id]
    
#     # State should be ERROR and stable
#     assert download.state == DownloadState.ERROR
    
#     # Verify state doesn't oscillate
#     await asyncio.sleep(3)
#     assert download.state == DownloadState.ERROR
    
#     # Should be able to clean up
#     delete_future = async_thread_runner.submit(dm.delete_download(task_id, remove_file=True))
#     assert delete_future.result(timeout=15) is True
    
#     future = async_thread_runner.submit(dm.shutdown())
#     future.result(timeout=15)


# @pytest.mark.asyncio
# async def test_parallel_worker_failure_state_transitions(async_thread_runner, create_parallel_mock_response_and_set_mock_session, test_file_setup_and_cleanup):
#     """Test detailed state transitions when worker fails
    
#     Verifies that state transitions are:
#     PENDING -> ALLOCATING_SPACE -> RUNNING -> ERROR
#     and not corrupted values
#     """
#     n_workers = 4
#     segment_size = 1024
#     dm = DownloadManager(maximum_workers_per_task=n_workers, parallel_download_segment_size=segment_size)

#     mock_url = "https://example.com/file.txt"
#     mock_file_name = "test_file.txt"
#     test_file_setup_and_cleanup(mock_file_name)

#     data = {
#         str(segment_size - 1):  list(b"a" * segment_size),
#         str((segment_size * 2) - 1):  list(b"b" * segment_size),
#         str((segment_size * 3) - 1):  list(b"c" * segment_size),
#         str((segment_size * 4) - 1): list(b"d" * segment_size)
#     }

#     mock_response = create_parallel_mock_response_and_set_mock_session(
#         206,
#         {
#             "Content-Length": 4 * segment_size,
#             "Accept-Ranges": "bytes"
#         },
#         mock_url,
#         list(data.keys()),
#         data
#     )

#     task_id = dm.add_download(mock_url, mock_file_name)
    
#     # Track state transitions
#     seen_states = []
    
#     async def track_states():
#         while True:
#             event = await dm.get_oldest_event()
#             if event is None:
#                 await asyncio.sleep(0.1)
#                 continue
#             if event.task_id == task_id:
#                 seen_states.append(event.state)
#                 if event.state == DownloadState.ERROR:
#                     break
    
#     tracker_task = asyncio.create_task(track_states())
    
#     # Set up first segment to work, rest to fail
#     mock_response.set_range_end_n_send(str(segment_size - 1), segment_size)
#     mock_response.set_range_end_done(str(segment_size - 1))
    
#     for key in list(data.keys())[1:]:
#         data[key] = "invalid"
#         mock_response.set_range_end_n_send(key, 100)
    
#     async_thread_runner.submit(dm.start_download(task_id, use_parallel_download=True))
    
#     await wait_for_state(dm, task_id, DownloadState.ERROR, timeout_sec=60)
    
#     # Stop tracker
#     tracker_task.cancel()
#     try:
#         await tracker_task
#     except asyncio.CancelledError:
#         pass
    
#     logging.info(f"Seen states: {seen_states}")
    
#     # Verify state progression is valid
#     valid_states = {DownloadState.PENDING, DownloadState.ALLOCATING_SPACE, 
#                     DownloadState.RUNNING, DownloadState.ERROR, DownloadState.COMPLETED}
    
#     for state in seen_states:
#         assert state in valid_states, f"Invalid state in progression: {state}"
    
#     # Should have RUNNING before ERROR
#     if DownloadState.ERROR in seen_states:
#         error_idx = seen_states.index(DownloadState.ERROR)
#         running_indices = [i for i, s in enumerate(seen_states) if s == DownloadState.RUNNING]
#         if running_indices:
#             assert min(running_indices) < error_idx, "ERROR appeared before RUNNING"
    
#     future = async_thread_runner.submit(dm.shutdown())
#     future.result(timeout=15)


# @pytest.mark.asyncio
# async def test_parallel_worker_failure_metadata_integrity(async_thread_runner, create_parallel_mock_response_and_set_mock_session, test_file_setup_and_cleanup):
#     """Test that download metadata remains valid after worker failure
    
#     Ensures fields like downloaded_bytes, file_size_bytes, etc. stay consistent
#     """
#     n_workers = 4
#     segment_size = 1024
#     dm = DownloadManager(maximum_workers_per_task=n_workers, parallel_download_segment_size=segment_size)

#     mock_url = "https://example.com/file.txt"
#     mock_file_name = "test_file.txt"
#     test_file_setup_and_cleanup(mock_file_name)

#     data = {
#         str(segment_size - 1):  list(b"a" * segment_size),
#         str((segment_size * 2) - 1):  list(b"b" * segment_size),
#         str((segment_size * 3) - 1):  list(b"c" * segment_size),
#         str((segment_size * 4) - 1): list(b"d" * segment_size)
#     }

#     mock_response = create_parallel_mock_response_and_set_mock_session(
#         206,
#         {
#             "Content-Length": 4 * segment_size,
#             "Accept-Ranges": "bytes"
#         },
#         mock_url,
#         list(data.keys()),
#         data
#     )

#     # Send partial data then fail
#     for key in list(data.keys())[:2]:
#         mock_response.set_range_end_n_send(key, segment_size // 2)
    
#     for key in list(data.keys())[2:]:
#         data[key] = "invalid"
#         mock_response.set_range_end_n_send(key, 100)
    
#     task_id = dm.add_download(mock_url, mock_file_name)
#     async_thread_runner.submit(dm.start_download(task_id, use_parallel_download=True))
    
#     await wait_for_state(dm, task_id, DownloadState.ALLOCATING_SPACE)
#     await wait_for_state(dm, task_id, DownloadState.RUNNING)
#     await wait_for_state(dm, task_id, DownloadState.ERROR, timeout_sec=30)
    
#     download = dm.get_downloads()[task_id]
    
#     # Verify metadata is sane
#     assert download.task_id == task_id
#     assert download.url == mock_url
#     assert download.output_file == mock_file_name
#     assert download.file_size_bytes == 4 * segment_size
#     assert download.state == DownloadState.ERROR
    
#     # downloaded_bytes should be >= 0 and <= file_size_bytes
#     assert 0 <= download.downloaded_bytes <= download.file_size_bytes
    
#     # parallel_metadata should exist
#     assert download.parallel_metadata is not None
#     assert download.parallel_metadata.worker_states is not None
    
#     # No corruption of basic types
#     assert isinstance(download.task_id, int)
#     assert isinstance(download.url, str)
#     assert isinstance(download.output_file, str)
#     assert isinstance(download.state, DownloadState)
    
#     future = async_thread_runner.submit(dm.shutdown())
#     future.result(timeout=15)


# @pytest.mark.asyncio
# async def test_parallel_pause_during_worker_error_transition(async_thread_runner, create_parallel_mock_response_and_set_mock_session, test_file_setup_and_cleanup):
#     """Test pausing while worker is transitioning to error state
    
#     Race condition: pause called just as worker errors
#     """
#     n_workers = 4
#     segment_size = 1024
#     dm = DownloadManager(maximum_workers_per_task=n_workers, parallel_download_segment_size=segment_size)

#     mock_url = "https://example.com/file.txt"
#     mock_file_name = "test_file.txt"
#     test_file_setup_and_cleanup(mock_file_name)

#     data = {
#         str(segment_size - 1):  list(b"a" * segment_size),
#         str((segment_size * 2) - 1):  list(b"b" * segment_size),
#         str((segment_size * 3) - 1):  list(b"c" * segment_size),
#         str((segment_size * 4) - 1): list(b"d" * segment_size)
#     }

#     mock_response = create_parallel_mock_response_and_set_mock_session(
#         206,
#         {
#             "Content-Length": 4 * segment_size,
#             "Accept-Ranges": "bytes"
#         },
#         mock_url,
#         list(data.keys()),
#         data
#     )

#     # Send some data
#     for key in list(data.keys())[:2]:
#         mock_response.set_range_end_n_send(key, segment_size // 3)
    
#     task_id = dm.add_download(mock_url, mock_file_name)
#     async_thread_runner.submit(dm.start_download(task_id, use_parallel_download=True))
    
#     await wait_for_state(dm, task_id, DownloadState.ALLOCATING_SPACE)
#     await wait_for_state(dm, task_id, DownloadState.RUNNING)
    
#     # Trigger error
#     for key in list(data.keys())[2:]:
#         data[key] = "invalid"
#         mock_response.set_range_end_n_send(key, 100)
    
#     # Immediately try to pause (race with error)
#     pause_future = async_thread_runner.submit(dm.pause_download(task_id))
    
#     # Wait for either ERROR or PAUSED
#     await asyncio.sleep(2)
    
#     download = dm.get_downloads()[task_id]
    
#     # Should be in either ERROR or PAUSED state (both valid)
#     assert download.state in [DownloadState.ERROR, DownloadState.PAUSED]
    
#     # Should still be able to delete
#     delete_future = async_thread_runner.submit(dm.delete_download(task_id, remove_file=True))
#     assert delete_future.result(timeout=15) is True
    
#     future = async_thread_runner.submit(dm.shutdown())
#     future.result(timeout=15)

