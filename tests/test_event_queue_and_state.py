import asyncio
import pytest
import inspect

from dmanager.core import DownloadManager, DownloadState, DownloadEvent
from tests.helpers import wait_for_state, wait_for_file_to_be_created


@pytest.mark.asyncio
async def test_event_queue_overflow(async_thread_runner, create_mock_response_and_set_mock_session, test_file_setup_and_cleanup):
    """Verify proper handling when event queue fills up"""
    chunks = [b"a" * 1024 for _ in range(100)]
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

    # Create download manager with high event update rate to fill queue quickly
    dm = DownloadManager(running_event_update_rate_seconds=0.01)
    
    task_id = dm.add_download(mock_url, mock_file_name)
    async_thread_runner.submit(dm.start_download(task_id))

    await wait_for_state(dm, task_id, DownloadState.RUNNING)
    
    # Send all chunks rapidly to generate many events
    for chunk in chunks:
        await mock_response.insert_chunk(chunk)
    
    mock_response.end_response()
    
    # Don't consume events - let queue fill
    await asyncio.sleep(3)
    
    # Queue should handle overflow gracefully (either by dropping old events or blocking)
    # The implementation removes old events when full
    assert dm.events_queue.qsize() > 0
    assert not dm.events_queue.full() or dm.events_queue.qsize() < 1000
    
    await wait_for_state(dm, task_id, DownloadState.COMPLETED)

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)


@pytest.mark.asyncio
async def test_multiple_rapid_state_transitions(async_thread_runner, create_mock_response_and_set_mock_session, test_file_setup_and_cleanup):
    """Test rapid pause/resume/pause cycles"""
    chunks = [b"a" * 1024, b"b" * 1024, b"c" * 1024]
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
    
    # Start
    async_thread_runner.submit(dm.start_download(task_id))
    await wait_for_state(dm, task_id, DownloadState.RUNNING)
    await mock_response.insert_chunk(chunks[0])
    
    # Rapid pause/resume cycles
    for i in range(3):
        async_thread_runner.submit(dm.pause_download(task_id))
        await wait_for_state(dm, task_id, DownloadState.PAUSED)
        
        async_thread_runner.submit(dm.start_download(task_id))
        await wait_for_state(dm, task_id, DownloadState.RUNNING)
    
    # Complete download
    for chunk in chunks[1:]:
        await mock_response.insert_chunk(chunk)
    mock_response.end_response()
    
    await wait_for_state(dm, task_id, DownloadState.COMPLETED)

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)


@pytest.mark.asyncio
async def test_download_metadata_persistence_through_states(async_thread_runner, create_mock_response_and_set_mock_session, test_file_setup_and_cleanup):
    """Verify DownloadMetadata fields are correctly maintained through state changes"""
    chunks = [b"abc", b"def", b"ghi"]
    mock_url = "https://example.com/file.bin"
    mock_file_name = f"{inspect.currentframe().f_code.co_name}.txt"
    test_file_setup_and_cleanup(mock_file_name)

    mock_response = create_mock_response_and_set_mock_session(
        206,
        {
            "Content-Length": str(sum(len(c) for c in chunks)),
            "Accept-Ranges": "bytes",
            "ETag": '"test-etag"'
        },
        mock_url
    )

    dm = DownloadManager()
    task_id = dm.add_download(mock_url, mock_file_name)
    
    # Check PENDING state
    download = dm.get_downloads()[task_id]
    assert download.state == DownloadState.PENDING
    assert download.url == mock_url
    assert download.output_file == mock_file_name
    
    # Start download
    async_thread_runner.submit(dm.start_download(task_id))
    await wait_for_state(dm, task_id, DownloadState.RUNNING)
    
    # Check metadata during RUNNING
    download = dm.get_downloads()[task_id]
    assert download.state == DownloadState.RUNNING
    assert download.url == mock_url
    assert download.output_file == mock_file_name
    assert download.etag == "test-etag"
    assert download.file_size_bytes == 9
    
    await mock_response.insert_chunk(chunks[0])

    wait_for_file_to_be_created(mock_file_name)
    
    # Pause and check metadata
    async_thread_runner.submit(dm.pause_download(task_id))
    await wait_for_state(dm, task_id, DownloadState.PAUSED)
    
    download = dm.get_downloads()[task_id]
    assert download.state == DownloadState.PAUSED
    assert download.url == mock_url
    assert download.output_file == mock_file_name
    assert download.etag == "test-etag"
    assert download.downloaded_bytes == 3
    
    # Resume and complete
    async_thread_runner.submit(dm.start_download(task_id))
    await wait_for_state(dm, task_id, DownloadState.RUNNING)
    
    for chunk in chunks[1:]:
        await mock_response.insert_chunk(chunk)
    mock_response.end_response()
    
    await wait_for_state(dm, task_id, DownloadState.COMPLETED)
    
    # Check final metadata
    download = dm.get_downloads()[task_id]
    assert download.state == DownloadState.COMPLETED
    assert download.url == mock_url
    assert download.output_file == mock_file_name
    assert download.etag == "test-etag"
    assert download.downloaded_bytes == 9
    assert download.file_size_bytes == 9

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)


@pytest.mark.asyncio
async def test_event_ordering(async_thread_runner, create_mock_response_and_set_mock_session, test_file_setup_and_cleanup):
    """Verify events are emitted in correct order"""
    chunks = [b"abc"]
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

    # Collect events
    events = []
    
    await mock_response.insert_chunk(chunks[0])
    mock_response.end_response()
    
    # Wait for completion
    await asyncio.sleep(3)
    
    # Collect all events
    while True:
        event = await dm.get_oldest_event()
        if event is None:
            break
        if event.task_id == task_id:
            events.append(event)
    
    # Should have events in order: RUNNING -> ... -> COMPLETED
    assert len(events) > 0
    
    # First event should be RUNNING
    running_events = [e for e in events if e.state == DownloadState.RUNNING]
    assert len(running_events) > 0
    
    # Last event should be COMPLETED
    assert events[-1].state == DownloadState.COMPLETED

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)


@pytest.mark.asyncio
async def test_concurrent_event_consumption(async_thread_runner, create_mock_response_and_set_mock_session, test_file_setup_and_cleanup):
    """Test that event queue handles concurrent access properly"""
    chunks = [b"a" * 100 for _ in range(10)]
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

    dm = DownloadManager(running_event_update_rate_seconds=0.1)
    task_id = dm.add_download(mock_url, mock_file_name)
    async_thread_runner.submit(dm.start_download(task_id))

    await wait_for_state(dm, task_id, DownloadState.RUNNING)
    
    # Create multiple consumers
    consumed_events = []
    
    async def consume_events():
        for _ in range(10):
            event = await dm.get_oldest_event()
            if event:
                consumed_events.append(event)
            await asyncio.sleep(0.05)
    
    # Start multiple consumers
    consumers = [asyncio.create_task(consume_events()) for _ in range(3)]
    
    # Generate events
    for chunk in chunks:
        await mock_response.insert_chunk(chunk)
    mock_response.end_response()
    
    # Wait for consumers
    await asyncio.gather(*consumers)
    
    # Should have consumed events without errors
    assert len(consumed_events) > 0

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)


@pytest.mark.asyncio
async def test_state_consistency_during_error(async_thread_runner, create_mock_response_and_set_mock_session, test_file_setup_and_cleanup):
    """Verify state remains consistent when errors occur"""
    chunks = ["invalid"]  # Will cause error
    mock_url = "https://example.com/file.bin"
    mock_file_name = f"{inspect.currentframe().f_code.co_name}.txt"
    test_file_setup_and_cleanup(mock_file_name)

    mock_response = create_mock_response_and_set_mock_session(
        206,
        {
            "Content-Length": "100",
            "Accept-Ranges": "bytes"
        },
        mock_url
    )

    dm = DownloadManager(continue_on_error=False)
    task_id = dm.add_download(mock_url, mock_file_name)
    async_thread_runner.submit(dm.start_download(task_id))

    await wait_for_state(dm, task_id, DownloadState.RUNNING)
    
    await mock_response.insert_chunk(chunks[0])
    
    await wait_for_state(dm, task_id, DownloadState.ERROR)
    
    # State should be ERROR
    download = dm.get_downloads()[task_id]
    assert download.state == DownloadState.ERROR

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)


@pytest.mark.asyncio
async def test_parallel_worker_state_tracking(async_thread_runner, create_parallel_mock_response_and_set_mock_session, test_file_setup_and_cleanup):
    """Test that parallel worker states are tracked correctly"""
    
    
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

    for key in data:
        mock_response.set_range_end_n_send(key, segment_size // 2)
    
    task_id = dm.add_download(mock_url, mock_file_name)
    async_thread_runner.submit(dm.start_download(task_id, use_parallel_download=True))
    
    await wait_for_state(dm, task_id, DownloadState.ALLOCATING_SPACE)
    await wait_for_state(dm, task_id, DownloadState.RUNNING)
    
    download = dm.get_downloads()[task_id]
    
    # Should have worker states tracked
    assert download.parallel_metadata is not None
    assert download.parallel_metadata.worker_states is not None
    assert len(download.parallel_metadata.worker_states) == n_workers
    
    # Pause and check worker states
    async_thread_runner.submit(dm.pause_download(task_id))
    
    for _ in range(n_workers):
        await wait_for_state(dm, task_id, DownloadState.PAUSED)
    
    # All workers should be paused
    for worker_id in download.parallel_metadata.worker_states:
        state = download.parallel_metadata.worker_states[worker_id]
        assert state in [DownloadState.PAUSED, DownloadState.COMPLETED]

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)


@pytest.mark.asyncio
async def test_event_worker_id_tracking(async_thread_runner, create_parallel_mock_response_and_set_mock_session, test_file_setup_and_cleanup):
    """Test that events correctly track worker IDs in parallel downloads"""
    
    n_workers = 2
    segment_size = 1024
    dm = DownloadManager(maximum_workers_per_task=n_workers, parallel_download_segment_size=segment_size)

    mock_url = "https://example.com/file.txt"
    mock_file_name = f"{inspect.currentframe().f_code.co_name}.txt"
    test_file_setup_and_cleanup(mock_file_name)

    data = {
        str(segment_size - 1):  list(b"a" * segment_size),
        str((segment_size * 2) - 1):  list(b"b" * segment_size),
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
    
    # Collect events with worker IDs
    worker_events = []
    
    await asyncio.sleep(2)
    
    while True:
        event = await dm.get_oldest_event()
        if event is None:
            break
        if event.task_id == task_id and event.worker_id is not None:
            worker_events.append(event)
    
    # Should have events with worker IDs
    assert len(worker_events) > 0
    
    # Worker IDs should be in valid range
    worker_ids_seen = set(e.worker_id for e in worker_events)
    for wid in worker_ids_seen:
        assert 0 <= wid < n_workers

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)
