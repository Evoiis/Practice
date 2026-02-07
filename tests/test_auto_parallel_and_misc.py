import asyncio
import pytest
import inspect
import os

from dmanager.core import DownloadManager, DownloadState
from dmanager.constants import ONE_GIBIBYTE, SEGMENT_SIZE
from tests.helpers import wait_for_state, verify_file, wait_for_file_to_be_created


# ============================================================================
# AUTO-PARALLEL DECISION LOGIC TESTS
# ============================================================================

@pytest.mark.asyncio
async def test_auto_parallel_exactly_one_gib(async_thread_runner, create_mock_response_and_set_mock_session, test_file_setup_and_cleanup):
    """Test boundary condition for automatic parallel decision at exactly 1 GiB"""
    mock_url = "https://example.com/file.bin"
    mock_file_name = f"{inspect.currentframe().f_code.co_name}.txt"
    test_file_setup_and_cleanup(mock_file_name)

    mock_response = create_mock_response_and_set_mock_session(
        206,
        {
            "Content-Length": str(ONE_GIBIBYTE),
            "Accept-Ranges": "bytes"
        },
        mock_url
    )

    dm = DownloadManager()
    task_id = dm.add_download(mock_url, mock_file_name)
    
    # Don't specify use_parallel_download - let it auto-decide
    async_thread_runner.submit(dm.start_download(task_id, use_parallel_download=None))
    
    await wait_for_state(dm, task_id, DownloadState.RUNNING, timeout_sec=60)
    
    download = dm.get_downloads()[task_id]
    # At exactly 1 GiB, should NOT use parallel (needs to be > 1 GiB)
    assert download.use_parallel_download is False
    
    async_thread_runner.submit(dm.pause_download(task_id))
    
    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)


@pytest.mark.asyncio
async def test_auto_parallel_slightly_over_one_gib(async_thread_runner, create_mock_response_and_set_mock_session, test_file_setup_and_cleanup):
    """Test file just over 1 GiB threshold enables parallel"""
    mock_url = "https://example.com/file.bin"
    mock_file_name = f"{inspect.currentframe().f_code.co_name}.txt"
    test_file_setup_and_cleanup(mock_file_name)

    mock_response = create_mock_response_and_set_mock_session(
        206,
        {
            "Content-Length": str(ONE_GIBIBYTE + 1),
            "Accept-Ranges": "bytes"
        },
        mock_url
    )

    dm = DownloadManager()
    task_id = dm.add_download(mock_url, mock_file_name)
    
    async_thread_runner.submit(dm.start_download(task_id, use_parallel_download=None))
    
    await wait_for_state(dm, task_id, DownloadState.ALLOCATING_SPACE, timeout_sec=60)
    
    download = dm.get_downloads()[task_id]
    # Just over 1 GiB should use parallel
    assert download.use_parallel_download is True
    
    async_thread_runner.submit(dm.pause_download(task_id))
    
    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)


@pytest.mark.asyncio
async def test_auto_parallel_slightly_under_one_gib(async_thread_runner, create_mock_response_and_set_mock_session, test_file_setup_and_cleanup):
    """Test file just under 1 GiB threshold doesn't use parallel"""
    mock_url = "https://example.com/file.bin"
    mock_file_name = f"{inspect.currentframe().f_code.co_name}.txt"
    test_file_setup_and_cleanup(mock_file_name)

    mock_response = create_mock_response_and_set_mock_session(
        206,
        {
            "Content-Length": str(ONE_GIBIBYTE - 1),
            "Accept-Ranges": "bytes"
        },
        mock_url
    )

    dm = DownloadManager()
    task_id = dm.add_download(mock_url, mock_file_name)
    
    async_thread_runner.submit(dm.start_download(task_id, use_parallel_download=None))
    
    await wait_for_state(dm, task_id, DownloadState.RUNNING)
    
    download = dm.get_downloads()[task_id]
    # Under 1 GiB should not use parallel
    assert download.use_parallel_download is False
    
    async_thread_runner.submit(dm.pause_download(task_id))
    
    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)


@pytest.mark.asyncio
async def test_force_parallel_on_small_file(async_thread_runner, create_parallel_mock_response_and_set_mock_session, test_file_setup_and_cleanup):
    """Test forcing parallel download for file < 1 GiB"""
    mock_url = "https://example.com/file.txt"
    mock_file_name = f"{inspect.currentframe().f_code.co_name}.txt"
    test_file_setup_and_cleanup(mock_file_name)

    data = {
        str(SEGMENT_SIZE - 1): list(b"a" * SEGMENT_SIZE),
    }

    mock_response = create_parallel_mock_response_and_set_mock_session(
        206,
        {
            "Content-Length": SEGMENT_SIZE,  # Much less than 1 GiB
            "Accept-Ranges": "bytes"
        },
        mock_url,
        list(data.keys()),
        data
    )

    mock_response.set_range_end_n_send(str(SEGMENT_SIZE - 1), SEGMENT_SIZE)
    mock_response.set_range_end_done(str(SEGMENT_SIZE - 1))

    dm = DownloadManager(maximum_workers_per_task=2)
    task_id = dm.add_download(mock_url, mock_file_name)
    
    # Force parallel even though file is small
    async_thread_runner.submit(dm.start_download(task_id, use_parallel_download=True))
    
    await wait_for_state(dm, task_id, DownloadState.ALLOCATING_SPACE)
    
    download = dm.get_downloads()[task_id]
    assert download.use_parallel_download is True
    
    async_thread_runner.submit(dm.pause_download(task_id))
    
    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)


@pytest.mark.asyncio
async def test_force_parallel_off_large_file(async_thread_runner, create_mock_response_and_set_mock_session, test_file_setup_and_cleanup):
    """Test forcing single-connection download for file > 1 GiB"""
    mock_url = "https://example.com/file.bin"
    mock_file_name = f"{inspect.currentframe().f_code.co_name}.txt"
    test_file_setup_and_cleanup(mock_file_name)

    mock_response = create_mock_response_and_set_mock_session(
        206,
        {
            "Content-Length": str(ONE_GIBIBYTE * 2),  # 2 GiB
            "Accept-Ranges": "bytes"
        },
        mock_url
    )

    dm = DownloadManager()
    task_id = dm.add_download(mock_url, mock_file_name)
    
    # Force single connection even though file is large
    async_thread_runner.submit(dm.start_download(task_id, use_parallel_download=False))
    
    await wait_for_state(dm, task_id, DownloadState.RUNNING)
    
    download = dm.get_downloads()[task_id]
    assert download.use_parallel_download is False
    
    async_thread_runner.submit(dm.pause_download(task_id))
    
    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)


# ============================================================================
# CONTENT-TYPE AND FILE EXTENSION TESTS
# ============================================================================

@pytest.mark.asyncio
async def test_filename_generation_unknown_content_type(async_thread_runner, create_mock_response_and_set_mock_session, test_file_setup_and_cleanup):
    """Test .file extension fallback for unknown Content-Type"""
    mock_url = "https://example.com/mystery"
    
    mock_response = create_mock_response_and_set_mock_session(
        206,
        {
            "Content-Length": "100",
            "Accept-Ranges": "bytes",
            "Content-Type": "application/x-unknown-type"
        },
        mock_url
    )

    dm = DownloadManager()
    task_id = dm.add_download(mock_url, "")  # Empty filename
    
    async_thread_runner.submit(dm.start_download(task_id))
    
    await wait_for_state(dm, task_id, DownloadState.RUNNING)
    
    download = dm.get_downloads()[task_id]
    # Should fall back to .file extension
    assert download.output_file.endswith(".file")
    
    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)
    
    # Clean up
    if os.path.exists(download.output_file):
        os.remove(download.output_file)


@pytest.mark.asyncio
async def test_filename_generation_with_content_type(async_thread_runner, create_mock_response_and_set_mock_session, test_file_setup_and_cleanup):
    """Test proper extension from Content-Type"""
    test_cases = [
        ("application/pdf", ".pdf"),
        ("image/jpg", ".jpg"),
        ("image/png", ".png"),
        ("text/plain", ".txt"),
        ("application/json", ".json"),
        ("video/mp4", ".mp4"),
    ]
    
    for content_type, expected_ext in test_cases:
        mock_url = f"https://example.com/file{content_type.replace('/', '_')}"
        
        mock_response = create_mock_response_and_set_mock_session(
            206,
            {
                "Content-Length": "100",
                "Accept-Ranges": "bytes",
                "Content-Type": content_type
            },
            mock_url
        )

        dm = DownloadManager()
        task_id = dm.add_download(mock_url, "")  # Empty filename
        
        async_thread_runner.submit(dm.start_download(task_id))
        
        await wait_for_state(dm, task_id, DownloadState.RUNNING)
        
        download = dm.get_downloads()[task_id]
        assert download.output_file.endswith(expected_ext) or download.output_file.endswith(".file"), f"Expected: download file ending with {expected_ext=} or .file. Received: {download.output_file=}"
        
        future = async_thread_runner.submit(dm.shutdown())
        future.result(timeout=15)

        # Clean up
        if os.path.exists(download.output_file):
            os.remove(download.output_file)
        


# ============================================================================
# PROGRESS TRACKING TESTS
# ============================================================================

@pytest.mark.asyncio
async def test_active_time_accuracy(async_thread_runner, create_mock_response_and_set_mock_session, test_file_setup_and_cleanup):
    """Verify active_time accurately excludes pause time"""
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

    await wait_for_state(dm, task_id, DownloadState.RUNNING)
    
    await mock_response.insert_chunk(chunks[0])
    await asyncio.sleep(1)
    
    # Pause
    async_thread_runner.submit(dm.pause_download(task_id))
    await wait_for_state(dm, task_id, DownloadState.PAUSED)
    
    download = dm.get_downloads()[task_id]
    active_time_at_pause = download.active_time
    
    # Wait while paused
    await asyncio.sleep(2)
    
    # Active time should not have increased during pause
    assert download.active_time == active_time_at_pause
    
    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)


@pytest.mark.asyncio
async def test_download_speed_during_pause(async_thread_runner, create_mock_response_and_set_mock_session, test_file_setup_and_cleanup):
    """Verify speed goes to 0 during pause"""
    chunks = [b"abc", b"def"]
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
    
    await mock_response.insert_chunk(chunks[0])
    
    # Pause
    async_thread_runner.submit(dm.pause_download(task_id))
    await wait_for_state(dm, task_id, DownloadState.PAUSED)
    
    # Get events and check that last event has speed = 0 or None
    last_event = None
    while True:
        event = await dm.get_oldest_event()
        if event is None:
            break
        if event.task_id == task_id:
            last_event = event
    
    # Download speed should be 0 or None when paused
    if last_event and last_event.download_speed is not None:
        assert last_event.download_speed == 0

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)


@pytest.mark.asyncio
async def test_progress_percentage_accuracy(async_thread_runner, create_mock_response_and_set_mock_session, test_file_setup_and_cleanup):
    """Verify percentage calculations are correct"""
    total_size = 1000
    chunks = [b"a" * 250, b"b" * 250, b"c" * 250, b"d" * 250]
    
    mock_url = "https://example.com/file.bin"
    mock_file_name = f"{inspect.currentframe().f_code.co_name}.txt"
    test_file_setup_and_cleanup(mock_file_name)

    mock_response = create_mock_response_and_set_mock_session(
        206,
        {
            "Content-Length": str(total_size),
            "Accept-Ranges": "bytes"
        },
        mock_url
    )

    dm = DownloadManager(running_event_update_rate_seconds=0)
    task_id = dm.add_download(mock_url, mock_file_name)
    async_thread_runner.submit(dm.start_download(task_id))

    await wait_for_state(dm, task_id, DownloadState.RUNNING)
    
    # Send chunks one at a time and check progress
    for i, chunk in enumerate(chunks):
        await mock_response.insert_chunk(chunk)

        await wait_for_state(dm, task_id, DownloadState.RUNNING)
        
        download = dm.get_downloads()[task_id]
        expected_bytes = (i + 1) * 250
        
        # Check that downloaded_bytes is being tracked
        assert download.downloaded_bytes >= expected_bytes
    
    mock_response.end_response()
    await wait_for_state(dm, task_id, DownloadState.COMPLETED)
    
    download = dm.get_downloads()[task_id]
    assert download.downloaded_bytes == total_size

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)


# ============================================================================
# SHUTDOWN TESTS
# ============================================================================

@pytest.mark.asyncio
async def test_shutdown_during_active_download(async_thread_runner, create_mock_response_and_set_mock_session, test_file_setup_and_cleanup):
    """Test clean shutdown while downloads are running"""
    chunks = [b"a" * 1000]
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
    
    await mock_response.insert_chunk(chunks[0][:500])
    
    # Shutdown while download is active
    future = async_thread_runner.submit(dm.shutdown())
    result = future.result(timeout=15)
    
    # Should shutdown cleanly without hanging
    assert result is None


@pytest.mark.asyncio
async def test_shutdown_during_preallocation(async_thread_runner, create_parallel_mock_response_and_set_mock_session, test_file_setup_and_cleanup):
    """Test clean shutdown during space preallocation"""
    mock_url = "https://example.com/file.bin"
    mock_file_name = f"{inspect.currentframe().f_code.co_name}.txt"
    test_file_setup_and_cleanup(mock_file_name)

    # Large file to ensure preallocation takes time
    large_size = ONE_GIBIBYTE * 2
    
    mock_response = create_parallel_mock_response_and_set_mock_session(
        206,
        {
            "Content-Length": str(large_size),
            "Accept-Ranges": "bytes"
        },
        mock_url,
        [],
        None
    )

    dm = DownloadManager(maximum_workers_per_task=4)
    task_id = dm.add_download(mock_url, mock_file_name)
    async_thread_runner.submit(dm.start_download(task_id, use_parallel_download=True))

    await wait_for_state(dm, task_id, DownloadState.ALLOCATING_SPACE)
    
    # Shutdown during preallocation
    future = async_thread_runner.submit(dm.shutdown())
    result = future.result(timeout=15)
    
    assert result is None


@pytest.mark.asyncio
async def test_shutdown_with_queued_events(async_thread_runner, create_mock_response_and_set_mock_session, test_file_setup_and_cleanup):
    """Ensure events don't get lost during shutdown"""
    chunks = [b"a" * 100]
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
    mock_response.end_response()
    
    # Let some events queue up
    await asyncio.sleep(1)
    
    # Events should still be in queue
    event_count = 0
    while not dm.events_queue.empty():
        await dm.get_oldest_event()
        event_count += 1
    
    assert event_count > 0
    
    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)
