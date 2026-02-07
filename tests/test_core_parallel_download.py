import asyncio
import os
import pytest
import logging
import inspect

from dmanager.core import DownloadManager, DownloadState, DownloadMetadata
from tests.helpers import wait_for_state, verify_file, wait_for_file_to_be_created, wait_for_multiple_states


@pytest.mark.parametrize(
    "n_workers",
    [1, 4]
)
@pytest.mark.asyncio
async def test_n_worker_parallel_download_coroutine(async_thread_runner, create_parallel_mock_response_and_set_mock_session, test_file_setup_and_cleanup, n_workers):
    logging.info(f"Running with {n_workers=}")
    
    segment_size = 16 * 1024
    dm = DownloadManager(maximum_workers_per_task=n_workers, parallel_download_segment_size=segment_size)

    mock_url = "https://example.com/file.txt"
    mock_file_name = f"{inspect.currentframe().f_code.co_name}.txt"
    test_file_setup_and_cleanup(mock_file_name)


    data = {
        str(segment_size - 1):  list(b"a" * segment_size ),
        str((segment_size * 2) - 1):  list(b"b" * segment_size ),
        str((segment_size * 3) - 1):  list(b"c" * segment_size ),
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

    for _ in range(n_workers + 1):
        await wait_for_state(dm, task_id, DownloadState.COMPLETED, 120)

    verify_file(
        mock_file_name,
        "".join(bytes(x).decode('ascii') for x in data.values())
    )

    
    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)


@pytest.mark.asyncio
async def test_parallel_download_pause(async_thread_runner, create_parallel_mock_response_and_set_mock_session, test_file_setup_and_cleanup):
    n_workers = 4
    segment_size = 16 * 1024
    dm = DownloadManager(maximum_workers_per_task=n_workers, parallel_download_segment_size=segment_size)

    mock_url = "https://example.com/file.txt"
    mock_file_name = f"{inspect.currentframe().f_code.co_name}.txt"
    test_file_setup_and_cleanup(mock_file_name)

    
    data = {
        str(segment_size - 1):  list(b"a" * segment_size ),
        str((segment_size * 2) - 1):  list(b"b" * segment_size ),
        str((segment_size * 3) - 1):  list(b"c" * segment_size ),
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
        mock_response.set_range_end_n_send(key, 12)

    task_id = dm.add_download(mock_url, mock_file_name)

    async_thread_runner.submit(dm.start_download(task_id, use_parallel_download=True)) 
    
    await wait_for_state(dm, task_id, DownloadState.ALLOCATING_SPACE)
    await wait_for_state(dm, task_id, DownloadState.RUNNING)

    async_thread_runner.submit(dm.pause_download(task_id)) 

    for _ in range(n_workers + 1):
        await wait_for_state(dm, task_id, DownloadState.PAUSED)
    
    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)


@pytest.mark.asyncio
async def test_parallel_download_resume(async_thread_runner, create_parallel_mock_response_and_set_mock_session, test_file_setup_and_cleanup):
    n_workers = 4
    segment_size = 16 * 1024
    dm = DownloadManager(maximum_workers_per_task=n_workers, parallel_download_segment_size=segment_size)

    mock_url = "https://example.com/file.txt"
    mock_file_name = f"{inspect.currentframe().f_code.co_name}.txt"
    test_file_setup_and_cleanup(mock_file_name)

    
    data = {
        str(segment_size - 1):  list(b"a" * segment_size ),
        str((segment_size * 2) - 1):  list(b"b" * segment_size ),
        str((segment_size * 3) - 1):  list(b"c" * segment_size ),
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
        mock_response.set_range_end_n_send(key, segment_size//2)
    
    task_id = dm.add_download(mock_url, mock_file_name)

    async_thread_runner.submit(dm.start_download(task_id, use_parallel_download=True)) 
    
    await wait_for_state(dm, task_id, DownloadState.ALLOCATING_SPACE)
    await wait_for_state(dm, task_id, DownloadState.RUNNING)

    async_thread_runner.submit(dm.pause_download(task_id)) 

    for _ in range(n_workers):
        await wait_for_state(dm, task_id, DownloadState.PAUSED)

    async_thread_runner.submit(dm.start_download(task_id, use_parallel_download=True))

    for key in data:
        mock_response.set_range_end_n_send(key, segment_size)
        mock_response.set_range_end_done(key)

    for _ in range(n_workers):
        await wait_for_state(dm, task_id, DownloadState.COMPLETED)
    
    verify_file(
        mock_file_name,
        "".join(bytes(x).decode('ascii') for x in data.values())
    )
    
    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)



@pytest.mark.asyncio
async def test_parallel_download_delete_running(async_thread_runner, create_parallel_mock_response_and_set_mock_session, test_file_setup_and_cleanup):
    n_workers = 4
    segment_size = 16 * 1024
    dm = DownloadManager(maximum_workers_per_task=n_workers, parallel_download_segment_size=segment_size)

    mock_url = "https://example.com/file.txt"
    mock_file_name = f"{inspect.currentframe().f_code.co_name}.txt"
    test_file_setup_and_cleanup(mock_file_name)

    
    
    data = {
        str(segment_size - 1):  list(b"a" * segment_size ),
        str((segment_size * 2) - 1):  list(b"b" * segment_size ),
        str((segment_size * 3) - 1):  list(b"c" * segment_size ),
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
        mock_response.set_range_end_n_send(key, segment_size//2)
    
    task_id = dm.add_download(mock_url, mock_file_name)

    async_thread_runner.submit(dm.start_download(task_id, use_parallel_download=True)) 
    
    await wait_for_state(dm, task_id, DownloadState.ALLOCATING_SPACE)
    await wait_for_state(dm, task_id, DownloadState.RUNNING)

    async_thread_runner.submit(dm.delete_download(task_id, remove_file=False))

    await wait_for_state(dm, task_id, DownloadState.DELETED)

    assert task_id not in dm._downloads
    assert task_id not in dm._task_pools

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)

@pytest.mark.asyncio
async def test_parallel_download_delete_completed(async_thread_runner, create_parallel_mock_response_and_set_mock_session, test_file_setup_and_cleanup):
    n_workers = 4
    segment_size = 16 * 1024
    dm = DownloadManager(maximum_workers_per_task=n_workers, parallel_download_segment_size=segment_size)

    mock_url = "https://example.com/file.txt"
    mock_file_name = f"{inspect.currentframe().f_code.co_name}.txt"
    test_file_setup_and_cleanup(mock_file_name)

    data = {
        str(segment_size - 1):        list(b"a" * segment_size ),
        str((segment_size * 2) - 1):  list(b"b" * segment_size ),
        str((segment_size * 3) - 1):  list(b"c" * segment_size ),
        str((segment_size * 4) - 1):  list(b"d" * segment_size)
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
    await wait_for_state(dm, task_id, DownloadState.COMPLETED)

    await wait_for_multiple_states(
        dm,
        {
            (task_id, DownloadState.COMPLETED): n_workers   
        }
    )

    async_thread_runner.submit(dm.delete_download(task_id, remove_file=False))

    await wait_for_state(dm, task_id, DownloadState.DELETED)

    assert task_id not in dm._downloads
    assert task_id not in dm._task_pools

    verify_file(
        mock_file_name,
        "".join(bytes(x).decode('ascii') for x in data.values())
    )

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)

@pytest.mark.asyncio
async def test_multiple_simultaneous_parallel_download(async_thread_runner, create_multiple_parallel_mock_response_and_mock_sessions, test_multiple_file_setup_and_cleanup):
    n_workers = 4
    segment_size = 16 * 1024
    dm = DownloadManager(maximum_workers_per_task=n_workers, parallel_download_segment_size=segment_size)

    mock_url = "https://example.com/file.txt"
    mock_file_name = f"{inspect.currentframe().f_code.co_name}.txt"

    mock_url_2 = "https://example.com/file_2.txt"
    mock_file_name_2 = "test_file_2.txt"
    test_multiple_file_setup_and_cleanup([mock_file_name, mock_file_name_2])

    
    data = {
        str(segment_size - 1):  list(b"a" * segment_size ),
        str((segment_size * 2) - 1):  list(b"b" * segment_size ),
        str((segment_size * 3) - 1):  list(b"c" * segment_size ),
        str((segment_size * 4) - 1): list(b"d" * segment_size)
    }

    mock_responses = create_multiple_parallel_mock_response_and_mock_sessions({
        mock_url: {
            "status": 206,
            "headers": {"Content-Length": 4 * segment_size, "Accept-Ranges": "bytes"},
            "range_ends": list(data.keys()),
            "data": data
        },
        mock_url_2: {
            "status": 206,
            "headers": {"Content-Length": 4 * segment_size, "Accept-Ranges": "bytes"},
            "range_ends": list(data.keys()),
            "data": data
        },
    })

    for key in data:
        mock_responses[mock_url].set_range_end_n_send(key, segment_size)
        mock_responses[mock_url_2].set_range_end_n_send(key, segment_size)
        mock_responses[mock_url].set_range_end_done(key)
        mock_responses[mock_url_2].set_range_end_done(key)
    
    task_id = dm.add_download(mock_url, mock_file_name)
    task_id_2 = dm.add_download(mock_url_2, mock_file_name_2)

    async_thread_runner.submit(dm.start_download(task_id, use_parallel_download=True))
    async_thread_runner.submit(dm.start_download(task_id_2, use_parallel_download=True))

    await wait_for_multiple_states(
        dm,
        {
            (task_id, DownloadState.COMPLETED): n_workers,
            (task_id_2, DownloadState.COMPLETED): n_workers
        }
    )

    wait_for_file_to_be_created(mock_file_name)
    wait_for_file_to_be_created(mock_file_name_2)

    verify_file(
        mock_file_name,
        "".join(bytes(x).decode('ascii') for x in data.values())
    )

    verify_file(
        mock_file_name_2,
        "".join(bytes(x).decode('ascii') for x in data.values())
    )

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)

@pytest.mark.asyncio
async def test_core_file_preallocation(test_file_setup_and_cleanup):

    n_workers = 4
    segment_size = 16 * 1024
    dm = DownloadManager(maximum_workers_per_task=n_workers, parallel_download_segment_size=segment_size)

    mock_file_name = f"{inspect.currentframe().f_code.co_name}.txt"
    mock_file_total_size = 9000
    test_file_setup_and_cleanup(mock_file_name)
    
    download = DownloadMetadata(
        1,
        "",
        mock_file_name,
        file_size_bytes=mock_file_total_size,

    )
    await dm._preallocate_file_space_on_disk(download)

    wait_for_file_to_be_created(mock_file_name)

    assert mock_file_total_size == os.path.getsize(mock_file_name)


@pytest.mark.asyncio
async def test_parallel_pause_during_preallocate(async_thread_runner, create_parallel_mock_response_and_set_mock_session, test_file_setup_and_cleanup):
    n_workers = 4
    segment_size = 16 * 1024
    dm = DownloadManager(maximum_workers_per_task=n_workers, parallel_download_segment_size=segment_size)

    mock_url = "https://example.com/file.txt"
    mock_file_name = f"{inspect.currentframe().f_code.co_name}.txt"
    test_file_setup_and_cleanup(mock_file_name)
    mock_file_total_size = 6442450944 # 6 GIBIBYTES
    
    data = None

    create_parallel_mock_response_and_set_mock_session(
        206,
        {
            "Content-Length": mock_file_total_size,
            "Accept-Ranges": "bytes"
        },
        mock_url,
        [],
        data
    )
    
    task_id = dm.add_download(mock_url, mock_file_name)

    async_thread_runner.submit(dm.start_download(task_id, use_parallel_download=True)) 

    await wait_for_state(dm, task_id, DownloadState.ALLOCATING_SPACE)
    wait_for_file_to_be_created(mock_file_name)

    future = async_thread_runner.submit(dm.pause_download(task_id))

    assert future.result(timeout=15) == True
    await wait_for_state(dm, task_id, DownloadState.PAUSED)
    
    # Assumption: CPU/Disk won't be able to allocate 6 GIBIBYTES by this point
    current_file_size = os.path.getsize(mock_file_name)
    assert current_file_size != mock_file_total_size

    await asyncio.sleep(10)

    assert current_file_size == os.path.getsize(mock_file_name)

    async_thread_runner.submit(dm.start_download(task_id, use_parallel_download=True)) 
    
    await wait_for_state(dm, task_id, DownloadState.ALLOCATING_SPACE)
    await asyncio.sleep(1)

    async_thread_runner.submit(dm.pause_download(task_id))

    await wait_for_state(dm, task_id, DownloadState.PAUSED)

    assert current_file_size < os.path.getsize(mock_file_name)
    
    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)
