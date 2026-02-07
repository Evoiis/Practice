import logging
import asyncio
import os
import pytest
import aiohttp
import aiofiles
import filecmp

from dmanager.core import DownloadManager, DownloadState
from dmanager.constants import CHUNK_SIZE
from tests.helpers import wait_for_state, wait_for_file_to_be_created

TEST_URL = "https://ia601000.us.archive.org/7/items/youtube-5sskbSvha9M/Error_Correction_-_Computerphile-5sskbSvha9M.mp4" # ~95 MB

HELPER_FILE_NAME = "test_helper_file.mp4"


@pytest.fixture(scope="session", autouse=True)
def cleanup_helper_file():
    yield

    if os.path.exists(HELPER_FILE_NAME):
        os.remove(HELPER_FILE_NAME)

async def download_file():
    logging.debug("Test helper, Downloading file.")
    file_name = HELPER_FILE_NAME
    url = TEST_URL

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            assert resp.status == 200

            async with aiofiles.open(file_name, "wb") as file:
                async for chunk in resp.content.iter_chunked(CHUNK_SIZE):
                    await file.write(chunk)
    
    assert os.path.exists(file_name)
    return


@pytest.mark.parametrize(
    "use_parallel",
    [True, False]
)
@pytest.mark.asyncio
async def test_download_real_file(async_thread_runner, test_file_setup_and_cleanup, cleanup_helper_file, use_parallel):
    """Download a real file from the internet and verify correctness"""
    
    test_url = TEST_URL
    n_workers = 4
    file_name = "test_single.mp4"
    if use_parallel:
        file_name = "test_parallel.mp4"

    test_file_setup_and_cleanup(file_name)
    

    try:
        async with aiohttp.ClientSession() as session:
            async with session.head(test_url, timeout=aiohttp.ClientTimeout(connect=10)) as resp:
                if resp.status >= 400:
                    pytest.skip(f"Test URL not accessible: {resp.status}")
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        pytest.skip(f"Network not available: {e}")
    

    if not os.path.exists(HELPER_FILE_NAME):
        await download_file()

    dm = DownloadManager(continue_on_error=False, maximum_workers_per_task=n_workers)
    task_id = dm.add_download(test_url, file_name)
    
    async_thread_runner.submit(dm.start_download(task_id, use_parallel_download=use_parallel))

    if use_parallel:
        for _ in range(n_workers + 1):
            await wait_for_state(dm, task_id, DownloadState.COMPLETED, timeout_sec=180)
    else:
        await wait_for_state(dm, task_id, DownloadState.COMPLETED, timeout_sec=180)
    
    assert os.path.exists(file_name)
    
    download = dm.get_downloads()[task_id]
    assert download.downloaded_bytes > 0
    assert download.state == DownloadState.COMPLETED

    assert filecmp.cmp(HELPER_FILE_NAME, file_name, shallow=False)
    
    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)

@pytest.mark.parametrize(
    "use_parallel",
    [True, False]
)
@pytest.mark.asyncio
async def test_download_real_file_with_pause(async_thread_runner, test_file_setup_and_cleanup, cleanup_helper_file, use_parallel):
    """Download a real file from the internet and verify correctness"""

    test_url = TEST_URL
    n_workers = 4
    file_name = "test_single.mp4"
    if use_parallel:
        file_name = "test_parallel.mp4"
    test_file_setup_and_cleanup(file_name)
    

    try:
        async with aiohttp.ClientSession() as session:
            async with session.head(test_url, timeout=aiohttp.ClientTimeout(connect=10)) as resp:
                if resp.status >= 400:
                    pytest.skip(f"Test URL not accessible: {resp.status}")
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        pytest.skip(f"Network not available: {e}")
    

    if not os.path.exists(HELPER_FILE_NAME):
        await download_file()

    dm = DownloadManager(continue_on_error=False, maximum_workers_per_task=n_workers)
    task_id = dm.add_download(test_url, file_name)
    
    async_thread_runner.submit(dm.start_download(task_id, use_parallel_download=use_parallel))

    await wait_for_state(dm, task_id, DownloadState.RUNNING)

    async_thread_runner.submit(dm.pause_download(task_id))

    await wait_for_state(dm, task_id, DownloadState.PAUSED)

    async_thread_runner.submit(dm.start_download(task_id))

    if use_parallel:
        for _ in range(n_workers + 1):
            await wait_for_state(dm, task_id, DownloadState.COMPLETED, timeout_sec=180)
    else:
        await wait_for_state(dm, task_id, DownloadState.COMPLETED, timeout_sec=180)
    
    assert os.path.exists(file_name)
    
    download = dm.get_downloads()[task_id]
    assert download.downloaded_bytes > 0
    assert download.state == DownloadState.COMPLETED

    assert filecmp.cmp(HELPER_FILE_NAME, file_name, shallow=False)
    
    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)