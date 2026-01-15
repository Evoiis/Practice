import pytest
import logging
import os

from dmanager.core import DownloadManager, DownloadState
from tests.helpers import wait_for_state

@pytest.mark.asyncio
async def test_add_download():
    dm = DownloadManager()
    mock_url = "https://example.com/file.bin"
    mock_file_name = "test_file.bin"

    task_id = dm.add_download(mock_url, mock_file_name)
    
    download_metadata = dm.get_downloads()[task_id]

    assert download_metadata.url == mock_url
    assert download_metadata.output_file == mock_file_name

    await dm.shutdown()

@pytest.mark.asyncio
async def test_output_file_with_invalid_characters():
    dm = DownloadManager()
    mock_url = "https://example.com/file.bin"
    mock_file_names = {
        "report:Q1.txt":        "reportQ1.txt",
        "budget/2026.xlsx":     "budget2026.xlsx",
        "notes*final?.md":      "notesfinal.md",
        'quote"test".txt':      "quotetest.txt",
        "data<raw>.csv":        "dataraw.csv",
        "pipe|name.log":        "pipename.log",
        "trailingspace.txt ":   "trailingspace.txt",
        "trailingdot.":         "trailingdot",
        "mix<>:\"/\\|?*.txt":   "mix.txt",
        "   .":                 "",
        "hello?.mp4":           "hello.mp4"
    }

    for invalid_name, fixed_name in mock_file_names.items():
        task_id = dm.add_download(mock_url, invalid_name)
        download_metadata = dm.get_downloads()[task_id]
        logging.debug(f"{download_metadata.output_file}, {fixed_name}, {download_metadata.output_file == fixed_name}")
        assert download_metadata.url == mock_url
        assert download_metadata.output_file == fixed_name, f"Failed to change invalid file name to fixed file name. Received: {invalid_name}, Expected: {fixed_name}"

    await dm.shutdown()
    

@pytest.mark.asyncio
async def test_empty_output_file_name(test_file_setup_and_cleanup, async_thread_runner, create_mock_response_and_set_mock_session):
    chunks = [b"abc", b"def", b"ghi"]
    mock_url = "https://example.com/file.bin"

    create_mock_response_and_set_mock_session(
        206,
        {
            "Content-Length": str(sum(len(c) for c in chunks)),
            "Accept-Ranges": "bytes",
            "ETag": '"ETAGSTRING"',
            "Content-Type": "video/mp4"
        },
        mock_url
    )    

    logging.debug("Add and start download")
    dm = DownloadManager()
    task_id = dm.add_download(mock_url, "")
    async_thread_runner.submit(dm.start_download(task_id))
    await wait_for_state(dm, task_id, DownloadState.RUNNING)

    logging.debug(dm.get_downloads())
    assert dm.get_downloads()[task_id].output_file.endswith(".mp4")

    if os.path.exists(dm.get_downloads()[task_id].output_file):
        os.remove(dm.get_downloads()[task_id].output_file)

    await dm.shutdown()


@pytest.mark.asyncio
async def test_input_invalid_url(async_thread_runner):
    invalid_url = "http://exa mple.com"
    # mock_file_name = "test_file.bin"
    # test_file_setup_and_cleanup(mock_file_name)

    logging.debug("Add and start download")
    dm = DownloadManager()
    task_id = dm.add_download(invalid_url, "")
    future = async_thread_runner.submit(dm.start_download(task_id))

    assert not future.result()
    await wait_for_state(dm, task_id, DownloadState.ERROR)

    await dm.shutdown()
    

@pytest.mark.asyncio
async def test_input_already_used_output_file():
    
    dm = DownloadManager()
    mock_url = "https://example.com/file.bin"
    mock_file_name = "test_file.bin"

    task_id = dm.add_download(mock_url, mock_file_name)
    
    download_metadata = dm.get_downloads()[task_id]

    assert download_metadata.url == mock_url
    assert download_metadata.output_file == mock_file_name
    
    mock_url_2 = "https://example.com/file.bin"
    mock_file_name_2 = "test_file.bin"

    task_id_2 = dm.add_download(mock_url_2, mock_file_name_2)
    
    download_metadata = dm.get_downloads()[task_id_2]

    assert download_metadata.url == mock_url_2
    assert download_metadata.output_file == ""

    await dm.shutdown()

@pytest.mark.asyncio
async def test_invalid_test_id(async_thread_runner):
    invalid_task_id = 50

    dm = DownloadManager()
    future = async_thread_runner.submit(dm.start_download(invalid_task_id))
    assert future.result() is False
    
    assert await dm.start_download(invalid_task_id) is False

    assert await dm.delete_download(invalid_task_id) is False

    assert await dm.pause_download(invalid_task_id) is False

    await dm.shutdown()    
