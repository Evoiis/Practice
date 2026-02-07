import pytest
import logging
import os
import inspect

from dmanager.core import DownloadManager, DownloadState
from tests.helpers import wait_for_state

@pytest.mark.asyncio
async def test_add_download():
    dm = DownloadManager()
    mock_url = "https://example.com/file.bin"
    mock_file_name = f"{inspect.currentframe().f_code.co_name}.txt"

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
async def test_empty_output_file_name(async_thread_runner, create_mock_response_and_set_mock_session):
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

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)


    if os.path.exists(dm.get_downloads()[task_id].output_file):
        os.remove(dm.get_downloads()[task_id].output_file)


@pytest.mark.asyncio
async def test_input_invalid_urls(async_thread_runner, create_mock_response_and_set_mock_session):
    invalid_urls = [
        "htt://example.com/file.bin",           # invalid scheme
        "ftp://example.com/file.bin",           # unsupported scheme
        "http:///example.com/file.bin",         # missing netloc
        "https://",                             # empty host
        "https://exa mple.com/file.bin",        # space in host
        "https://example..com/file.bin",        # double dot in domain
        "http://?query=param",                  # missing host
        "://example.com/file.bin",              # missing scheme
    ]
    

    logging.debug("Add and start download")
    dm = DownloadManager()
    for invalid_url in invalid_urls:
        assert dm.add_download(invalid_url, "") == None, f"Unexpected url passed validation: {invalid_url}"

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)

@pytest.mark.asyncio
async def test_add_download_windows_reserved_names():
    """Test Windows reserved filename handling"""
    dm = DownloadManager()
    
    reserved = ["CON.txt", "PRN.doc", "AUX", "NUL.bin"]
    
    for name in reserved:
        task_id = dm.add_download("https://example.com/file.bin", name)
        download = dm.get_downloads()[task_id]
        # Should have been cleared
        assert download.output_file == ""

@pytest.mark.asyncio
async def test_add_download_negative_workers():
    """Test n_workers validation"""
    dm = DownloadManager()
    
    task_id = dm.add_download("https://example.com/file.bin", "file.txt", n_workers=-5)
    assert task_id is not None
    
    download = dm.get_downloads()[task_id]
    # Should have been reset to None or clamped
    assert download.parallel_metadata is None or download.parallel_metadata.n_workers > 0


@pytest.mark.asyncio
async def test_input_already_used_output_file():
    
    dm = DownloadManager()
    mock_url = "https://example.com/file.bin"
    mock_file_name = f"{inspect.currentframe().f_code.co_name}.txt"

    task_id = dm.add_download(mock_url, mock_file_name)
    
    download_metadata = dm.get_downloads()[task_id]

    assert download_metadata.url == mock_url
    assert download_metadata.output_file == mock_file_name
    
    mock_url_2 = "https://example.com/file.bin"
    mock_file_name_2 = mock_file_name

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
    assert future.result(timeout=15) is False
    
    assert await dm.start_download(invalid_task_id) is False

    assert await dm.delete_download(invalid_task_id) is False

    assert await dm.pause_download(invalid_task_id) is False

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)

@pytest.mark.asyncio
async def test_add_download_zero_workers_input():
    dm = DownloadManager()
    mock_url = "https://example.com/file.txt"
    mock_file_name = f"{inspect.currentframe().f_code.co_name}.txt"
    
    task_id = dm.add_download(mock_url, mock_file_name, n_workers=0)
    
    download_metadata = dm.get_downloads()[task_id]
    assert download_metadata.parallel_metadata is None

