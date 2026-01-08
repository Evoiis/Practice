import asyncio
import pytest
import os
import logging

from dmanager.core import DownloadManager, DownloadState

def test_add_download():
    dm = DownloadManager()
    mock_url = "https://example.com/file.bin"
    mock_file_name = "test_file.bin"

    task_id = dm.add_download(mock_url, mock_file_name)
    
    download_metadata = dm.get_downloads()[task_id]

    assert download_metadata.url == mock_url
    assert download_metadata.output_file == mock_file_name

@pytest.mark.asyncio
async def test_output_file_with_invalid_characters(monkeypatch, async_thread_runner):
    pass

@pytest.mark.asyncio
async def test_invalid_test_id(async_thread_runner):
    pass

@pytest.mark.asyncio
async def test_empty_output_file_name(monkeypatch, async_thread_runner):
    pass

@pytest.mark.asyncio
async def test_input_invalid_url(monkeypatch, async_thread_runner):
    pass

@pytest.mark.asyncio
async def test_input_already_used_output_file(monkeypatch, async_thread_runner):
    pass
