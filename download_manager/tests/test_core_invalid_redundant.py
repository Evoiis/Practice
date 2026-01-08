import asyncio
import pytest
import os
import logging

# Invalid/Redundant usage

@pytest.mark.asyncio
async def test_start_running_download(async_thread_runner):
    pass

@pytest.mark.asyncio
async def test_resume_running_download(async_thread_runner):
    pass

@pytest.mark.asyncio
async def test_cancel_completed_download(async_thread_runner):
    pass

@pytest.mark.asyncio
async def test_start_completed_download(async_thread_runner):
    pass

@pytest.mark.asyncio
async def test_pause_completed_download(async_thread_runner):
    pass

@pytest.mark.asyncio
async def test_cancel_paused_download(async_thread_runner):
    pass

@pytest.mark.asyncio
async def test_start_paused_download(async_thread_runner):
    pass

@pytest.mark.asyncio
async def test_pause_paused_download(async_thread_runner):
    pass

@pytest.mark.asyncio
async def test_resume_on_download_with_no_http_range_support(async_thread_runner):
    pass