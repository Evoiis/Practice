import asyncio
import pytest
import os
import logging

@pytest.mark.asyncio
async def test_start_in_running_state(async_thread_runner):
    pass

@pytest.mark.asyncio
async def test_start_in_completed_state(async_thread_runner):
    pass

@pytest.mark.asyncio
async def test_start_in_paused_state(async_thread_runner):
    pass

@pytest.mark.asyncio
async def test_resume_in_running_state(async_thread_runner):
    pass

@pytest.mark.asyncio
async def test_resume_in_pending_state(async_thread_runner):
    pass

@pytest.mark.asyncio
async def test_resume_completed_download(async_thread_runner):
    pass

@pytest.mark.asyncio
async def test_pause_completed_download(async_thread_runner):
    pass

@pytest.mark.asyncio
async def test_pause_in_paused_state(async_thread_runner):
    pass

@pytest.mark.asyncio
async def test_pause_in_pending_state(async_thread_runner):
    pass

@pytest.mark.asyncio
async def test_pause_in_error_state(async_thread_runner):
    pass


