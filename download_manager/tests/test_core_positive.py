import asyncio
import pytest
import os
import logging

@pytest.mark.asyncio
async def test_add_and_start_download(async_thread_runner, create_mock_response_and_set_mock_session):
    from dmanager.core import DownloadManager, DownloadState

    # Prepare mock session and response
    chunks = [b"abc", b"def", b"ghi"]
    mock_file_name = "file.bin"
    mock_url = "https://example.com/file.bin"
    
    mock_res = create_mock_response_and_set_mock_session(
        206,
        {
            "Content-Length": str(sum(len(c) for c in chunks)),
            "Accept-Ranges": "bytes"
        },
        mock_url
    )    

    logging.debug("Add and start download")
    dm = DownloadManager()
    task_id = dm.add_download(mock_url, mock_file_name)
    async_thread_runner.submit(dm.start_download(task_id))

    logging.debug(f"{task_id=}")

    received_running_event = False

    for chunk in chunks:
        await mock_res.insert_chunk(chunk)
    
    mock_res.end_response()

    logging.debug("Consume Events")
    counter = 0
    while True:
        await asyncio.sleep(1)
        counter += 1
        event = await dm.get_latest_event()
        logging.debug(f"{event=}")

        if event is None:
            continue

        if event.state == DownloadState.RUNNING and event.task_id == task_id:
            received_running_event = True
        
        if event.state == DownloadState.COMPLETED and event.task_id == task_id:
            assert(received_running_event, "Expected to receive download running event before completed event")
            break

        assert(counter < 20, "Download Manager took too long to respond!")
    
    download_metadata = dm.get_downloads()[1]

    logging.debug(download_metadata)

    assert(download_metadata.url == mock_url)
    assert(download_metadata.output_file == mock_file_name)
    assert(download_metadata.file_size_bytes == 9)
    assert(download_metadata.downloaded_bytes == 9)

    expected_text = "abcdefghi"
    with open(mock_file_name) as f:
        file_text = f.read()
        assert(file_text == expected_text, f"Downloaded file text did not match expected.\nDownloaded: {file_text}\nExpected: {expected_text}")

    if os.path.exists(mock_file_name):
        os.remove(mock_file_name)


@pytest.mark.asyncio
async def test_pause_download(monkeypatch, async_thread_runner):
    # Set up mocks with slow task
    # MockResponse(
    #     chunks=[b"abc", b"def", b"ghi"], 
    #     status=206, 
    #     headers={
    #         "Content-Length": "9",
    #         "Accept-Ranges": "bytes"
    #     },
    #     chunk_iter_wait=2.0
    # )

    # Add and start download

    # Call pause

    # Check task is paused
    pass

# TODO: Test dmanager._tasks is cleaned up properly

@pytest.mark.asyncio
async def test_resume_download(monkeypatch, async_thread_runner):

    # Set up mocks

    # 

    pass


@pytest.mark.asyncio
async def test_cancel_download(monkeypatch, async_thread_runner):
    pass





# TODO: Error path tests
# Chunk write Failure
# Asyncio/Aiohttp errors
    # Network disconnect
    # Server Errors 500

# TODO: Concurrency tests
"""
Put tests like:

Tests asserting event ordering

Tests asserting _tasks cleanup

Pause/resume timing correctness

Partial-write verification

Race-condition-related tests
"""


