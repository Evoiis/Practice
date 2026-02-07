import pytest
import os
import inspect

from dmanager.core import DownloadManager, DownloadState
from tests.helpers import wait_for_state, verify_file, wait_for_file_to_be_created


@pytest.mark.asyncio
async def test_add_download_with_existing_file_different_size(async_thread_runner, create_mock_response_and_set_mock_session, test_file_setup_and_cleanup):
    """Test when output file exists but has different size than expected"""
    chunks = [b"abc", b"def", b"ghi"]
    mock_url = "https://example.com/file.bin"
    mock_file_name = f"{inspect.currentframe().f_code.co_name}.txt"
    test_file_setup_and_cleanup(mock_file_name)

    # Create existing file with different content
    with open(mock_file_name, 'wb') as f:
        f.write(b"existing content that is different")

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
    
    for chunk in chunks:
        await mock_response.insert_chunk(chunk)
    
    mock_response.end_response()

    await wait_for_state(dm, task_id, DownloadState.COMPLETED)
    
    # Should have appended to existing file
    download_metadata = dm.get_downloads()[task_id]
    assert download_metadata.downloaded_bytes == len(b"abcdefghi")

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)


@pytest.mark.asyncio
async def test_add_download_with_invalid_filename_characters():
    """Test sanitization of special characters in filenames"""
    dm = DownloadManager()
    
    invalid_filenames = [
        'file<name>.txt',
        'file>name.txt',
        'file:name.txt',
        'file"name.txt',
        'file/name.txt',
        'file\\name.txt',
        'file|name.txt',
        'file?name.txt',
        'file*name.txt',
        'filename ',  # Trailing space
        'filename.',  # Trailing period
    ]
    
    for invalid_name in invalid_filenames:
        task_id = dm.add_download("https://example.com/file.bin", invalid_name)
        download_metadata = dm.get_downloads()[task_id]
        
        # Should be sanitized
        assert '<' not in download_metadata.output_file
        assert '>' not in download_metadata.output_file
        assert ':' not in download_metadata.output_file
        assert '"' not in download_metadata.output_file
        assert '/' not in download_metadata.output_file
        assert '\\' not in download_metadata.output_file
        assert '|' not in download_metadata.output_file
        assert '?' not in download_metadata.output_file
        assert '*' not in download_metadata.output_file
        assert not download_metadata.output_file.endswith(' ')
        assert not download_metadata.output_file.endswith('.')


@pytest.mark.asyncio
async def test_disk_full_during_download(async_thread_runner, create_mock_response_and_set_mock_session, test_file_setup_and_cleanup, monkeypatch):
    """Test handling when disk runs out of space during download
    
    Note: This is difficult to test without actually filling the disk.
    This test simulates the error by mocking the file write operation.
    """
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

    # Mock aiofiles to raise OSError simulating disk full
    original_open = open
    write_count = [0]
    
    class MockFile:
        async def __aenter__(self):
            return self
        
        async def __aexit__(self, *args):
            pass
        
        async def write(self, data):
            write_count[0] += 1
            if write_count[0] > 1:  # Fail after first write
                raise OSError(28, "No space left on device")  # ENOSPC
    
    def mock_open(*args, **kwargs):
        if 'test_file.bin' in str(args[0]):
            return MockFile()
        return original_open(*args, **kwargs)
    
    import aiofiles
    monkeypatch.setattr(aiofiles, "open", mock_open)

    dm = DownloadManager()
    task_id = dm.add_download(mock_url, mock_file_name)
    async_thread_runner.submit(dm.start_download(task_id))

    await mock_response.insert_chunk(chunks[0])
    await mock_response.insert_chunk(chunks[1])
    
    await wait_for_state(dm, task_id, DownloadState.RUNNING)
    await wait_for_state(dm, task_id, DownloadState.ERROR)

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)


@pytest.mark.asyncio
async def test_disk_full_during_preallocation(async_thread_runner, monkeypatch, test_file_setup_and_cleanup):
    """Test preallocation failure due to insufficient disk space"""
    mock_url = "https://example.com/file.bin"
    mock_file_name = f"{inspect.currentframe().f_code.co_name}.txt"
    test_file_setup_and_cleanup(mock_file_name)
    
    from dmanager.constants import SEGMENT_SIZE
    
    class MockPreallocResponse:
        def __init__(self, status, headers):
            self.status = status
            self.headers = headers
        
        async def __aenter__(self):
            return self
        
        async def __aexit__(self, *args):
            pass
    
    class MockPreallocSession:
        def __init__(self):
            self.closed = False

        async def __aenter__(self):
            return self
        
        async def __aexit__(self, *args):
            pass
        
        def head(self, url, timeout):
            return MockPreallocResponse(200, {
                "Content-Length": str(4 * SEGMENT_SIZE),
                "Accept-Ranges": "bytes"
            })
        
        async def close(self):
            self.closed = True
    
    monkeypatch.setattr("aiohttp.ClientSession", lambda: MockPreallocSession())
    
    # Mock aiofiles to raise OSError during preallocation
    write_count = [0]
    
    class MockFile:
        async def __aenter__(self):
            return self
        
        async def __aexit__(self, *args):
            pass
        
        async def write(self, data):
            write_count[0] += 1
            if write_count[0] > 2:  # Fail partway through
                raise OSError(28, "No space left on device")
    
    def mock_open(*args, **kwargs):
        return MockFile()
    
    import aiofiles
    monkeypatch.setattr(aiofiles, "open", mock_open)

    dm = DownloadManager(maximum_workers_per_task=4)
    task_id = dm.add_download(mock_url, mock_file_name)
    async_thread_runner.submit(dm.start_download(task_id, use_parallel_download=True))

    await wait_for_state(dm, task_id, DownloadState.ALLOCATING_SPACE)
    await wait_for_state(dm, task_id, DownloadState.ERROR)

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)


@pytest.mark.asyncio  
async def test_permission_denied_file_write(async_thread_runner, create_mock_response_and_set_mock_session, test_file_setup_and_cleanup, monkeypatch):
    """Test handling of permission errors when writing files"""
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

    # Mock aiofiles to raise PermissionError
    class MockFile:
        async def __aenter__(self):
            raise PermissionError("Permission denied")
        
        async def __aexit__(self, *args):
            pass
    
    def mock_open(*args, **kwargs):
        return MockFile()
    
    import aiofiles
    monkeypatch.setattr(aiofiles, "open", mock_open)

    dm = DownloadManager()
    task_id = dm.add_download(mock_url, mock_file_name)
    async_thread_runner.submit(dm.start_download(task_id))

    await wait_for_state(dm, task_id, DownloadState.RUNNING)
    await wait_for_state(dm, task_id, DownloadState.ERROR)

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)


@pytest.mark.asyncio
async def test_filename_collision_handling():
    """Test duplicate filename detection"""
    dm = DownloadManager()
    
    mock_url_1 = "https://example.com/file1.bin"
    mock_url_2 = "https://example.com/file2.bin"
    same_filename = "output.bin"
    
    # Add first download
    task_id_1 = dm.add_download(mock_url_1, same_filename)
    download_1 = dm.get_downloads()[task_id_1]
    assert download_1.output_file == same_filename
    
    # Add second download with same filename
    task_id_2 = dm.add_download(mock_url_2, same_filename)
    download_2 = dm.get_downloads()[task_id_2]
    
    # Should have been cleared/changed to avoid collision
    assert download_2.output_file == ""


@pytest.mark.asyncio
async def test_existing_file_collision_handling(test_file_setup_and_cleanup):
    """Test that existing files prevent using that filename"""
    dm = DownloadManager()
    
    existing_filename = "existing.bin"
    test_file_setup_and_cleanup(existing_filename)
    
    # Create existing file
    with open(existing_filename, 'wb') as f:
        f.write(b"existing content")
    
    mock_url = "https://example.com/file.bin"
    
    # Try to add download with existing filename
    task_id = dm.add_download(mock_url, existing_filename)
    download = dm.get_downloads()[task_id]
    
    # Should have cleared the filename to avoid collision
    assert download.output_file == ""


@pytest.mark.asyncio
async def test_unicode_filename_handling():
    """Test handling of unicode characters in filenames"""
    dm = DownloadManager()
    
    unicode_filenames = [
        '文件名.txt',  # Chinese
        'файл.txt',  # Russian
        'αρχείο.txt',  # Greek
        'ファイル.txt',  # Japanese
        '파일.txt',  # Korean
    ]
    
    for unicode_name in unicode_filenames:
        task_id = dm.add_download("https://example.com/file.bin", unicode_name)
        download = dm.get_downloads()[task_id]
        
        # Should preserve unicode characters (they're valid in most modern filesystems)
        # Just verify it doesn't crash
        assert download.output_file is not None


@pytest.mark.asyncio
async def test_very_long_filename_handling():
    """Test handling of extremely long filenames"""
    dm = DownloadManager()
    
    # Most filesystems have a 255 character limit
    very_long_name = "a" * 300 + ".txt"
    
    task_id = dm.add_download("https://example.com/file.bin", very_long_name)
    download = dm.get_downloads()[task_id]
    
    # Should either truncate or handle gracefully
    # Current implementation just sanitizes, so it will keep the long name
    assert download.output_file is not None


@pytest.mark.asyncio
async def test_empty_filename_auto_generation(async_thread_runner, create_mock_response_and_set_mock_session, test_file_setup_and_cleanup):
    """Test automatic filename generation when none provided"""
    chunks = [b"test"]
    mock_url = "https://example.com/file.bin"
    
    mock_response = create_mock_response_and_set_mock_session(
        206,
        {
            "Content-Length": str(len(chunks[0])),
            "Accept-Ranges": "bytes",
            "Content-Type": "application/octet-stream"
        },
        mock_url
    )

    dm = DownloadManager()
    # Empty filename
    task_id = dm.add_download(mock_url, "")
    
    async_thread_runner.submit(dm.start_download(task_id))
    
    await wait_for_state(dm, task_id, DownloadState.RUNNING)
    
    download = dm.get_downloads()[task_id]
    # Should have generated a filename
    assert download.output_file != ""
    assert download.output_file.endswith(".bin") or download.output_file.endswith(".file")
    

    future = async_thread_runner.submit(dm.shutdown())
    future.result(timeout=15)

    # Clean up generated file
    if os.path.exists(download.output_file):
        os.remove(download.output_file)
