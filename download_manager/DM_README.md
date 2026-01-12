# Download Manager

A simple python download manager for windows.


# Features

### Implemented:
- Asynchronous downloads using aioHTTP
- Download controls: Start/Resume/Pausing/Delete
- Test Suite
- Simple TKinter GUI to add, control, and check progress of downloads


### Planned:
# TODO: Show worker progress in GUI
# TODO: Support default download folder
# TODO: Save metadata to file: Persist preferences and download_metadata between restarts

# TODO: Get rid of resume_download and replace with start_downlaod
# TODO: Parallel downloads support resume download
# TODO Count worker errors and handle accordingly
- Parallel downloads for large files
    - And new test for parallel downloads
    - Resume support
- Save metadata to file: Persist preferences and download_metadata between restarts
    - Support default download folder

### Possible Extensions:
- Auto-shutdown/Keep-on Computer Logic

# Usage

### App


### Tests

Run `pytest` from /Download_Manager folder

Use `-k <test or test_file name>` option to choose a specific test or test_file to run
Use `--log-cli-level=INFO` option to mute debug log messages
