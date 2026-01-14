# Download Manager

A simple python download manager for windows.


# Features

### Implemented:
- Asynchronous downloads using aioHTTP
- Download controls: Start/Resume/Pausing/Delete
- Test Suite
- Simple TKinter GUI to add, control, and check progress of downloads


### Planned:

1. Finish parallel download tasks
# TODO: Get rid of resume_download and replace with start_download
# TODO: Parallel downloads support resume download
# TODO: Can't pause when allocating space :/

# TODO more parallel tests
# Delete running
# Resume parallel download
# Multiple different downloads at the same time
# TODO Count worker errors and handle accordingly

2. Secondary Tasks
# TODO: Save metadata to file: Persist preferences and download_metadata between restarts
# TODO: Support default download folder
# TODO: Output file auto naming rework?

# TODO more tests with delete
# Delete download and re-add the same download
# delete download with/without deleting the file
    # TODO: Weird delete and retry same url bug?

- Parallel downloads for large files
    - And new tests for parallel downloads
    - Resume support
- Save metadata to file: Persist preferences and download_metadata between restarts
    - Support default download folder

### Possible Extensions:
- Auto-shutdown/Keep-on Computer Logic

# Usage

### App
`python main.py`

### Tests

Run `pytest` from /Download_Manager folder

Use `-k <test or test_file name>` option to choose a specific test or test_file to run
Use `--log-cli-level=INFO` option to mute debug log messages
