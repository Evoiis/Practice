# Download Manager

A simple python download manager for windows.


# Features

### Implemented:
- Asynchronous downloads using aioHTTP
- Download controls: Start/Resume/Pausing/Delete


### Planned:
- Test Suite
- Simple TKinter GUI to add, control, and check progress of downloads
- Parallel downloads for large files
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
