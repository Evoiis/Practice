# Download Manager

A simple python download manager for Windows.


# Features

- Asynchronous downloads using aioHTTP
- Download Manager controls: Start/Resume/Pause/Delete
- Download Manager Test Suite
- Simple Stateless GUI to add, control, and check progress of downloads
- Single File Parallel Download
    - Using multiple connections to download one file faster

# Usage

### App
`python main.py`

### Tests

Run `pytest` from /Download_Manager folder

Use `-k <test or test_file name>` option to choose a specific test or test_file to run
Use `--log-cli-level=INFO` option to mute debug log messages


## Possible Extensions:
- Auto-shutdown/Keep-on Computer Logic
- Download Speed limiter
- Save metadata to file: Persist preferences and download_metadata between restarts
- GUI Elements to edit preferences
    - Let user override number of parallel workers
    - Support default download folder
