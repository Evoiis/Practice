# Download Manager

A simple python download manager for windows.


# Features

### Implemented:
- Asynchronous downloads using aioHTTP
- Download controls: Start/Resume/Pausing/Delete
- Test Suite
- Simple TKinter GUI to add, control, and check progress of downloads


### Planned:
# TODO: Save metadata to file: Persist preferences and download_metadata between restarts
Let user override number of parallel workers?
# TODO: Support default download folder
# TODO: Output file auto naming rework?


### Possible Extensions (Not included to avoid scope creep):
- Auto-shutdown/Keep-on Computer Logic
- Download Speed limiter

# Usage

### App
`python main.py`

### Tests

Run `pytest` from /Download_Manager folder

Use `-k <test or test_file name>` option to choose a specific test or test_file to run
Use `--log-cli-level=INFO` option to mute debug log messages
