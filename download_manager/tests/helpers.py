import asyncio
import time
import os
import logging

DEFAULT_TIMEOUT = 20

async def wait_for_state(dm, task_id, expected_state, timeout_sec=DEFAULT_TIMEOUT):
    """
    Wait for download manager to emit a task matching task_id has transitioned to the expected_state.
    """
    for _ in range(timeout_sec):
        event = await dm.get_oldest_event()
        if event:
            logging.debug(f"Event received: {event}")
        if event and event.task_id == task_id and event.state == expected_state:
            return event

        await asyncio.sleep(1)
    raise AssertionError(f"Timed out while waiting for {task_id=} to reach {expected_state}.")

def wait_for_file_to_be_created(file_name, timeout_sec=DEFAULT_TIMEOUT):

    for _ in range(timeout_sec):
        time.sleep(1)
        if os.path.exists(file_name):
            return
    
    raise AssertionError(f"Timed out while waiting for {file_name=} to be created")
    

def verify_file(file_name, expected_string):
    with open(file_name) as f:
        file_text = f.read()
        assert(file_text == expected_string, f"Downloaded file text did not match expected.\nDownloaded: {file_text}\nExpected: {expected_string}")
