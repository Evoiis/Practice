import time
import os
import logging
import asyncio

from typing import Dict, Tuple

from dmanager.core import DownloadState

DEFAULT_TIMEOUT = 30

async def wait_for_state(dm, task_id, expected_state, timeout_sec=DEFAULT_TIMEOUT):
    """
    Wait for download manager to emit a task matching task_id has transitioned to the expected_state.
    """
    start = time.monotonic()
    while time.monotonic() - start < timeout_sec:
        event = await dm.get_oldest_event()
        if event:
            logging.debug(f"WFS: Event received: {event}")
            if event.task_id == task_id and event.state == expected_state:
                return event
        else:
            await asyncio.sleep(0.5)
            
    raise AssertionError(f"Timed out while waiting for {task_id=} to reach {expected_state}.")

async def wait_for_multiple_states(dm, states: Dict[Tuple[int, DownloadState], int], timeout_sec=DEFAULT_TIMEOUT):
    start = time.monotonic()
    while time.monotonic() - start < timeout_sec:
        event = await dm.get_oldest_event()
        if event:
            logging.debug(f"WFMS: Event received: {event}")

            if (event.task_id, event.state) in states:
                states[(event.task_id, event.state)] -= 1
                if states[(event.task_id, event.state)] == 0:
                    del states[(event.task_id, event.state)]

                if len(states) == 0:
                    return
        else:
            await asyncio.sleep(0.5)
    raise AssertionError(f"Timed out while waiting for states to be reached. {states=}")

def wait_for_file_to_be_created(file_name, timeout_sec=DEFAULT_TIMEOUT):

    for _ in range(timeout_sec):
        time.sleep(1)
        if os.path.exists(file_name):
            return
    raise AssertionError(f"Timed out while waiting for {file_name=} to be created")

def verify_file(file_name, expected_string):
    with open(file_name, "r") as f:
        file_text = f.read()
    
    mismatch_indexes = []

    for i in range(len(file_text)):
        if i >= len(expected_string):
            logging.error(f"File string longer than expected string! {len(file_text)=}, {len(expected_string)=}")
            break
        if file_text[i] != expected_string[i]:
            mismatch_indexes.append(i)
    
    if len(expected_string) < 200:
        assert file_text == expected_string, f"Downloaded file text did not match expected.\nDownloaded: {file_text}\nExpected: {expected_string}"
    else:

        intervals = []
        if mismatch_indexes:
            current_interval = [mismatch_indexes.pop(0)]
            prev = current_interval[0]
            while mismatch_indexes:
                index = mismatch_indexes.pop(0)

                if index == prev + 1:
                    prev = index
                else:
                    current_interval.append(prev)
                    intervals.append(current_interval)
                    current_interval = [index]
                    prev = index
            current_interval.append(prev)
            intervals.append(current_interval)
        assert file_text == expected_string, f"Downloaded file text did not match expected.\n{len(file_text)=}\n{len(expected_string)=}\nMismatched Intervals:{intervals}"
