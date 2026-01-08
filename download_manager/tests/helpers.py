import asyncio

DEFAULT_TIMEOUT = 20

async def wait_for_state(dm, task_id, expected_state, timeout_sec=DEFAULT_TIMEOUT):
    for i in range(timeout_sec):
        event = await dm.get_oldest_event()
        if event and event.task_id == task_id and event.state == expected_state:
            return event

        await asyncio.sleep(1)
    raise AssertionError(f"Timeout waiting for {task_id=} to reach {expected_state}.")
