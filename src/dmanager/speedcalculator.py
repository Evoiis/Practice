import time

from collections import deque

class SpeedCalculator:
    def __init__(self, window_sec: float = 1.5):
        self.window = window_sec
        self.bytes_last = 0
        self.time_last = time.monotonic()
        self.samples = deque() #  List[time:float , bytes_delta: int]

    def add_bytes(self, n_bytes: int):
        now = time.monotonic()
        self.samples.append((now, n_bytes))
        
        # Remove old samples
        cutoff = now - self.window
        while self.samples and self.samples[0][0] < cutoff:
            self.samples.popleft()

    def get_speed(self) -> float:  # bytes/sec
        if len(self.samples) < 2:
            return 0.0

        total_bytes = sum(b for _, b in self.samples)
        total_time  = self.samples[-1][0] - self.samples[0][0]
        
        if total_time < 0.15:   # too short → unstable
            return 0.0
            
        return total_bytes / total_time