from collections import deque

class StreamMonitor:
    def __init__(self, timeout=3.0, window=10.0):
        self.timeout = timeout
        self.window = window
        self.times = deque()
        self.ever_received = False
        self.last_received = None
        self.last_stamp = None
        self.backward_count = 0
        self.last_stamp_went_backward = False

    def record(self, now):
        self.ever_received = True
        self.last_received = now
        self.times.append(now)
        self._prune(now)

    def record_stamp(self, stamp):
        self.last_stamp_went_backward = (
            self.last_stamp is not None and stamp < self.last_stamp
        )
        if self.last_stamp_went_backward:
            self.backward_count += 1

        self.last_stamp = stamp

    def _prune(self, now):
        while self.times and now - self.times[0] > self.window:
            self.times.popleft()

    def status(self, now):
        if not self.ever_received:
            return 'WAITING'
        if now - self.last_received > self.timeout:
            return 'NO DATA'
        return 'OK'

    def rate(self, now):
        self._prune(now)
        if len(self.times) < 2:
            return 0.0
        duration = self.times[-1] - self.times[0]
        return (len(self.times) - 1) / duration if duration > 0 else 0.0

    def age(self, now):
        return None if self.last_received is None else now - self.last_received
