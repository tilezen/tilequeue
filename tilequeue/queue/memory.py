from tilequeue.tile import CoordMessage


class MemoryQueue(object):

    def __init__(self):
        self.q = []

    def enqueue(self, coord):
        self.q.append(coord)

    def enqueue_batch(self, coords):
        n = 0
        for coord in coords:
            self.enqueue(coord)
            n += 1
        return n

    def read(self, max_to_read=1, timeout_seconds=None):
        self.q, coords = self.q[max_to_read:], self.q[:max_to_read]
        return [CoordMessage(coord, None) for coord in coords]

    def job_done(self, message):
        pass

    def jobs_done(self, messages):
        pass

    def close(self):
        pass
