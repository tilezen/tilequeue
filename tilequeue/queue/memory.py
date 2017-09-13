from tilequeue.queue import MessageHandle
from tilequeue.tile import serialize_coord


class MemoryQueue(object):

    def __init__(self):
        self.q = []

    def enqueue(self, coord):
        self.q.append(coord)

    def enqueue_batch(self, coords):
        for coord in coords:
            self.enqueue(coord)

    def read(self):
        max_to_read = 10
        self.q, coords = self.q[max_to_read:], self.q[:max_to_read]
        return [MessageHandle(None, serialize_coord(coord)) for coord in coords]

    def job_done(self, coord_message):
        pass

    def clear(self):
        n = len(self.q)
        del self.q[:]
        return n

    def close(self):
        pass
