from tilequeue.queue import MessageHandle


class MemoryQueue(object):

    def __init__(self):
        self.q = []

    def enqueue(self, payload):
        self.q.append(payload)

    def enqueue_batch(self, payloads):
        for payload in payloads:
            self.enqueue(payload)

    def read(self):
        max_to_read = 10
        self.q, payloads = self.q[max_to_read:], self.q[:max_to_read]
        return [MessageHandle(None, payload) for payload in payloads]

    def job_done(self, msg_handle):
        pass

    def clear(self):
        n = len(self.q)
        del self.q[:]
        return n

    def close(self):
        pass
