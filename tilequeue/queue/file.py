from tilequeue.queue import MessageHandle
import threading


class OutputFileQueue(object):
    '''
    A local, file-based queue for storing the coordinates of tiles to render.
    Can be used as a drop-in replacement for `tilequeue.queue.sqs.SqsQueue`.
    Note that it doesn't support reading/writing from multiple `tilequeue`
    instances; you *can* `seed` and `process` at the same time, but you *can't*
    run more than one `seed` or `write` instance at the same time. This is
    primarily meant for development/debugging, so adding multi-process locking
    probably isn't worth the complexity.
    '''

    def __init__(self, fp, read_size=10):
        self.read_size = read_size
        self.fp = fp
        self.lock = threading.RLock()

    def enqueue(self, payload):
        with self.lock:
            self.fp.write(payload + '\n')

    def enqueue_batch(self, payloads):
        n = 0
        for payload in payloads:
            self.enqueue(payload)
            n += 1
        return n, 0

    def read(self):
        with self.lock:
            msg_handles = []
            for _ in range(self.read_size):
                payload = self.fp.readline().strip()
                msg_handle = MessageHandle(None, payload)
                msg_handles.append(msg_handle)

        return msg_handles

    def job_done(self, msg_handle):
        pass

    def clear(self):
        with self.lock:
            self.fp.seek(0)
            self.fp.truncate()
            return -1

    def close(self):
        with self.lock:
            self.clear()
            self.fp.close()
