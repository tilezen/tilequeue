from tilequeue.queue import MessageHandle
from tilequeue.tile import deserialize_coord
from tilequeue.tile import serialize_coord
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

    def enqueue(self, coord):
        with self.lock:
            payload = serialize_coord(coord)
            self.fp.write(payload + '\n')

    def enqueue_batch(self, coords):
        n = 0
        for coord in coords:
            self.enqueue(coord)
            n += 1
        return n, 0

    def read(self):
        with self.lock:
            msg_handles = []
            for _ in range(self.read_size):
                coord_str = self.fp.readline() or ''
                coord = deserialize_coord(coord_str)
                if coord:
                    msg_handle = MessageHandle(None, coord)
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
