from tilequeue.tile import serialize_coord, deserialize_coord, CoordMessage
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

    def __init__(self, fp):
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

    def read(self, max_to_read=1, timeout_seconds=20):
        with self.lock:
            coords = []
            for _ in range(max_to_read):
                coord = self.fp.readline()
                if coord:
                    coords.append(CoordMessage(deserialize_coord(coord), None))
                else:
                    break

        return coords

    def job_done(self, coord_message):
        pass

    def clear(self):
        with self.lock:
            self.fp.seek(0)
            self.fp.truncate()
            return -1

    def close(self):
        with self.lock:
            self.clear()

            # `self.fp` has already been advanced in `self.read()`, so
            # `fp.read()` will return the remainder of the file.
            self.fp.write(self.fp.read())
            self.fp.close()
