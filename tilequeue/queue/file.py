from tilequeue.tile import serialize_coord, deserialize_coord, CoordMessage
import threading


class OutputFileQueue(object):

    def __init__(self, fp):
        self.fp = fp
        self._lock = threading.RLock()

    def enqueue(self, coord):
        with self._lock:
            payload = serialize_coord(coord)
            self.fp.write(payload + '\n')

    def enqueue_batch(self, coords):
        n = 0
        for coord in coords:
            self.enqueue(coord)
            n += 1
        return n, 0

    def read(self, max_to_read=1, timeout_seconds=20):
        with self._lock:
            coords = []
            for _ in range(max_to_read):
                try:
                    coord = next(self.fp)
                except StopIteration:
                    break
                coords.append(CoordMessage(deserialize_coord(coord), None))

        return coords

    def job_done(self, coord_message):
        pass

    def clear(self):
        with self._lock:
            self.fp.seek(0)
            self.fp.truncate()
            return -1

    def close(self):
        with self._lock:
            remaining_queue = "".join([ln for ln in self.fp])
            self.clear()
            self.fp.write(remaining_queue)
            self.fp.close()
