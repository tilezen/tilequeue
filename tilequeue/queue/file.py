from tilequeue.tile import serialize_coord


class OutputFileQueue(object):

    def __init__(self, fp):
        self.fp = fp

    def enqueue(self, coord):
        payload = serialize_coord(coord)
        self.fp.write(payload + '\n')

    def enqueue_batch(self, coords):
        n = 0
        for coord in coords:
            self.enqueue(coord)
            n += 1
        return n, 0

    def read(self, max_to_read=1, timeout_seconds=20):
        coords = []
        for _ in range(max_to_read):
            try:
                coords.append(next(self.fp))
            except StopIteration:
                break

        return coords

    def job_done(self, coord_message):
        pass

    def clear(self):
        self.fp.seek(0)
        self.fp.truncate()
        return -1

    def close(self):
        remaining_queue = "".join([ln for ln in self.fp])
        self.clear()
        self.fp.write(remaining_queue)
        self.fp.close()
