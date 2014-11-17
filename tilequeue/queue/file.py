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
        return n

    def read(self, max_to_read=1, timeout_seconds=20):
        raise NotImplementedError

    def job_done(self, message):
        raise NotImplementedError

    def jobs_done(self, messages):
        raise NotImplementedError

    def clear(self):
        self.fp.truncate()
        return -1

    def close(self):
        self.fp.close()
