from tilequeue.tile import serialize_coord


class OutputFileQueue(object):

    def __init__(self, fp):
        self.fp = fp

    def enqueue(self, coord):
        payload = serialize_coord(coord)
        self.fp.write(payload + '\n')

    def enqueue_batch(self, coords):
        for coord in coords:
            self.enqueue(coord)

    def read(self, max_to_read=1, timeout_seconds=20):
        raise NotImplementedError

    def job_done(self, message):
        raise NotImplementedError

    def jobs_done(self, messages):
        raise NotImplementedError

    def close(self):
        self.fp.close()
