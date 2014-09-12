from tilequeue.tile import TileJobMessage

class MemoryQueue(object):

    def __init__(self):
        self.q = []

    def enqueue_tile_job(self, tile_job):
        self.q.append(tile_job)

    def enqueue_tile_jobs(self, tile_jobs):
        self.q.extend(tile_jobs)

    def read_tile_jobs(self, max_tile_jobs=1, timeout_seconds=None):
        self.q, jobs = self.q[max_tile_jobs:], self.q[:max_tile_jobs]
        return [TileJobMessage(tile_job, None) for tile_job in jobs]

    def job_done(self, message):
        pass

    def jobs_done(self, messages):
        pass
