from tilequeue.tile import serialize_coord
import time
from tilequeue.utils import trap_signal


class Worker(object):
    daemonized = False
    logger = None

    def __init__(self, queue=None, job_creator=None):
        self.queue = queue
        self.job_creator = job_creator

    def _log(self, message):
        if self.logger:
            self.logger.info(message)

    def process(self):
        trap_signal()
        while True:
            msgs = self.queue.read(max_to_read=1)
            for msg in msgs:
                start_time = time.time()
                coord = msg.coord
                coord_str = serialize_coord(coord)
                self._log('processing %s ...' % coord_str)
                self.job_creator.process_jobs_for_coord(msg.coord)
                self.queue.job_done(msg.message_handle)
                total_time = time.time() - start_time
                self._log('processing %s ... done took %s (seconds)'
                          % (coord_str, total_time))
            if not self.daemonized:
                break
