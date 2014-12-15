from tilequeue.tile import serialize_coord
import time
from tilequeue.utils import trap_signal
from psycopg2.extensions import TransactionRollbackError


class Worker(object):
    daemonized = False
    logger = None

    def __init__(self, queue, job_creator):
        self.queue = queue
        self.job_creator = job_creator

    def _log(self, message):
        if self.logger:
            self.logger.info(message)

    def process(self, max_to_read=1):
        trap_signal()

        # process specific initialization
        self.job_creator.initialize()

        while True:
            msgs = self.queue.read(max_to_read=max_to_read)
            for msg in msgs:
                try:
                    start_time = time.time()
                    coord = msg.coord
                    coord_str = serialize_coord(coord)
                    self._log('processing %s ...' % coord_str)
                    self.job_creator.process_jobs_for_coord(msg.coord)
                    self.queue.job_done(msg.message_handle)
                    total_time = time.time() - start_time
                    self._log('processing %s ... done took %s (seconds)'
                              % (coord_str, total_time))
                except TransactionRollbackError:
                    self._log('TransactionRollbackError continuing...')
                except Exception as e:
                    self._log(e)
            if not self.daemonized:
                break
