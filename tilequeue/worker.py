from tilequeue.tile import serialize_coord
from tilequeue.utils import trap_signal
import sys
import time
import traceback


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
                start_time = time.time()
                coord = msg.coord
                coord_str = serialize_coord(coord)
                self._log('processing %s ...' % coord_str)
                try:
                    self.job_creator.process_jobs_for_coord(msg.coord)
                    self.queue.job_done(msg.message_handle)
                    current_time = time.time()
                    total_time = current_time - start_time
                    self._log('processing %s ... done took %s (seconds)'
                              % (coord_str, total_time))
                except:
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    stacktrace = ''.join(traceback.format_exception(
                        exc_type, exc_value, exc_traceback))
                    self._log('processing %s ... failed' % coord_str)
                    self._log(stacktrace)
                sent_timestamp = int(msg.attributes.get('SentTimestamp'))
                message_sent = sent_timestamp / 1000
                time_in_queue = int(current_time) - message_sent
                self._log('time in queue %s (seconds)' % (time_in_queue))

            if not self.daemonized:
                break
