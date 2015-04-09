from operator import attrgetter
from psycopg2.extensions import TransactionRollbackError
from tilequeue.process import process_coord
from tilequeue.tile import serialize_coord
from tilequeue.utils import format_stacktrace_one_line
import logging
import Queue
import signal
import sys
import time


# long enough to not fight with other threads, but not long enough
# that it prevents a timely stop
timeout_seconds = 2


def _non_blocking_put(q, data):
    # don't block indefinitely when trying to put to a queue
    # this helps prevent deadlocks if the destination queue is full
    # and stops
    try:
        q.put(data, timeout=timeout_seconds)
    except Queue.Full:
        return False
    else:
        return True


def _force_empty_queue(q):
    # expects a sentinel None value to get enqueued
    # throws out all messages until we receive the sentinel
    # with no sentinel this will block indefinitely
    while q.get() is not None:
        continue


# The strategy with each worker is to loop on a thread event. When the
# main thread/process receives a kill signal, it will issue stops to
# each worker to signal that work should end.
# Additionally, all workers that receive work from a python queue will
# also wait for a sentinel value, None, before terminating. They will
# discard all messages until receiving this sentinel value. Special
# care is also given to the scenario where a None value is received
# before the stop event is checked. The sentinel value here counts as
# a hard stop as well.
# Furthermore, all queue gets and puts are done with timeouts. This is
# to prevent race conditions where a worker is blocked waiting to read
# from a queue that upstream will no longer write to, or try to put to
# a queue that downstream will no longer read from. After any timeout,
# the stop event is checked before any processing to see whether a
# stop event has been received in the interim.


class SqsQueueReader(object):

    def __init__(self, sqs_queue, output_queue, logger, stop,
                 sqs_msgs_to_read_size=10):
        self.sqs_queue = sqs_queue
        self.output_queue = output_queue
        self.sqs_msgs_to_read_size = sqs_msgs_to_read_size
        self.logger = logger
        self.stop = stop

    def __call__(self):
        while not self.stop.is_set():
            try:
                msgs = self.sqs_queue.read(
                    max_to_read=self.sqs_msgs_to_read_size)
            except:
                stacktrace = format_stacktrace_one_line()
                self.logger.error(stacktrace)
                continue

            for msg in msgs:
                # if asked to stop, break as soon as possible
                if self.stop.is_set():
                    break

                metadata = dict(
                    timing=dict(
                        fetch_seconds=None,
                        process_seconds=None,
                        s3_seconds=None,
                    ),
                    sqs_handle=msg.message_handle,
                )
                data = dict(
                    metadata=metadata,
                    coord=msg.coord,
                )
                while not _non_blocking_put(self.output_queue, data):
                    if self.stop.is_set():
                        break

        self.logger.debug('sqs queue reader stopped')


class DataFetch(object):

    def __init__(self, fetcher, input_queue, output_queue, logger):
        self.fetcher = fetcher
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.logger = logger

    def __call__(self, stop):
        saw_sentinel = False
        while not stop.is_set():
            try:
                data = self.input_queue.get(timeout=timeout_seconds)
            except Queue.Empty:
                continue
            if data is None:
                saw_sentinel = True
                break

            coord = data['coord']

            start = time.time()

            try:
                fetch_data = self.fetcher(coord)
            except:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                stacktrace = format_stacktrace_one_line(
                    (exc_type, exc_value, exc_traceback))
                if isinstance(exc_value, TransactionRollbackError):
                    log_level = logging.WARNING
                else:
                    log_level = logging.ERROR
                self.logger.log(log_level, stacktrace)
                continue

            metadata = data['metadata']
            metadata['timing']['fetch_seconds'] = time.time() - start

            data = dict(
                metadata=metadata,
                coord=coord,
                feature_layers=fetch_data['feature_layers'],
                unpadded_bounds=fetch_data['unpadded_bounds'],
                padded_bounds=fetch_data['padded_bounds'],
            )

            while not _non_blocking_put(self.output_queue, data):
                if stop.is_set():
                    break

        if not saw_sentinel:
            _force_empty_queue(self.input_queue)
        self.logger.debug('data fetch stopped')


class ProcessAndFormatData(object):

    scale = 4096

    def __init__(self, formats, input_queue, output_queue, logger):
        formats.sort(key=attrgetter('sort_key'))
        self.formats = formats
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.logger = logger

    def __call__(self, stop):
        # ignore ctrl-c interrupts when run from terminal
        signal.signal(signal.SIGINT, signal.SIG_IGN)

        saw_sentinel = False
        while not stop.is_set():
            try:
                data = self.input_queue.get(timeout=timeout_seconds)
            except Queue.Empty:
                continue
            if data is None:
                saw_sentinel = True
                break

            coord = data['coord']
            feature_layers = data['feature_layers']
            padded_bounds = data['padded_bounds']
            unpadded_bounds = data['unpadded_bounds']

            start = time.time()

            try:
                formatted_tiles = process_coord(
                    coord, feature_layers, self.formats,
                    unpadded_bounds, padded_bounds)
            except:
                stacktrace = format_stacktrace_one_line()
                self.logger.error(stacktrace)
                continue

            metadata = data['metadata']
            metadata['timing']['process_seconds'] = time.time() - start

            data = dict(
                metadata=metadata,
                coord=coord,
                formatted_tiles=formatted_tiles,
            )

            while not _non_blocking_put(self.output_queue, data):
                if stop.is_set():
                    break

        if not saw_sentinel:
            _force_empty_queue(self.input_queue)
        self.logger.debug('processor stopped')


class S3Storage(object):

    def __init__(self, input_queue, output_queue, io_pool, store, logger,
                 stop):
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.io_pool = io_pool
        self.store = store
        self.logger = logger
        self.stop = stop

    def __call__(self):
        saw_sentinel = False

        while not self.stop.is_set():
            try:
                data = self.input_queue.get(timeout=timeout_seconds)
            except Queue.Empty:
                continue
            if data is None:
                saw_sentinel = True
                break

            coord = data['coord']

            start = time.time()
            async_jobs = []
            for formatted_tile in data['formatted_tiles']:

                async_result = self.io_pool.apply_async(
                    self.store.write_tile,
                    (formatted_tile['tile'], coord, formatted_tile['format'])
                )
                async_jobs.append(async_result)

            async_exc_info = None
            for async_job in async_jobs:
                try:
                    async_job.get()
                except:
                    # it's important to wait for all async jobs to
                    # complete
                    # but we just keep a reference to the last
                    # exception
                    # it's unlikely that we would receive multiple
                    # different exceptions when uploading to s3
                    async_exc_info = sys.exc_info()

            if async_exc_info:
                stacktrace = format_stacktrace_one_line(async_exc_info)
                self.logger.error(stacktrace)
                continue

            metadata = data['metadata']
            metadata['timing']['s3_seconds'] = time.time() - start

            data = dict(
                coord=coord,
                metadata=metadata,
            )

            while not _non_blocking_put(self.output_queue, data):
                if self.stop.is_set():
                    break

        if not saw_sentinel:
            _force_empty_queue(self.input_queue)
        self.logger.debug('s3 storage stopped')


class SqsQueueWriter(object):

    def __init__(self, sqs_queue, input_queue, logger, stop):
        self.sqs_queue = sqs_queue
        self.input_queue = input_queue
        self.logger = logger
        self.stop = stop

    def __call__(self):
        saw_sentinel = False
        while not self.stop.is_set():
            try:
                data = self.input_queue.get(timeout=timeout_seconds)
            except Queue.Empty:
                continue
            if data is None:
                saw_sentinel = True
                break

            metadata = data['metadata']
            sqs_handle = metadata['sqs_handle']

            self.sqs_queue.job_done(sqs_handle)

            timing = metadata['timing']
            sqs_timestamp_millis = float(
                sqs_handle.attributes.get('SentTimestamp'))
            sqs_timestamp_seconds = sqs_timestamp_millis / 1000.0
            time_in_queue = time.time() - sqs_timestamp_seconds

            coord = data['coord']
            coord_str = serialize_coord(coord)

            self.logger.info(
                '%s '
                'fetch(%.2fs) '
                'process(%.2fs) '
                'upload(%.2fs) '
                'sqs(%.2fs) ' % (
                    coord_str,
                    timing['fetch_seconds'],
                    timing['process_seconds'],
                    timing['s3_seconds'],
                    time_in_queue,
                ))

        if not saw_sentinel:
            _force_empty_queue(self.input_queue)
        self.logger.debug('sqs queue writer stopped')


class QueuePrint(object):

    def __init__(self, interval_seconds, queue_info, logger, stop):
        self.interval_seconds = interval_seconds
        self.queue_info = queue_info
        self.logger = logger
        self.stop = stop

    def __call__(self):
        # sleep in smaller increments, so that when we're asked to
        # stop we aren't caught sleeping on the job
        sleep_interval_seconds = min(timeout_seconds, self.interval_seconds)
        while not self.stop.is_set():
            i = float(0)
            while i < self.interval_seconds:
                if self.stop.is_set():
                    break
                time.sleep(sleep_interval_seconds)
                i += sleep_interval_seconds

            # to prevent the final empty queue log message
            if self.stop.is_set():
                break

            self.logger.info('')
            for queue, queue_name in self.queue_info:
                self.logger.info(
                    '%s %d %s%s' % (
                        queue_name,
                        queue.qsize(),
                        'empty ' if queue.empty() else '',
                        'full' if queue.full() else '',
                    ))
            self.logger.info('')

        self.logger.debug('queue printer stopped')
