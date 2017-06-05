from operator import attrgetter
from psycopg2.extensions import TransactionRollbackError
from tilequeue.process import process_coord
from tilequeue.store import write_tile_if_changed
from tilequeue.tile import coord_children_range
from tilequeue.tile import coord_to_mercator_bounds
from tilequeue.tile import serialize_coord
from tilequeue.utils import format_stacktrace_one_line
from tilequeue.metatile import make_metatiles
import logging
import Queue
import signal
import sys
import time


# long enough to not fight with other threads, but not long enough
# that it prevents a timely stop
timeout_seconds = 5


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


# OutputQueue wraps the process of sending data to a multiprocessing queue
# so that we can simultaneously check for the "stop" signal when it's time
# to shut down.
class OutputQueue(object):
    def __init__(self, output_queue, stop):
        self.output_queue = output_queue
        self.stop = stop

    def __call__(self, coord, data):
        """
        Send data, associated with coordinate coord, to the queue. While also
        watching for a signal to stop. If the data is too large to send, then
        trap the MemoryError and exit the program.
        """

        try:
            while not _non_blocking_put(self.output_queue, data):
                if self.stop.is_set():
                    return True

        except MemoryError:
            stacktrace = format_stacktrace_one_line()
            self.logger.error(
                "MemoryError while sending %s to the queue. Stacktrace: %s" %
                (serialize_coord(coord), stacktrace))
            # memory error might not leave the malloc subsystem in a usable
            # state, so better to exit the whole worker here than crash this
            # thread, which would lock up the whole worker.
            sys.exit(1)

        return False


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

    def __init__(
            self, sqs_queue, output_queue, logger, stop, max_zoom,
            sqs_msgs_to_read_size=10):
        self.sqs_queue = sqs_queue
        self.output = OutputQueue(output_queue, stop)
        self.sqs_msgs_to_read_size = sqs_msgs_to_read_size
        self.logger = logger
        self.stop = stop
        self.max_zoom = max_zoom

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

                if msg.coord.zoom > self.max_zoom:
                    self.logger.log(
                        logging.WARNING,
                        'Job coordinates above max zoom are not supported, '
                        'skipping %r > %d' % (msg.coord, self.max_zoom))

                    # delete jobs that we can't handle from the queue,
                    # otherwise we'll get stuck in a cycle of timed-out jobs
                    # being re-added to the queue until they overflow
                    # max-retries.
                    try:
                        self.sqs_queue.job_done(msg)
                    except:
                        stacktrace = format_stacktrace_one_line()
                        self.logger.error('Error acknowledging: %s - %s' % (
                            serialize_coord(msg.coord), stacktrace))
                    continue

                metadata = dict(
                    timing=dict(
                        fetch_seconds=None,
                        process_seconds=None,
                        s3_seconds=None,
                        ack_seconds=None,
                    ),
                    coord_message=msg,
                )
                data = dict(
                    metadata=metadata,
                    coord=msg.coord,
                )
                if self.output(msg.coord, data):
                    break

        self.sqs_queue.close()
        self.logger.debug('sqs queue reader stopped')


class DataFetch(object):

    def __init__(
            self, fetcher, input_queue, output_queue, io_pool,
            logger, metatile_zoom, max_zoom):
        self.fetcher = fetcher
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.io_pool = io_pool
        self.logger = logger
        self.metatile_zoom = metatile_zoom
        self.max_zoom = max_zoom

    def __call__(self, stop):
        saw_sentinel = False
        output = OutputQueue(self.output_queue, stop)

        while not stop.is_set():
            try:
                data = self.input_queue.get(timeout=timeout_seconds)
            except Queue.Empty:
                continue
            if data is None:
                saw_sentinel = True
                break

            coord = data['coord']
            nominal_zoom = coord.zoom + self.metatile_zoom
            unpadded_bounds = coord_to_mercator_bounds(coord)

            start = time.time()

            try:
                fetch_data = self.fetcher(nominal_zoom, unpadded_bounds)
            except:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                stacktrace = format_stacktrace_one_line(
                    (exc_type, exc_value, exc_traceback))
                if isinstance(exc_value, TransactionRollbackError):
                    log_level = logging.WARNING
                else:
                    log_level = logging.ERROR
                self.logger.log(log_level, 'Error fetching: %s - %s' % (
                    serialize_coord(coord), stacktrace))
                continue

            metadata = data['metadata']
            metadata['timing']['fetch_seconds'] = time.time() - start

            # every tile job that we get from the queue is a "parent" tile
            # and its four children to cut from it. at zoom 15, this may
            # also include a whole bunch of other children below the max
            # zoom.
            cut_coords = list()
            if nominal_zoom > coord.zoom:
                cut_coords.extend(coord_children_range(coord, nominal_zoom))

            data = dict(
                metadata=metadata,
                coord=coord,
                feature_layers=fetch_data['feature_layers'],
                unpadded_bounds=fetch_data['unpadded_bounds'],
                cut_coords=cut_coords,
                nominal_zoom=nominal_zoom,
            )

            if output(coord, data):
                break

        if not saw_sentinel:
            _force_empty_queue(self.input_queue)
        self.logger.debug('data fetch stopped')


class ProcessAndFormatData(object):

    scale = 4096

    def __init__(self, post_process_data, formats, input_queue,
                 output_queue, buffer_cfg, logger):
        formats.sort(key=attrgetter('sort_key'))
        self.post_process_data = post_process_data
        self.formats = formats
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.buffer_cfg = buffer_cfg
        self.logger = logger

    def __call__(self, stop):
        # ignore ctrl-c interrupts when run from terminal
        signal.signal(signal.SIGINT, signal.SIG_IGN)

        output = OutputQueue(self.output_queue, stop)

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
            unpadded_bounds = data['unpadded_bounds']
            cut_coords = data['cut_coords']
            nominal_zoom = data['nominal_zoom']

            start = time.time()

            try:
                formatted_tiles, extra_data = process_coord(
                    coord, nominal_zoom, feature_layers,
                    self.post_process_data, self.formats, unpadded_bounds,
                    cut_coords, self.buffer_cfg)
            except:
                stacktrace = format_stacktrace_one_line()
                self.logger.error('Error processing: %s - %s' % (
                    serialize_coord(coord), stacktrace))
                continue

            metadata = data['metadata']
            metadata['timing']['process_seconds'] = time.time() - start
            metadata['layers'] = extra_data

            data = dict(
                metadata=metadata,
                coord=coord,
                formatted_tiles=formatted_tiles,
            )

            if output(coord, data):
                break

        if not saw_sentinel:
            _force_empty_queue(self.input_queue)
        self.logger.debug('processor stopped')


class S3Storage(object):

    def __init__(self, input_queue, output_queue, io_pool, store, logger,
                 metatile_size):
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.io_pool = io_pool
        self.store = store
        self.logger = logger
        self.metatile_size = metatile_size

    def __call__(self, stop):
        saw_sentinel = False

        queue_output = OutputQueue(self.output_queue, stop)

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
                async_jobs = self.save_tiles(data['formatted_tiles'])

            except:
                # cannot propagate this error - it crashes the thread and
                # blocks up the whole queue!
                stacktrace = format_stacktrace_one_line(sys.exc_info())
                self.logger.error('Error saving tiles: %s - %s' % (
                    serialize_coord(coord), stacktrace))
                continue

            async_exc_info = None
            n_stored = 0
            n_not_stored = 0
            for async_job in async_jobs:
                try:
                    did_store = async_job.get()
                    if did_store:
                        n_stored += 1
                    else:
                        n_not_stored += 1
                except:
                    # it's important to wait for all async jobs to
                    # complete but we just keep a reference to the last
                    # exception it's unlikely that we would receive multiple
                    # different exceptions when uploading to s3
                    async_exc_info = sys.exc_info()

            if async_exc_info:
                stacktrace = format_stacktrace_one_line(async_exc_info)
                self.logger.error('Error storing: %s - %s' % (
                    serialize_coord(coord), stacktrace))
                continue

            metadata = data['metadata']
            metadata['timing']['s3_seconds'] = time.time() - start
            metadata['store'] = dict(
                stored=n_stored,
                not_stored=n_not_stored,
            )

            data = dict(
                coord=coord,
                metadata=metadata,
            )

            if queue_output(coord, data):
                break

        if not saw_sentinel:
            _force_empty_queue(self.input_queue)
        self.logger.debug('s3 storage stopped')

    def save_tiles(self, tiles):
        async_jobs = []

        if self.metatile_size:
            tiles = make_metatiles(self.metatile_size, tiles)

        for tile in tiles:

            async_result = self.io_pool.apply_async(
                write_tile_if_changed, (
                    self.store,
                    tile['tile'],
                    # important to use the coord from the
                    # formatted tile here, because we could have
                    # cut children tiles that have separate zooms
                    # too
                    tile['coord'],
                    tile['format'],
                    tile['layer']))
            async_jobs.append(async_result)

        return async_jobs


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
            coord_message = metadata['coord_message']
            coord = data['coord']

            start = time.time()
            try:
                self.sqs_queue.job_done(coord_message)
            except:
                stacktrace = format_stacktrace_one_line()
                self.logger.error('Error acknowledging: %s - %s' % (
                    serialize_coord(coord), stacktrace))
                continue

            timing = metadata['timing']
            now = time.time()
            timing['ack_seconds'] = now - start

            coord_message = metadata['coord_message']
            msg_metadata = coord_message.metadata
            time_in_queue = 0
            if msg_metadata:
                sqs_timestamp_millis = msg_metadata.get('timestamp')
                if sqs_timestamp_millis is not None:
                    sqs_timestamp_seconds = sqs_timestamp_millis / 1000.0
                    time_in_queue = now - sqs_timestamp_seconds

            layers = metadata['layers']
            size = layers['size']
            size_as_str = repr(size)

            store_info = metadata['store']

            self.logger.info(
                '%s '
                'data(%.2fs) '
                'proc(%.2fs) '
                's3(%.2fs) '
                'ack(%.2fs) '
                'sqs(%.2fs) '
                'size(%s) '
                'stored(%s) '
                'not_stored(%s)' % (
                    serialize_coord(coord),
                    timing['fetch_seconds'],
                    timing['process_seconds'],
                    timing['s3_seconds'],
                    timing['ack_seconds'],
                    time_in_queue,
                    size_as_str,
                    store_info['stored'],
                    store_info['not_stored'],
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
