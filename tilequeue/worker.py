from collections import namedtuple
from itertools import izip
from operator import attrgetter
from tilequeue.log import LogCategory
from tilequeue.log import LogLevel
from tilequeue.log import MsgType
from tilequeue.metatile import common_parent
from tilequeue.metatile import make_metatiles
from tilequeue.process import convert_source_data_to_feature_layers
from tilequeue.process import process_coord
from tilequeue.queue import JobProgressException
from tilequeue.queue.message import QueueHandle
from tilequeue.store import write_tile_if_changed
from tilequeue.tile import coord_children_subrange
from tilequeue.tile import coord_to_mercator_bounds
from tilequeue.tile import serialize_coord
from tilequeue.utils import convert_seconds_to_millis
from tilequeue.utils import format_stacktrace_one_line
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
    def __init__(self, output_queue, tile_proc_logger, stop):
        self.output_queue = output_queue
        self.tile_proc_logger = tile_proc_logger
        self.stop = stop

    def __call__(self, coord, data):
        """
        Send data, associated with coordinate coord, to the queue. While also
        watching for a signal to stop. If the data is too large to send, then
        trap the MemoryError and exit the program.

        Note that `coord` may be a Coordinate instance or a string. It is only
        used for printing out a message if there's a MemoryError, so for
        requests which have no meaningful single coordinate, something else
        can be used.

        Returns True if the "stop signal" has been set and the thread should
        shut down. False if normal operations should continue.
        """

        try:
            while not _non_blocking_put(self.output_queue, data):
                if self.stop.is_set():
                    return True

        except MemoryError as e:
            stacktrace = format_stacktrace_one_line()
            self.tile_proc_logger.error(
                'MemoryError sending to queue', e, stacktrace, coord)
            # memory error might not leave the malloc subsystem in a usable
            # state, so better to exit the whole worker here than crash this
            # thread, which would lock up the whole worker.
            sys.exit(1)

        return False


def _ack_coord_handle(
        coord, coord_handle, queue_mapper, msg_tracker, timing_state,
        tile_proc_logger, stats_handler):
    """share code for acknowledging a coordinate"""

    # returns tuple of (handle, error), either of which can be None

    track_result = msg_tracker.done(coord_handle)
    queue_handle = track_result.queue_handle
    if not queue_handle:
        return None, None

    tile_queue = queue_mapper.get_queue(queue_handle.queue_id)
    assert tile_queue, \
        'Missing tile_queue: %s' % queue_handle.queue_id

    parent_tile = None
    if track_result.all_done:
        parent_tile = track_result.parent_tile

        try:
            tile_queue.job_done(queue_handle.handle)
        except Exception as e:
            stacktrace = format_stacktrace_one_line()
            tile_proc_logger.error_job_done(
                'tile_queue.job_done', e, stacktrace,
                queue_handle, coord, parent_tile,
            )
            return queue_handle, e

        if parent_tile is not None:
            # we completed a tile pyramid and should log appropriately

            start_time = timing_state['start']
            stop_time = convert_seconds_to_millis(time.time())
            tile_proc_logger.log_processed_pyramid(
                parent_tile, start_time, stop_time)
            stats_handler.processed_pyramid(
                parent_tile, start_time, stop_time)
    else:
        try:
            tile_queue.job_progress(queue_handle.handle)
        except Exception as e:
            stacktrace = format_stacktrace_one_line()
            err_details = None
            if isinstance(e, JobProgressException):
                err_details = e.err_details
            tile_proc_logger.error_job_progress(
                'tile_queue.job_progress', e, stacktrace,
                queue_handle, coord, parent_tile, err_details,
            )
            return queue_handle, e

    return queue_handle, None


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


class TileQueueReader(object):

    def __init__(
            self, queue_mapper, msg_marshaller, msg_tracker, output_queue,
            tile_proc_logger, stats_handler, stop, max_zoom):
        self.queue_mapper = queue_mapper
        self.msg_marshaller = msg_marshaller
        self.msg_tracker = msg_tracker
        self.output = OutputQueue(output_queue, tile_proc_logger, stop)
        self.tile_proc_logger = tile_proc_logger
        self.stats_handler = stats_handler
        self.stop = stop
        self.max_zoom = max_zoom

    def __call__(self):
        while not self.stop.is_set():

            msg_handles = ()

            for queue_id, tile_queue in (
                    self.queue_mapper.queues_in_priority_order()):
                try:
                    msg_handles = tile_queue.read()
                except Exception as e:
                    stacktrace = format_stacktrace_one_line()
                    self.tile_proc_logger.error(
                        'Queue read error', e, stacktrace)
                    continue
                if msg_handles:
                    break

            if not msg_handles:
                continue

            for msg_handle in msg_handles:
                # if asked to stop, break as soon as possible
                if self.stop.is_set():
                    break

                now = convert_seconds_to_millis(time.time())
                msg_timestamp = None
                if msg_handle.metadata:
                    msg_timestamp = msg_handle.metadata.get('timestamp')
                timing_state = dict(
                    msg_timestamp=msg_timestamp,
                    start=now,
                )

                coords = self.msg_marshaller.unmarshall(msg_handle.payload)

                queue_handle = QueueHandle(queue_id, msg_handle.handle)
                coord_handles = self.msg_tracker.track(
                    queue_handle, coords)

                all_coords_data = []
                top_tile = None
                for coord, coord_handle in izip(coords, coord_handles):
                    if coord.zoom > self.max_zoom:
                        self._reject_coord(coord, coord_handle, timing_state)
                        continue

                    metadata = dict(
                        # the timing is just what will be filled out later
                        timing=dict(
                            fetch=None,
                            process=None,
                            s3=None,
                            ack=None,
                        ),
                        # this is temporary state that is used later on to
                        # determine timing information
                        timing_state=timing_state,
                        coord_handle=coord_handle,
                    )
                    data = dict(
                        metadata=metadata,
                        coord=coord,
                    )

                    # find the parent of all the tiles. this is useful to be
                    # able to describe the job without having to list all the
                    # tiles.
                    if top_tile:
                        top_tile = common_parent(top_tile, coord)
                    else:
                        top_tile = coord

                    all_coords_data.append(data)

                coord_input_spec = all_coords_data, top_tile
                msg = "group of %d tiles below %s" \
                      % (len(all_coords_data), serialize_coord(top_tile))
                if self.output(msg, coord_input_spec):
                    break

        for _, tile_queue in self.queue_mapper.queues_in_priority_order():
            tile_queue.close()
        self.tile_proc_logger.lifecycle('tile queue reader stopped')

    def _reject_coord(self, coord, coord_handle, timing_state):
        self.tile_proc_logger.log(
            LogLevel.WARNING,
            LogCategory.PROCESS,
            MsgType.INDIVIDUAL,
            'Job coordinates above max zoom are not '
            'supported, skipping %d > %d' % (
                coord.zoom, self.max_zoom),
            None,  # exception
            None,  # stacktrace
            coord,
        )
        # delete jobs that we can't handle from the
        # queue, otherwise we'll get stuck in a cycle
        # of timed-out jobs being re-added to the
        # queue until they overflow max-retries.
        _ack_coord_handle(
            coord, coord_handle, self.queue_mapper, self.msg_tracker,
            timing_state, self.tile_proc_logger, self.stats_handler)


class DataFetch(object):

    def __init__(
            self, fetcher, input_queue, output_queue, io_pool,
            tile_proc_logger, stats_handler, metatile_zoom, max_zoom,
            metatile_start_zoom=0):
        self.fetcher = fetcher
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.io_pool = io_pool
        self.tile_proc_logger = tile_proc_logger
        self.stats_handler = stats_handler
        self.metatile_zoom = metatile_zoom
        self.max_zoom = max_zoom
        self.metatile_start_zoom = metatile_start_zoom

    def __call__(self, stop):
        saw_sentinel = False
        output = OutputQueue(self.output_queue, self.tile_proc_logger, stop)

        while not stop.is_set():
            try:
                coord_input_spec = self.input_queue.get(
                    timeout=timeout_seconds)
            except Queue.Empty:
                continue
            if coord_input_spec is None:
                saw_sentinel = True
                break

            coord = None
            parent = None
            try:
                all_data, parent = coord_input_spec
                for fetch, data in self.fetcher.fetch_tiles(all_data):
                    metadata = data['metadata']
                    coord = data['coord']
                    if self._fetch_and_output(fetch, coord, metadata, output):
                        break
            except Exception as e:
                stacktrace = format_stacktrace_one_line()
                self.tile_proc_logger.fetch_error(e, stacktrace, coord, parent)
                self.stats_handler.fetch_error()

        if not saw_sentinel:
            _force_empty_queue(self.input_queue)

        self.tile_proc_logger.lifecycle('data fetch stopped')

    def _fetch_and_output(self, fetch, coord, metadata, output):
        data = self._fetch(fetch, coord, metadata)
        return output(coord, data)

    def _fetch(self, fetch, coord, metadata):
        nominal_zoom = coord.zoom + self.metatile_zoom
        start_zoom = coord.zoom + self.metatile_start_zoom
        unpadded_bounds = coord_to_mercator_bounds(coord)

        start = time.time()

        source_rows = fetch(nominal_zoom, unpadded_bounds)

        metadata['timing']['fetch'] = convert_seconds_to_millis(
            time.time() - start)

        # every tile job that we get from the queue is a "parent" tile
        # and its four children to cut from it. at zoom 15, this may
        # also include a whole bunch of other children below the max
        # zoom.
        cut_coords = list(
            coord_children_subrange(coord, start_zoom, nominal_zoom))

        return dict(
            metadata=metadata,
            coord=coord,
            source_rows=source_rows,
            unpadded_bounds=unpadded_bounds,
            cut_coords=cut_coords,
            nominal_zoom=nominal_zoom,
        )


class ProcessAndFormatData(object):

    scale = 4096

    def __init__(self, post_process_data, formats, input_queue,
                 output_queue, buffer_cfg, output_calc_mapping, layer_data,
                 tile_proc_logger, stats_handler):
        formats.sort(key=attrgetter('sort_key'))
        self.post_process_data = post_process_data
        self.formats = formats
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.buffer_cfg = buffer_cfg
        self.output_calc_mapping = output_calc_mapping
        self.layer_data = layer_data
        self.tile_proc_logger = tile_proc_logger
        self.stats_handler = stats_handler

    def __call__(self, stop):
        # ignore ctrl-c interrupts when run from terminal
        signal.signal(signal.SIGINT, signal.SIG_IGN)

        output = OutputQueue(self.output_queue, self.tile_proc_logger, stop)

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
            unpadded_bounds = data['unpadded_bounds']
            cut_coords = data['cut_coords']
            nominal_zoom = data['nominal_zoom']
            source_rows = data['source_rows']

            start = time.time()

            try:
                feature_layers = convert_source_data_to_feature_layers(
                    source_rows, self.layer_data, unpadded_bounds,
                    nominal_zoom)
                formatted_tiles, extra_data = process_coord(
                    coord, nominal_zoom, feature_layers,
                    self.post_process_data, self.formats, unpadded_bounds,
                    cut_coords, self.buffer_cfg, self.output_calc_mapping)
            except Exception as e:
                stacktrace = format_stacktrace_one_line()
                self.tile_proc_logger.error(
                    'Processing error', e, stacktrace, coord)
                self.stats_handler.proc_error()
                continue

            metadata = data['metadata']
            metadata['timing']['process'] = convert_seconds_to_millis(
                time.time() - start)
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
        self.tile_proc_logger.lifecycle('processor stopped')


class S3Storage(object):

    def __init__(self, input_queue, output_queue, io_pool, store,
                 tile_proc_logger, metatile_size):
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.io_pool = io_pool
        self.store = store
        self.tile_proc_logger = tile_proc_logger
        self.metatile_size = metatile_size

    def __call__(self, stop):
        saw_sentinel = False

        queue_output = OutputQueue(
            self.output_queue, self.tile_proc_logger, stop)

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

            except Exception as e:
                # cannot propagate this error - it crashes the thread and
                # blocks up the whole queue!
                stacktrace = format_stacktrace_one_line()
                self.tile_proc_logger.error('Save error', e, stacktrace, coord)
                continue

            async_exc_info = None
            e = None
            n_stored = 0
            n_not_stored = 0
            for async_job in async_jobs:
                try:
                    did_store = async_job.get()
                    if did_store:
                        n_stored += 1
                    else:
                        n_not_stored += 1
                except Exception as e:
                    # it's important to wait for all async jobs to
                    # complete but we just keep a reference to the last
                    # exception it's unlikely that we would receive multiple
                    # different exceptions when uploading to s3
                    async_exc_info = sys.exc_info()

            if async_exc_info:
                stacktrace = format_stacktrace_one_line(async_exc_info)
                self.tile_proc_logger.error(
                    'Store error', e, stacktrace, coord)
                continue

            metadata = data['metadata']
            metadata['timing']['s3'] = convert_seconds_to_millis(
                time.time() - start)
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
        self.tile_proc_logger.lifecycle('s3 storage stopped')

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


CoordProcessData = namedtuple(
    'CoordProcessData',
    ('coord', 'timing', 'size', 'store_info',),
)


class TileQueueWriter(object):

    def __init__(
            self, queue_mapper, input_queue, inflight_mgr, msg_tracker,
            tile_proc_logger, stats_handler, stop):
        self.queue_mapper = queue_mapper
        self.input_queue = input_queue
        self.inflight_mgr = inflight_mgr
        self.msg_tracker = msg_tracker
        self.tile_proc_logger = tile_proc_logger
        self.stats_handler = stats_handler
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
            coord_handle = metadata['coord_handle']
            coord = data['coord']
            timing_state = metadata['timing_state']

            start = time.time()

            try:
                self.inflight_mgr.unmark_inflight(coord)
            except Exception as e:
                stacktrace = format_stacktrace_one_line()
                self.tile_proc_logger.error(
                    'Unmarking in-flight error', e, stacktrace, coord)
                continue

            queue_handle, err = _ack_coord_handle(
                coord, coord_handle, self.queue_mapper, self.msg_tracker,
                timing_state, self.tile_proc_logger, self.stats_handler)
            if err is not None:
                continue

            timing = metadata['timing']
            now = time.time()
            timing['ack'] = convert_seconds_to_millis(now - start)

            time_in_queue = 0
            msg_timestamp = timing_state['msg_timestamp']
            if msg_timestamp:
                time_in_queue = convert_seconds_to_millis(now) - msg_timestamp
            timing['queue'] = time_in_queue

            layers = metadata['layers']
            size = layers['size']

            store_info = metadata['store']

            coord_proc_data = CoordProcessData(
                coord,
                timing,
                size,
                store_info,
            )
            self.tile_proc_logger.log_processed_coord(coord_proc_data)
            self.stats_handler.processed_coord(coord_proc_data)

        if not saw_sentinel:
            _force_empty_queue(self.input_queue)
        self.tile_proc_logger.lifecycle('tile queue writer stopped')


class QueuePrint(object):

    def __init__(self, interval_seconds, queue_info, tile_proc_logger, stop):
        self.interval_seconds = interval_seconds
        self.queue_info = queue_info
        self.tile_proc_logger = tile_proc_logger
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

            self.tile_proc_logger.log_queue_sizes(self.queue_info)

        self.tile_proc_logger.lifecycle('queue printer stopped')
