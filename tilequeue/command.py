from collections import namedtuple
from contextlib import closing
from itertools import chain
from jinja2 import Environment
from jinja2 import FileSystemLoader
from multiprocessing.pool import ThreadPool
from tilequeue.config import make_config_from_argparse
from tilequeue.format import lookup_format_by_extension
from tilequeue.metro_extract import city_bounds
from tilequeue.metro_extract import parse_metro_extract
from tilequeue.query import DataFetcher
from tilequeue.query import jinja_filter_bbox_filter
from tilequeue.query import jinja_filter_bbox_intersection
from tilequeue.query import jinja_filter_bbox
from tilequeue.query import jinja_filter_geometry
from tilequeue.queue import make_sqs_queue
from tilequeue.tile import bounds_buffer
from tilequeue.tile import coord_int_zoom_up
from tilequeue.tile import coord_marshall_int
from tilequeue.tile import coord_unmarshall_int
from tilequeue.tile import parse_expired_coord_string
from tilequeue.tile import seed_tiles
from tilequeue.tile import tile_generator_for_multiple_bounds
from tilequeue.tile import tile_generator_for_single_bounds
from tilequeue.tile import zoom_mask
from tilequeue.top_tiles import parse_top_tiles
from tilequeue.worker import DataFetch
from tilequeue.worker import ProcessAndFormatData
from tilequeue.worker import QueuePrint
from tilequeue.worker import S3Storage
from tilequeue.worker import SqsQueueReader
from tilequeue.worker import SqsQueueWriter
from urllib2 import urlopen
from zope.dottedname.resolve import resolve
import argparse
import logging
import logging.config
import multiprocessing
import os
import Queue
import signal
import sys
import threading
import time
import yaml
import datetime
import os.path
import traceback


def create_command_parser(fn):
    def create_parser_fn(parser):
        parser.add_argument('--config', required=True,
                            help='The path to the tilequeue config file.')
        parser.set_defaults(func=fn)
        return parser
    return create_parser_fn


def create_coords_generator_from_tiles_file(fp, logger=None):
    for line in fp:
        line = line.strip()
        if not line:
            continue
        coord = parse_expired_coord_string(line)
        if coord is None:
            if logger is not None:
                logger.warning('Could not parse coordinate from line: ' % line)
            continue
        yield coord


def lookup_formats(format_extensions):
    formats = []
    for extension in format_extensions:
        format = lookup_format_by_extension(extension)
        assert format is not None, 'Unknown extension: %s' % extension
        formats.append(format)
    return formats


def uniquify_generator(generator):
    s = set(generator)
    for tile in s:
        yield tile


def make_queue(queue_type, queue_name, redis_client,
               aws_access_key_id=None, aws_secret_access_key=None):
    if queue_type == 'sqs':
        return make_sqs_queue(queue_name, redis_client,
                              aws_access_key_id, aws_secret_access_key)
    elif queue_type == 'mem':
        from tilequeue.queue import MemoryQueue
        return MemoryQueue()
    elif queue_type == 'file':
        from tilequeue.queue import OutputFileQueue
        if os.path.exists(queue_name):
            assert os.path.isfile(queue_name), \
                'Could not create file queue. `./{}` is not a file!'.format(
                    queue_name)

        # The mode here is important: if `tilequeue seed` is being run, then
        # new tile coordinates will get appended to the queue file due to the
        # `a`. Otherwise, if it's something like `tilequeue process`,
        # coordinates will be read from the beginning of the file thanks to the
        # `+`.
        fp = open(queue_name, 'a+')
        return OutputFileQueue(fp)
    elif queue_type == 'stdout':
        # only support writing
        from tilequeue.queue import OutputFileQueue
        return OutputFileQueue(sys.stdout)
    elif queue_type == 'redis':
        from tilequeue.queue import make_redis_queue
        return make_redis_queue(redis_client, queue_name)
    else:
        raise ValueError('Unknown queue type: %s' % queue_type)


def make_redis_client(cfg):
    from redis import StrictRedis
    redis_client = StrictRedis(cfg.redis_host, cfg.redis_port, cfg.redis_db)
    return redis_client


def make_redis_cache_index(redis_client, cfg):
    if cfg.redis_type == 'redis_client':
        from tilequeue.cache import RedisCacheIndex
        redis_cache_index = RedisCacheIndex(
            redis_client, cfg.redis_cache_set_key)
        return redis_cache_index
    else:
        from tilequeue.cache import StubIndex
        return StubIndex()


def make_logger(cfg, logger_name):
    if getattr(cfg, 'logconfig') is not None:
        logging.config.fileConfig(cfg.logconfig)
    logger = logging.getLogger(logger_name)
    return logger


def make_seed_tile_generator(cfg):
    if cfg.seed_all_zoom_start is not None:
        assert cfg.seed_all_zoom_until is not None
        all_tiles = seed_tiles(cfg.seed_all_zoom_start,
                               cfg.seed_all_zoom_until)
    else:
        all_tiles = ()

    if cfg.seed_metro_extract_url:
        assert cfg.seed_metro_extract_zoom_start is not None
        assert cfg.seed_metro_extract_zoom_until is not None
        with closing(urlopen(cfg.seed_metro_extract_url)) as fp:
            # will raise a MetroExtractParseError on failure
            metro_extracts = parse_metro_extract(fp)

        city_filter = cfg.seed_metro_extract_cities
        if city_filter is not None:
            metro_extracts = [
                city for city in metro_extracts if city.city in city_filter]

        multiple_bounds = city_bounds(metro_extracts)
        metro_extract_tiles = tile_generator_for_multiple_bounds(
            multiple_bounds, cfg.seed_metro_extract_zoom_start,
            cfg.seed_metro_extract_zoom_until)
    else:
        metro_extract_tiles = ()

    if cfg.seed_top_tiles_url:
        assert cfg.seed_top_tiles_zoom_start is not None
        assert cfg.seed_top_tiles_zoom_until is not None
        with closing(urlopen(cfg.seed_top_tiles_url)) as fp:
            top_tiles = parse_top_tiles(
                fp, cfg.seed_top_tiles_zoom_start,
                cfg.seed_top_tiles_zoom_until)
    else:
        top_tiles = ()

    if cfg.seed_custom_bboxes:
        assert cfg.seed_custom_zoom_start is not None
        assert cfg.seed_custom_zoom_until is not None
        custom_tiles = tile_generator_for_multiple_bounds(
            cfg.seed_custom_bboxes, cfg.seed_custom_zoom_start,
            cfg.seed_custom_zoom_until)
    else:
        custom_tiles = ()

    combined_tiles = chain(
        all_tiles, metro_extract_tiles, top_tiles, custom_tiles)
    tile_generator = uniquify_generator(combined_tiles)

    return tile_generator


def tilequeue_drain(cfg, peripherals):
    queue = peripherals.queue
    logger = make_logger(cfg, 'drain')
    logger.info('Draining queue ...')
    n = queue.clear()
    logger.info('Draining queue ... done')
    logger.info('Removed %d messages' % n)


def explode_and_intersect(coord_ints, tiles_of_interest, until=0):
    next_coord_ints = coord_ints
    coord_ints_at_parent_zoom = set()
    while True:
        for coord_int in next_coord_ints:
            if coord_int in tiles_of_interest:
                yield coord_int
            zoom = zoom_mask & coord_int
            if zoom > until:
                parent_coord_int = coord_int_zoom_up(coord_int)
                coord_ints_at_parent_zoom.add(parent_coord_int)
        if not coord_ints_at_parent_zoom:
            return
        next_coord_ints = coord_ints_at_parent_zoom
        coord_ints_at_parent_zoom = set()


def coord_ints_from_paths(paths):
    coord_set = set()
    for path in paths:
        with open(path) as fp:
            coords = create_coords_generator_from_tiles_file(fp)
            for coord in coords:
                coord_int = coord_marshall_int(coord)
                coord_set.add(coord_int)
    return coord_set


def tilequeue_intersect(cfg, peripherals):
    logger = make_logger(cfg, 'intersect')
    logger.info("Intersecting expired tiles with tiles of interest")
    sqs_queue = peripherals.queue

    assert cfg.intersect_expired_tiles_location, \
        'Missing tiles expired-location configuration'
    assert os.path.isdir(cfg.intersect_expired_tiles_location), \
        'tiles expired-location is not a directory'

    file_names = os.listdir(cfg.intersect_expired_tiles_location)
    if not file_names:
        logger.info('No expired tiles found, terminating.')
        return
    file_names.sort()
    # cap the total number of files that we process in one shot
    # this will limit memory usage, as well as keep progress moving
    # along more consistently rather than bursts
    expired_tile_files_cap = 20
    file_names = file_names[:expired_tile_files_cap]
    expired_tile_paths = [os.path.join(cfg.intersect_expired_tiles_location, x)
                          for x in file_names]

    logger.info('Fetching tiles of interest ...')
    tiles_of_interest = peripherals.redis_cache_index.fetch_tiles_of_interest()
    logger.info('Fetching tiles of interest ... done')

    logger.info('Will process %d expired tile files.'
                % len(expired_tile_paths))

    for expired_tile_path in expired_tile_paths:
        stat_result = os.stat(expired_tile_path)
        file_size = stat_result.st_size
        file_size_in_kilobytes = file_size / 1024
        logger.info('Processing %s. Size: %dK' %
                    (expired_tile_path, file_size_in_kilobytes))

    # This will store all coords from all paths as integers in a
    # set. A set is used because if the same tile has been expired in
    # more than one file, we only process it once
    all_coord_ints_set = coord_ints_from_paths(expired_tile_paths)
    logger.info('Unique expired tiles read to process: %d' %
                len(all_coord_ints_set))

    # determine the list of coordinates we would want to enqueue
    coord_ints = explode_and_intersect(all_coord_ints_set, tiles_of_interest,
                                       until=cfg.intersect_zoom_until)
    coords = map(coord_unmarshall_int, coord_ints)

    # clamp number of threads between 5 and 20
    n_threads = max(min(len(expired_tile_paths), 20), 5)
    enqueuer = ThreadedEnqueuer(sqs_queue, n_threads, logger)
    n_queued, n_in_flight = enqueuer(coords)

    # print results
    for expired_tile_path in expired_tile_paths:
        logger.info('Processing complete: %s' % expired_tile_path)
        os.remove(expired_tile_path)
        logger.info('Removed: %s' % expired_tile_path)

    logger.info('%d tiles enqueued. %d tiles in flight.' %
                (n_queued, n_in_flight))

    logger.info('Intersection complete.')


def make_store(store_type, store_name, cfg):
    if store_type == 'directory':
        from tilequeue.store import make_tile_file_store
        return make_tile_file_store(store_name)

    elif store_type == 's3':
        from tilequeue.store import make_s3_store
        return make_s3_store(
            cfg.s3_bucket, cfg.aws_access_key_id, cfg.aws_secret_access_key,
            path=cfg.s3_path, reduced_redundancy=cfg.s3_reduced_redundancy,
            date_prefix=cfg.s3_date_prefix)

    else:
        raise ValueError('Unrecognized store type: `{}`'.format(store_type))


def _parse_postprocess_resources(post_process_item, cfg_path):
    resources_cfg = post_process_item.get('resources', {})
    resources = {}

    for resource_name, resource_cfg in resources_cfg.iteritems():
        resource_type = resource_cfg.get('type')
        init_fn_name = resource_cfg.get('init_fn')

        assert resource_type, 'Missing type in resource %r' \
            % resource_name
        assert init_fn_name, 'Missing init function name in ' \
            'resource %r' % resource_name

        try:
            fn = resolve(init_fn_name)

        except:
            raise Exception('Unable to init resource %r with function %r due '
                            'to %s' % (resource_name, init_fn_name,
                                       "".join(traceback.format_exception(
                                           *sys.exc_info()))))

        if resource_type == 'file':
            path = resource_cfg.get('path')
            assert path, 'Resource %r of type file is missing the ' \
                'path parameter' % resource_name

            with open(os.path.join(cfg_path, path), 'r') as fh:
                resources[resource_name] = fn(fh)

        else:
            raise Exception('Resource type %r is not supported'
                            % resource_type)

    return resources


def _bounds_pad_no_buf(bounds, meters_per_pixel):
    return bounds


def _create_query_bounds_pad_fn(buffer_cfg, layer_name):
    # because we aren't changing the queries to have different bounds
    # specifiers for each geometry type, we take the largest buffer
    # size and use that as the bounds.

    if not buffer_cfg:
        return _bounds_pad_no_buf

    largest_buf = 0
    for format_ext, format_cfg in buffer_cfg.items():
        format_layer_cfg = format_cfg.get('layer', {}).get(layer_name)
        format_geometry_cfg = format_cfg.get('geometry', {})
        if format_layer_cfg:
            for geometry_type, buffer_size in format_layer_cfg.items():
                largest_buf = max(largest_buf, buffer_size)
        if format_geometry_cfg:
            for geometry_type, buffer_size in format_geometry_cfg.items():
                largest_buf = max(largest_buf, buffer_size)

    if largest_buf == 0:
        return _bounds_pad_no_buf

    def bounds_pad(bounds, meters_per_pixel):
        offset = meters_per_pixel * largest_buf
        result = bounds_buffer(bounds, offset)
        return result

    return bounds_pad


def parse_layer_data(query_cfg, buffer_cfg, template_path, reload_templates,
                     cfg_path):
    if reload_templates:
        from tilequeue.query import DevJinjaQueryGenerator
    else:
        from tilequeue.query import JinjaQueryGenerator
    all_layer_names = query_cfg['all']
    layers_config = query_cfg['layers']
    post_process_config = query_cfg.get('post_process', [])
    layer_data = []
    all_layer_data = []
    post_process_data = []

    environment = Environment(loader=FileSystemLoader(template_path))
    environment.filters['geometry'] = jinja_filter_geometry
    environment.filters['bbox_filter'] = jinja_filter_bbox_filter
    environment.filters['bbox_intersection'] = jinja_filter_bbox_intersection
    environment.filters['bbox'] = jinja_filter_bbox

    for layer_name, layer_config in layers_config.items():
        template_name = layer_config['template']
        start_zoom = layer_config['start_zoom']
        area_threshold = int(layer_config.get('area-inclusion-threshold', 1))
        if reload_templates:
            query_generator = DevJinjaQueryGenerator(
                environment, template_name, start_zoom)
        else:
            template = environment.get_template(template_name)
            query_generator = JinjaQueryGenerator(template, start_zoom)
        layer_datum = dict(
            name=layer_name,
            query_generator=query_generator,
            is_clipped=layer_config.get('clip', True),
            clip_factor=layer_config.get('clip_factor', 1.0),
            geometry_types=layer_config['geometry_types'],
            transform_fn_names=layer_config.get('transform', []),
            sort_fn_name=layer_config.get('sort'),
            simplify_before_intersect=layer_config.get(
                'simplify_before_intersect', False),
            simplify_start=layer_config.get('simplify_start', 0),
            area_threshold=area_threshold,
            query_bounds_pad_fn=_create_query_bounds_pad_fn(
                buffer_cfg, layer_name),
        )
        layer_data.append(layer_datum)
        if layer_name in all_layer_names:
            all_layer_data.append(layer_datum)

    for post_process_item in post_process_config:
        fn_name = post_process_item.get('fn')
        assert fn_name, 'Missing post process config fn'

        params = post_process_item.get('params')
        if params is None:
            params = {}

        resources = _parse_postprocess_resources(post_process_item, cfg_path)

        post_process_data.append(dict(
            fn_name=fn_name,
            params=dict(params),
            resources=resources))

    return all_layer_data, layer_data, post_process_data


def tilequeue_process(cfg, peripherals):
    logger = make_logger(cfg, 'process')
    logger.warn('tilequeue processing started')

    assert os.path.exists(cfg.query_cfg), \
        'Invalid query config path'

    with open(cfg.query_cfg) as query_cfg_fp:
        query_cfg = yaml.load(query_cfg_fp)
    all_layer_data, layer_data, post_process_data = (
        parse_layer_data(
            query_cfg, cfg.buffer_cfg, cfg.template_path, cfg.reload_templates,
            os.path.dirname(cfg.query_cfg)))

    formats = lookup_formats(cfg.output_formats)

    sqs_queue = peripherals.queue

    store = make_store(cfg.store_type, cfg.s3_bucket, cfg)

    assert cfg.postgresql_conn_info, 'Missing postgresql connection info'

    n_cpu = multiprocessing.cpu_count()
    sqs_messages_per_batch = 10
    n_simultaneous_query_sets = cfg.n_simultaneous_query_sets
    if not n_simultaneous_query_sets:
        # default to number of databases configured
        n_simultaneous_query_sets = len(cfg.postgresql_conn_info['dbnames'])
    assert n_simultaneous_query_sets > 0
    default_queue_buffer_size = 256
    n_layers = len(all_layer_data)
    n_formats = len(formats)
    n_simultaneous_s3_storage = cfg.n_simultaneous_s3_storage
    if not n_simultaneous_s3_storage:
        n_simultaneous_s3_storage = max(n_cpu / 2, 1)
    assert n_simultaneous_s3_storage > 0

    # thread pool used for queries and uploading to s3
    n_total_needed_query = n_layers * n_simultaneous_query_sets
    n_total_needed_s3 = n_formats * n_simultaneous_s3_storage
    n_total_needed = n_total_needed_query + n_total_needed_s3
    n_max_io_workers = 50
    n_io_workers = min(n_total_needed, n_max_io_workers)
    io_pool = ThreadPool(n_io_workers)

    feature_fetcher = DataFetcher(cfg.postgresql_conn_info, all_layer_data,
                                  io_pool, n_layers)

    # create all queues used to manage pipeline

    sqs_input_queue_buffer_size = sqs_messages_per_batch
    # holds coord messages from sqs
    sqs_input_queue = Queue.Queue(sqs_input_queue_buffer_size)

    # holds raw sql results - no filtering or processing done on them
    sql_data_fetch_queue = multiprocessing.Queue(default_queue_buffer_size)

    # holds data after it has been filtered and processed
    # this is where the cpu intensive part of the operation will happen
    # the results will be data that is formatted for each necessary format
    processor_queue = multiprocessing.Queue(default_queue_buffer_size)

    # holds data after it has been sent to s3
    s3_store_queue = Queue.Queue(default_queue_buffer_size)

    # create worker threads/processes
    thread_sqs_queue_reader_stop = threading.Event()
    sqs_queue_reader = SqsQueueReader(sqs_queue, sqs_input_queue, logger,
                                      thread_sqs_queue_reader_stop)

    data_fetch = DataFetch(
        feature_fetcher, sqs_input_queue, sql_data_fetch_queue, io_pool,
        peripherals.redis_cache_index, logger)

    data_processor = ProcessAndFormatData(
        post_process_data, formats, sql_data_fetch_queue, processor_queue,
        cfg.layers_to_format, cfg.buffer_cfg, logger)

    s3_storage = S3Storage(processor_queue, s3_store_queue, io_pool,
                           store, logger)

    thread_sqs_writer_stop = threading.Event()
    sqs_queue_writer = SqsQueueWriter(sqs_queue, s3_store_queue, logger,
                                      thread_sqs_writer_stop)

    def create_and_start_thread(fn, *args):
        t = threading.Thread(target=fn, args=args)
        t.start()
        return t

    thread_sqs_queue_reader = create_and_start_thread(sqs_queue_reader)

    threads_data_fetch = []
    threads_data_fetch_stop = []
    for i in range(n_simultaneous_query_sets):
        thread_data_fetch_stop = threading.Event()
        thread_data_fetch = create_and_start_thread(data_fetch,
                                                    thread_data_fetch_stop)
        threads_data_fetch.append(thread_data_fetch)
        threads_data_fetch_stop.append(thread_data_fetch_stop)

    # create a data processor per cpu
    n_data_processors = n_cpu
    data_processors = []
    data_processors_stop = []
    for i in range(n_data_processors):
        data_processor_stop = multiprocessing.Event()
        process_data_processor = multiprocessing.Process(
            target=data_processor, args=(data_processor_stop,))
        process_data_processor.start()
        data_processors.append(process_data_processor)
        data_processors_stop.append(data_processor_stop)

    threads_s3_storage = []
    threads_s3_storage_stop = []
    for i in range(n_simultaneous_s3_storage):
        thread_s3_storage_stop = threading.Event()
        thread_s3_storage = create_and_start_thread(s3_storage,
                                                    thread_s3_storage_stop)
        threads_s3_storage.append(thread_s3_storage)
        threads_s3_storage_stop.append(thread_s3_storage_stop)

    thread_sqs_writer = create_and_start_thread(sqs_queue_writer)

    if cfg.log_queue_sizes:
        assert(cfg.log_queue_sizes_interval_seconds > 0)
        queue_data = (
            (sqs_input_queue, 'sqs'),
            (sql_data_fetch_queue, 'sql'),
            (processor_queue, 'proc'),
            (s3_store_queue, 's3'),
        )
        queue_printer_thread_stop = threading.Event()
        queue_printer = QueuePrint(
            cfg.log_queue_sizes_interval_seconds, queue_data, logger,
            queue_printer_thread_stop)
        queue_printer_thread = create_and_start_thread(queue_printer)
    else:
        queue_printer_thread = None
        queue_printer_thread_stop = None

    def stop_all_workers(signum, stack):
        logger.warn('tilequeue processing shutdown ...')

        logger.info('requesting all workers (threads and processes) stop ...')

        # each worker guards its read loop with an event object
        # ask all these to stop first

        thread_sqs_queue_reader_stop.set()
        for thread_data_fetch_stop in threads_data_fetch_stop:
            thread_data_fetch_stop.set()
        for data_processor_stop in data_processors_stop:
            data_processor_stop.set()
        for thread_s3_storage_stop in threads_s3_storage_stop:
            thread_s3_storage_stop.set()
        thread_sqs_writer_stop.set()

        if queue_printer_thread_stop:
            queue_printer_thread_stop.set()

        logger.info('requesting all workers (threads and processes) stop ... '
                    'done')

        # Once workers receive a stop event, they will keep reading
        # from their queues until they receive a sentinel value. This
        # is mandatory so that no messages will remain on queues when
        # asked to join. Otherwise, we never terminate.

        logger.info('joining all workers ...')

        logger.info('joining sqs queue reader ...')
        thread_sqs_queue_reader.join()
        logger.info('joining sqs queue reader ... done')
        logger.info('enqueueing sentinels for data fetchers ...')
        for i in range(len(threads_data_fetch)):
            sqs_input_queue.put(None)
        logger.info('enqueueing sentinels for data fetchers ... done')
        logger.info('joining data fetchers ...')
        for thread_data_fetch in threads_data_fetch:
            thread_data_fetch.join()
        logger.info('joining data fetchers ... done')
        logger.info('enqueueing sentinels for data processors ...')
        for i in range(len(data_processors)):
            sql_data_fetch_queue.put(None)
        logger.info('enqueueing sentinels for data processors ... done')
        logger.info('joining data processors ...')
        for data_processor in data_processors:
            data_processor.join()
        logger.info('joining data processors ... done')
        logger.info('enqueueing sentinels for s3 storage ...')
        for i in range(len(threads_s3_storage)):
            processor_queue.put(None)
        logger.info('enqueueing sentinels for s3 storage ... done')
        logger.info('joining s3 storage ...')
        for thread_s3_storage in threads_s3_storage:
            thread_s3_storage.join()
        logger.info('joining s3 storage ... done')
        logger.info('enqueueing sentinel for sqs queue writer ...')
        s3_store_queue.put(None)
        logger.info('enqueueing sentinel for sqs queue writer ... done')
        logger.info('joining sqs queue writer ...')
        thread_sqs_writer.join()
        logger.info('joining sqs queue writer ... done')
        if queue_printer_thread:
            logger.info('joining queue printer ...')
            queue_printer_thread.join()
            logger.info('joining queue printer ... done')

        logger.info('joining all workers ... done')

        logger.info('joining io pool ...')
        io_pool.close()
        io_pool.join()
        logger.info('joining io pool ... done')

        logger.info('joining multiprocess data fetch queue ...')
        sql_data_fetch_queue.close()
        sql_data_fetch_queue.join_thread()
        logger.info('joining multiprocess data fetch queue ... done')

        logger.info('joining multiprocess process queue ...')
        processor_queue.close()
        processor_queue.join_thread()
        logger.info('joining multiprocess process queue ... done')

        logger.warn('tilequeue processing shutdown ... done')
        sys.exit(0)

    signal.signal(signal.SIGTERM, stop_all_workers)
    signal.signal(signal.SIGINT, stop_all_workers)
    signal.signal(signal.SIGQUIT, stop_all_workers)

    logger.warn('all tilequeue threads and processes started')

    # this is necessary for the main thread to receive signals
    # when joining on threads/processes, the signal is never received
    # http://www.luke.maurits.id.au/blog/post/threads-and-signals-in-python.html
    while True:
        time.sleep(1024)


class ThreadedEnqueuer(object):

    def __init__(self, sqs_queue, n_threads, logger=None):
        self.sqs_queue = sqs_queue
        self.n_threads = n_threads
        self.logger = logger

    def _log(self, level, msg):
        if self.logger is not None:
            self.logger.log(level, msg)

    def __call__(self, coords):
        # 10 is the number of messages that sqs can send at once
        buf_size = 10
        thread_work_queue = Queue.Queue(self.n_threads * buf_size)
        thread_results_queue = Queue.Queue(self.n_threads)
        threads = []

        def enqueue():
            buf = []
            done = False
            total_queued = 0
            total_in_flight = 0
            while not done:
                coord = thread_work_queue.get()
                if coord is None:
                    done = True
                else:
                    buf.append(coord)
                if len(buf) >= buf_size or (done and buf):
                    n_queued, n_in_flight = self.sqs_queue.enqueue_batch(buf)
                    total_queued += n_queued
                    total_in_flight += n_in_flight
                    del buf[:]
            thread_results_queue.put((total_queued, total_in_flight))

        # first start up all the threads
        self._log(logging.INFO, 'Starting %d enqueueing threads ...' %
                  self.n_threads)
        for i in xrange(self.n_threads):
            thread = threading.Thread(target=enqueue)
            thread.start()
            threads.append(thread)
        self._log(logging.INFO, 'Starting %d enqueueing threads ... done' %
                  self.n_threads)

        # queue up the work
        self._log(logging.INFO, 'Starting to enqueue coordinates ...')
        for coord in coords:
            thread_work_queue.put(coord)

        # tell the threads to stop
        total_queued_across_threads = 0
        total_in_flight_across_threads = 0
        for thread in threads:
            thread_work_queue.put(None)
        for thread in threads:
            # join with the thread
            thread.join()
            # and also get the results from that thread
            n_queued, n_in_flight = thread_results_queue.get()
            total_queued_across_threads += n_queued
            total_in_flight_across_threads += n_in_flight

        n_proc = total_queued_across_threads + total_in_flight_across_threads
        self._log(logging.INFO, 'Starting to enqueue coordinates ... done')
        self._log(logging.INFO, '%d processed - %d enqueued - %d in flight' %
                  (n_proc, total_queued_across_threads,
                   total_in_flight_across_threads))

        return total_queued_across_threads, total_in_flight_across_threads


def tilequeue_seed(cfg, peripherals):
    logger = make_logger(cfg, 'seed')
    logger.info('Seeding tiles ...')
    queue = peripherals.queue
    # suppresses checking the in flight list while seeding
    queue.is_seeding = True
    redis_cache_index = peripherals.redis_cache_index
    enqueuer = ThreadedEnqueuer(queue, cfg.seed_n_threads, logger)

    # based on cfg, create tile generator
    tile_generator = make_seed_tile_generator(cfg)
    # realize all tiles to simplify (they get realized anyway to
    # eliminate dupes)
    logger.info('Generating seed list ...')
    coords = list(tile_generator)
    logger.info('Generating seed list ... done')
    n_tiles = len(coords)
    logger.info('Will seed %d tiles' % n_tiles)

    # updating sqs and updating redis happen in background threads
    def redis_add():
        redis_cache_index.index_coords(coords)

    def sqs_enqueue():
        enqueuer(coords)

    logger.info('Sqs ... ')
    thread_enqueue = threading.Thread(target=sqs_enqueue)
    thread_enqueue.start()

    if cfg.seed_should_add_to_tiles_of_interest:
        logger.info('Tiles of interest ...')
        thread_redis = threading.Thread(target=redis_add)
        thread_redis.start()

    if cfg.seed_should_add_to_tiles_of_interest:
        thread_redis.join()
        logger.info('Tiles of interest ... done')

    thread_enqueue.join()
    logger.info('Sqs ... done')
    logger.info('Seeding tiles ... done')


def tilequeue_enqueue_tiles_of_interest(cfg, peripherals):
    logger = make_logger(cfg, 'enqueue_tiles_of_interest')
    logger.info('Enqueueing tiles of interest')

    sqs_queue = peripherals.queue
    logger.info('Fetching tiles of interest ...')
    tiles_of_interest = peripherals.redis_cache_index.fetch_tiles_of_interest()
    n_toi = len(tiles_of_interest)
    logger.info('Fetching tiles of interest ... done')

    coords = []
    for coord_int in tiles_of_interest:
        coord = coord_unmarshall_int(coord_int)
        if coord.zoom <= 16:
            coords.append(coord)

    enqueuer = ThreadedEnqueuer(sqs_queue, cfg.seed_n_threads, logger)
    n_queued, n_in_flight = enqueuer(coords)

    logger.info('%d enqueued - %d in flight' % (n_queued, n_in_flight))
    logger.info('%d tiles of interest processed' % n_toi)


def tilequeue_tile_sizes(cfg, peripherals):
    # find averages, counts, and medians for metro extract tiles
    assert cfg.metro_extract_url
    with closing(urlopen(cfg.metro_extract_url)) as fp:
        metro_extracts = parse_metro_extract(fp)

    # zooms to get sizes for, inclusive
    zoom_start = 11
    zoom_until = 15

    bucket_name = cfg.s3_bucket

    formats = lookup_formats(cfg.output_formats)

    work_buffer_size = 1000
    work = Queue.Queue(work_buffer_size)

    from boto import connect_s3
    from boto.s3.bucket import Bucket
    s3_conn = connect_s3(cfg.aws_access_key_id, cfg.aws_secret_access_key)
    bucket = Bucket(s3_conn, bucket_name)

    lock = threading.Lock()

    def new_total_count():
        return dict(
            sum=0,
            n=0,
            elts=[],
        )

    region_counts = {}
    city_counts = {}
    zoom_counts = {}
    format_counts = {}
    grand_total_count = new_total_count()

    def update_total_count(total_count, size):
        total_count['sum'] += size
        total_count['n'] += 1
        total_count['elts'].append(size)

    def add_size(metro, coord, format, size):
        with lock:
            region_count = region_counts.get(metro.region)
            if region_count is None:
                region_counts[metro.region] = region_count = new_total_count()
            update_total_count(region_count, size)

            city_count = city_counts.get(metro.city)
            if city_count is None:
                city_counts[metro.city] = city_count = new_total_count()
            update_total_count(city_count, size)

            zoom_count = zoom_counts.get(coord.zoom)
            if zoom_count is None:
                zoom_counts[coord.zoom] = zoom_count = new_total_count()
            update_total_count(zoom_count, size)

            format_count = format_counts.get(format.extension)
            if format_count is None:
                format_counts[format.extension] = format_count = \
                    new_total_count()
            update_total_count(format_count, size)

            update_total_count(grand_total_count, size)

    from tilequeue.tile import serialize_coord

    def process_work_data():
        while True:
            work_data = work.get()
            if work_data is None:
                break
            coord = work_data['coord']
            format = work_data['format']
            key_path = 'osm/all/%s.%s' % (
                serialize_coord(coord), format.extension)
            key = bucket.get_key(key_path)
            # this shouldn't practically happen
            if key is None:
                continue
            size = key.size
            add_size(work_data['metro'], coord, format, size)

    # start all threads
    n_threads = 50
    worker_threads = []
    for i in range(n_threads):
        worker_thread = threading.Thread(target=process_work_data)
        worker_thread.start()
        worker_threads.append(worker_thread)

    # enqueue all work
    for metro_extract in metro_extracts:
        metro_tiles = tile_generator_for_single_bounds(
            metro_extract.bounds, zoom_start, zoom_until)
        for tile in metro_tiles:
            for format in formats:
                work_data = dict(
                    metro=metro_extract,
                    coord=tile,
                    format=format,
                )
                work.put(work_data)

    # tell workers to stop
    for i in range(n_threads):
        work.put(None)
    for worker_thread in worker_threads:
        worker_thread.join()

    def calc_median(elts):
        if not elts:
            return -1
        elts.sort()
        n = len(elts)
        middle = n / 2
        if n % 2 == 0:
            return (float(elts[middle]) + float(elts[middle + 1])) / float(2)
        else:
            return elts[middle]

    def calc_avg(total, n):
        if n == 0:
            return -1
        return float(total) / float(n)

    def format_commas(x):
        return '{:,}'.format(x)

    def format_kilos(size_in_bytes):
        kilos = int(float(size_in_bytes) / float(1000))
        kilos_commas = format_commas(kilos)
        return '%sK' % kilos_commas

    # print results
    def print_count(label, total_count):
        median = calc_median(total_count['elts'])
        avg = calc_avg(total_count['sum'], total_count['n'])
        if label:
            label_str = '%s -> ' % label
        else:
            label_str = ''
        print '%scount: %s - avg: %s - median: %s' % (
            label_str, format_commas(total_count['n']),
            format_kilos(avg), format_kilos(median))

    print 'Regions'
    print '*' * 80
    region_counts = sorted(region_counts.iteritems())
    for region_name, region_count in region_counts:
        print_count(region_name, region_count)

    print '\n\n'
    print 'Cities'
    print '*' * 80
    city_counts = sorted(city_counts.iteritems())
    for city_name, city_count in city_counts:
        print_count(city_name, city_count)

    print '\n\n'
    print 'Zooms'
    print '*' * 80
    zoom_counts = sorted(zoom_counts.iteritems())
    for zoom, zoom_count in zoom_counts:
        print_count(zoom, zoom_count)

    print '\n\n'
    print 'Formats'
    print '*' * 80
    format_counts = sorted(format_counts.iteritems())
    for format_extension, format_count in format_counts:
        print_count(format_extension, format_count)

    print '\n\n'
    print 'Grand total'
    print '*' * 80
    print_count(None, grand_total_count)


def tilequeue_process_wof_neighbourhoods(cfg, peripherals):
    from tilequeue.wof import make_wof_model
    from tilequeue.wof import make_wof_url_neighbourhood_fetcher
    from tilequeue.wof import make_wof_processor

    wof_cfg = cfg.wof
    assert wof_cfg, 'Missing wof config'

    logger = make_logger(cfg, 'wof_process_neighbourhoods')
    logger.info('WOF process neighbourhoods run started')

    n_raw_neighbourhood_fetch_threads = 5
    fetcher = make_wof_url_neighbourhood_fetcher(
        wof_cfg['neighbourhoods-meta-url'],
        wof_cfg['microhoods-meta-url'],
        wof_cfg['macrohoods-meta-url'],
        wof_cfg['boroughs-meta-url'],
        wof_cfg['data-prefix-url'],
        n_raw_neighbourhood_fetch_threads,
        wof_cfg.get('max-retries', 0)
    )
    model = make_wof_model(wof_cfg['postgresql'])

    n_enqueue_threads = 20
    current_date = datetime.date.today()
    processor = make_wof_processor(
        fetcher, model, peripherals.redis_cache_index, peripherals.queue,
        n_enqueue_threads, logger, current_date)

    logger.info('Processing ...')
    processor()
    logger.info('Processing ... done')
    logger.info('WOF process neighbourhoods run completed')


def tilequeue_initial_load_wof_neighbourhoods(cfg, peripherals):
    from tilequeue.wof import make_wof_initial_loader
    from tilequeue.wof import make_wof_model
    from tilequeue.wof import make_wof_filesystem_neighbourhood_fetcher

    wof_cfg = cfg.wof
    assert wof_cfg, 'Missing wof config'

    logger = make_logger(cfg, 'wof_process_neighbourhoods')

    logger.info('WOF initial neighbourhoods load run started')

    n_raw_neighbourhood_fetch_threads = 50
    fetcher = make_wof_filesystem_neighbourhood_fetcher(
        wof_cfg['data-path'],
        n_raw_neighbourhood_fetch_threads,
    )

    model = make_wof_model(wof_cfg['postgresql'])

    loader = make_wof_initial_loader(fetcher, model, logger)

    logger.info('Loading ...')
    loader()
    logger.info('Loading ... done')


class TileArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        sys.stderr.write('error: %s\n' % message)
        self.print_help()
        sys.exit(2)


def tilequeue_main(argv_args=None):
    if argv_args is None:
        argv_args = sys.argv[1:]

    parser = TileArgumentParser()
    subparsers = parser.add_subparsers()

    parser_config = (
        ('process', create_command_parser(tilequeue_process)),
        ('seed', create_command_parser(tilequeue_seed)),
        ('drain', create_command_parser(tilequeue_drain)),
        ('intersect', create_command_parser(tilequeue_intersect)),
        ('enqueue-tiles-of-interest',
         create_command_parser(tilequeue_enqueue_tiles_of_interest)),
        ('tile-size', create_command_parser(tilequeue_tile_sizes)),
        ('wof-process-neighbourhoods', create_command_parser(
            tilequeue_process_wof_neighbourhoods)),
        ('wof-load-initial-neighbourhoods', create_command_parser(
            tilequeue_initial_load_wof_neighbourhoods)),
    )
    for parser_name, parser_func in parser_config:
        subparser = subparsers.add_parser(parser_name)
        parser_func(subparser)

    args = parser.parse_args(argv_args)
    assert os.path.exists(args.config), \
        'Config file {} does not exist!'.format(args.config)
    cfg = make_config_from_argparse(args.config)
    redis_client = make_redis_client(cfg)
    Peripherals = namedtuple('Peripherals', 'redis_cache_index queue')
    queue = make_queue(
        cfg.queue_type, cfg.queue_name, redis_client,
        aws_access_key_id=cfg.aws_access_key_id,
        aws_secret_access_key=cfg.aws_secret_access_key)
    peripherals = Peripherals(make_redis_cache_index(redis_client, cfg), queue)
    args.func(cfg, peripherals)
