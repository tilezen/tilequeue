from collections import defaultdict
from collections import Iterable
from collections import namedtuple
from contextlib import closing
from itertools import chain
from jinja2 import Environment
from jinja2 import FileSystemLoader
from multiprocessing.pool import ThreadPool
from tilequeue.config import create_query_bounds_pad_fn
from tilequeue.config import make_config_from_argparse
from tilequeue.format import lookup_format_by_extension
from tilequeue.metro_extract import city_bounds
from tilequeue.metro_extract import parse_metro_extract
from tilequeue.query import DataFetcher
from tilequeue.query import jinja_filter_bbox
from tilequeue.query import jinja_filter_bbox_filter
from tilequeue.query import jinja_filter_bbox_intersection
from tilequeue.query import jinja_filter_bbox_overlaps
from tilequeue.query import jinja_filter_bbox_padded_intersection
from tilequeue.query import jinja_filter_geometry
from tilequeue.queue import make_sqs_queue
from tilequeue.tile import coord_int_zoom_up
from tilequeue.tile import coord_is_valid
from tilequeue.tile import coord_marshall_int
from tilequeue.tile import coord_unmarshall_int
from tilequeue.tile import create_coord
from tilequeue.tile import deserialize_coord
from tilequeue.tile import parse_expired_coord_string
from tilequeue.tile import seed_tiles
from tilequeue.tile import tile_generator_for_multiple_bounds
from tilequeue.tile import tile_generator_for_single_bounds
from tilequeue.tile import zoom_mask
from tilequeue.toi import load_set_from_fp
from tilequeue.toi import save_set_to_fp
from tilequeue.top_tiles import parse_top_tiles
from tilequeue.utils import grouper
from tilequeue.utils import parse_log_file
from tilequeue.worker import DataFetch
from tilequeue.worker import ProcessAndFormatData
from tilequeue.worker import QueuePrint
from tilequeue.worker import S3Storage
from tilequeue.worker import SqsQueueReader
from tilequeue.worker import SqsQueueWriter
from tilequeue.postgresql import DBAffinityConnectionsNoLimit
from urllib2 import urlopen
from zope.dottedname.resolve import resolve
import argparse
import datetime
import logging
import logging.config
import multiprocessing
import operator
import os
import os.path
import Queue
import signal
import sys
import threading
import time
import traceback
import yaml


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


class GetSqsQueueNameForZoom(object):

    def __init__(self, zoom_queue_table):
        self.zoom_queue_table = zoom_queue_table

    def __call__(self, zoom):
        assert isinstance(zoom, (int, long))
        assert 0 <= zoom <= 20
        result = self.zoom_queue_table.get(zoom)
        assert result is not None, 'No queue name found for zoom: %d' % zoom
        return result


def make_get_queue_name_for_zoom(zoom_queue_map_cfg, queue_names):
    zoom_to_queue_name_table = {}

    for zoom_range, queue_name in zoom_queue_map_cfg.items():
        assert queue_name in queue_names

        assert '-' in zoom_range, 'Invalid zoom range: %s' % zoom_range
        zoom_fields = zoom_range.split('-')
        assert len(zoom_fields) == 2, 'Invalid zoom range: %s' % zoom_range
        zoom_start_str, zoom_until_str = zoom_fields
        try:
            zoom_start = int(zoom_start_str)
            zoom_until = int(zoom_until_str)
        except (ValueError, KeyError):
            assert not 'Invalid zoom range: %s' % zoom_range

        assert (0 <= zoom_start <= 20 and
                0 <= zoom_until <= 20 and
                zoom_start <= zoom_until), \
            'Invalid zoom range: %s' % zoom_range

        for i in range(zoom_start, zoom_until + 1):
            assert i not in zoom_to_queue_name_table, \
                'Overlapping zoom range: %s' % zoom_range
            zoom_to_queue_name_table[i] = queue_name

    result = GetSqsQueueNameForZoom(zoom_to_queue_name_table)
    return result


def make_queue(queue_type, queue_name, queue_cfg, redis_client,
               aws_access_key_id=None, aws_secret_access_key=None):
    if queue_type == 'multisqs':
        from tilequeue.queue import make_multi_sqs_queue
        assert isinstance(queue_name, list)
        queue_names = queue_name

        zoom_queue_map_cfg = queue_cfg.get('zoom-queue-map')
        assert zoom_queue_map_cfg, \
            'Missing zoom-queue-map config for multiqueue'
        assert isinstance(zoom_queue_map_cfg, dict), \
            'zoom-queue-map cfg must be a map'

        get_queue_idx_for_zoom = make_get_queue_name_for_zoom(
            zoom_queue_map_cfg, queue_names)
        result = make_multi_sqs_queue(
            queue_names, get_queue_idx_for_zoom, redis_client)
        return result

    elif queue_type == 'sqs':
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


def make_toi_helper(cfg):
    if cfg.toi_store_type == 's3':
        from tilequeue.toi import S3TilesOfInterestSet
        return S3TilesOfInterestSet(
            cfg.toi_store_s3_bucket,
            cfg.toi_store_s3_key,
        )
    elif cfg.toi_store_type == 'file':
        from tilequeue.toi import FileTilesOfInterestSet
        return FileTilesOfInterestSet(
            cfg.toi_store_file_name,
        )


def make_redis_client(cfg):
    from redis import StrictRedis
    redis_client = StrictRedis(cfg.redis_host, cfg.redis_port, cfg.redis_db)
    return redis_client


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

    if cfg.seed_unique:
        tile_generator = uniquify_generator(combined_tiles)
    else:
        tile_generator = combined_tiles

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

    total_coord_ints = []

    # to capture metrics
    total = 0
    hits = 0
    misses = 0

    while True:

        for coord_int in next_coord_ints:

            total += 1
            if coord_int in tiles_of_interest:
                hits += 1
                total_coord_ints.append(coord_int)
            else:
                misses += 1

            zoom = zoom_mask & coord_int
            if zoom > until:
                parent_coord_int = coord_int_zoom_up(coord_int)
                coord_ints_at_parent_zoom.add(parent_coord_int)

        if not coord_ints_at_parent_zoom:
            break

        next_coord_ints = coord_ints_at_parent_zoom
        coord_ints_at_parent_zoom = set()

    metrics = dict(
        total=total,
        hits=hits,
        misses=misses,
    )
    return total_coord_ints, metrics


def coord_ints_from_paths(paths):
    coord_set = set()
    path_counts = []
    for path in paths:
        path_count = 0
        with open(path) as fp:
            coords = create_coords_generator_from_tiles_file(fp)
            for coord in coords:
                coord_int = coord_marshall_int(coord)
                coord_set.add(coord_int)
                path_count += 1
            path_counts.append((path, path_count))
    result = dict(
        coord_set=coord_set,
        path_counts=path_counts,
    )
    return result


def tilequeue_intersect(cfg, peripherals):
    logger = make_logger(cfg, 'intersect')
    logger.info("Intersecting expired tiles with tiles of interest")
    sqs_queue = peripherals.queue

    assert cfg.intersect_expired_tiles_location, \
        'Missing tiles expired-location configuration'
    assert os.path.exists(cfg.intersect_expired_tiles_location), \
        'tiles expired-location does not exist'

    if os.path.isdir(cfg.intersect_expired_tiles_location):
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
        expired_tile_paths = \
            [os.path.join(cfg.intersect_expired_tiles_location, x)
             for x in file_names]
    else:
        expired_tile_paths = [cfg.intersect_expired_tiles_location]

    logger.info('Fetching tiles of interest ...')
    tiles_of_interest = peripherals.toi.fetch_tiles_of_interest()
    logger.info('Fetching tiles of interest ... done')

    logger.info('Will process %d expired tile files.'
                % len(expired_tile_paths))

    # This will store all coords from all paths as integers in a
    # set. A set is used because if the same tile has been expired in
    # more than one file, we only process it once
    coord_ints_path_result = coord_ints_from_paths(expired_tile_paths)
    all_coord_ints_set = coord_ints_path_result['coord_set']
    logger.info('Unique expired tiles read to process: %d' %
                len(all_coord_ints_set))

    # determine the list of coordinates we would want to enqueue
    coord_ints, intersect_metrics = explode_and_intersect(
        all_coord_ints_set, tiles_of_interest, until=cfg.intersect_zoom_until)
    coords = map(coord_unmarshall_int, coord_ints)

    # clamp number of threads between 5 and 20
    n_threads = max(min(len(expired_tile_paths), 20), 5)
    enqueuer = ThreadedEnqueuer(sqs_queue, n_threads, logger)
    n_queued, n_in_flight = enqueuer(coords)

    # print counts per file
    for path, count in coord_ints_path_result['path_counts']:
        logger.info('Path count: %s %d' % (path, count))

    # print metrics across intersection
    logger.info(
        'Intersect metrics: toi(%d) - candidates(%d) - hits(%d) - misses(%d)'
        ' - enqueued(%d) - in-flight(%d) - expiry-tiles(%d)' %
        (len(tiles_of_interest),
         intersect_metrics['total'],
         intersect_metrics['hits'],
         intersect_metrics['misses'],
         n_queued,
         n_in_flight,
         len(all_coord_ints_set),
         ))

    # print and remove expiry files
    for expired_tile_path in expired_tile_paths:
        logger.info('Processing complete: %s' % expired_tile_path)
        os.remove(expired_tile_path)
        logger.info('Removed: %s' % expired_tile_path)

    logger.info('Intersection complete.')


def make_store(store_type, store_name, cfg):
    if store_type == 'directory':
        from tilequeue.store import make_tile_file_store
        return make_tile_file_store(cfg.s3_path or store_name)

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
    environment.filters['bbox_padded_intersection'] = (
        jinja_filter_bbox_padded_intersection)
    environment.filters['bbox'] = jinja_filter_bbox
    environment.filters['bbox_overlaps'] = jinja_filter_bbox_overlaps

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
            query_bounds_pad_fn=create_query_bounds_pad_fn(
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
    # reduce queue size when we're rendering metatiles to try and avoid the
    # geometry waiting to be processed from taking up all the RAM!
    size_sqr = (cfg.metatile_size or 1)**2
    default_queue_buffer_size = max(1, 16 / size_sqr)
    sql_queue_buffer_size = cfg.sql_queue_buffer_size or \
        default_queue_buffer_size
    proc_queue_buffer_size = cfg.proc_queue_buffer_size or \
        default_queue_buffer_size
    s3_queue_buffer_size = cfg.s3_queue_buffer_size or \
        default_queue_buffer_size
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
    sql_data_fetch_queue = multiprocessing.Queue(sql_queue_buffer_size)

    # holds data after it has been filtered and processed
    # this is where the cpu intensive part of the operation will happen
    # the results will be data that is formatted for each necessary format
    processor_queue = multiprocessing.Queue(proc_queue_buffer_size)

    # holds data after it has been sent to s3
    s3_store_queue = Queue.Queue(s3_queue_buffer_size)

    # create worker threads/processes
    thread_sqs_queue_reader_stop = threading.Event()
    sqs_queue_reader = SqsQueueReader(
        sqs_queue, sqs_input_queue, logger, thread_sqs_queue_reader_stop,
        cfg.max_zoom)

    data_fetch = DataFetch(
        feature_fetcher, sqs_input_queue, sql_data_fetch_queue, io_pool,
        logger, cfg.metatile_zoom, cfg.max_zoom)

    data_processor = ProcessAndFormatData(
        post_process_data, formats, sql_data_fetch_queue, processor_queue,
        cfg.buffer_cfg, logger)

    s3_storage = S3Storage(processor_queue, s3_store_queue, io_pool, store,
                           logger, cfg.metatile_size)

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

    def __call__(self, coords_iter_or_queue):
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

        if isinstance(coords_iter_or_queue, Iterable):
            for coord in coords_iter_or_queue:
                thread_work_queue.put(coord)
        else:
            assert isinstance(coords_iter_or_queue, Queue.Queue), \
                'Unknown coords_iter_or_queue: %s' % coords_iter_or_queue
            while True:
                coord = coords_iter_or_queue.get()
                if coord is None:
                    break
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
    tile_queue = peripherals.queue
    # suppresses checking the in flight list while seeding
    tile_queue.is_seeding = True
    enqueuer = ThreadedEnqueuer(tile_queue, cfg.seed_n_threads, logger)

    # based on cfg, create tile generator
    tile_generator = make_seed_tile_generator(cfg)

    queue_buf_size = 1024
    tile_queue_queue = Queue.Queue(queue_buf_size)

    # updating sqs happens in background threads
    def tile_queue_enqueue():
        enqueuer(tile_queue_queue)

    logger.info('Enqueueing to SQS ... ')
    thread_enqueue = threading.Thread(target=tile_queue_enqueue)
    thread_enqueue.start()

    n_coords = 0
    for coord in tile_generator:
        tile_queue_queue.put(coord)
        n_coords += 1
        if n_coords % 100000 == 0:
            logger.info('%d enqueued' % n_coords)

    tile_queue_queue.put(None)

    thread_enqueue.join()
    logger.info('Enqueueing to SQS ... done')

    if cfg.seed_should_add_to_tiles_of_interest:
        logger.info('Adding to Tiles of Interest ... ')

        if (cfg.toi_store_type == 'file' and
                not os.path.exists(cfg.toi_store_file_name)):
            toi_set = set()
        else:
            toi_set = peripherals.toi.fetch_tiles_of_interest()

        tile_generator = make_seed_tile_generator(cfg)
        for coord in tile_generator:
            coord_int = coord_marshall_int(coord)
            toi_set.add(coord_int)

        peripherals.toi.set_tiles_of_interest(toi_set)
        emit_toi_stats(toi_set, peripherals)

        logger.info('Adding to Tiles of Interest ... done')

    logger.info('Seeding tiles ... done')
    logger.info('%d coordinates enqueued' % n_coords)


def tilequeue_enqueue_tiles_of_interest(cfg, peripherals):
    logger = make_logger(cfg, 'enqueue_tiles_of_interest')
    logger.info('Enqueueing tiles of interest')

    sqs_queue = peripherals.queue
    logger.info('Fetching tiles of interest ...')
    tiles_of_interest = peripherals.toi.fetch_tiles_of_interest()
    n_toi = len(tiles_of_interest)
    logger.info('Fetching tiles of interest ... done')

    coords = []
    for coord_int in tiles_of_interest:
        coord = coord_unmarshall_int(coord_int)
        if coord.zoom <= cfg.max_zoom:
            coords.append(coord)

    enqueuer = ThreadedEnqueuer(sqs_queue, cfg.seed_n_threads, logger)
    n_queued, n_in_flight = enqueuer(coords)

    logger.info('%d enqueued - %d in flight' % (n_queued, n_in_flight))
    logger.info('%d tiles of interest processed' % n_toi)


def tilequeue_consume_tile_traffic(cfg, peripherals):
    logger = make_logger(cfg, 'consume_tile_traffic')
    logger.info('Consuming tile traffic logs ...')

    tile_log_records = None
    with open(cfg.tile_traffic_log_path, 'r') as log_file:
        tile_log_records = parse_log_file(log_file)

    if not tile_log_records:
        logger.info("Couldn't parse log file")
        sys.exit(1)

    conn_info = dict(cfg.postgresql_conn_info)
    dbnames = conn_info.pop('dbnames')
    sql_conn_pool = DBAffinityConnectionsNoLimit(dbnames, conn_info, False)
    sql_conn = sql_conn_pool.get_conns(1)[0]
    with sql_conn.cursor() as cursor:

        # insert the log records after the latest_date
        cursor.execute('SELECT max(date) from tile_traffic_v4')
        max_timestamp = cursor.fetchone()[0]

        n_coords_inserted = 0
        for host, timestamp, coord_int in tile_log_records:
            if not max_timestamp or timestamp > max_timestamp:
                coord = coord_unmarshall_int(coord_int)
                cursor.execute(
                    "INSERT into tile_traffic_v4 "
                    "(date, z, x, y, tilesize, service, host) VALUES "
                    "('%s', %d, %d, %d, %d, '%s', '%s')"
                    % (timestamp, coord.zoom, coord.column, coord.row, 512,
                       'vector-tiles', host))
                n_coords_inserted += 1

        logger.info('Inserted %d records' % n_coords_inserted)

    sql_conn_pool.put_conns([sql_conn])


def emit_toi_stats(toi_set, peripherals):
    """
    Calculates new TOI stats and emits them via statsd.
    """

    count_by_zoom = defaultdict(int)
    total = 0
    for coord_int in toi_set:
        coord = coord_unmarshall_int(coord_int)
        count_by_zoom[coord.zoom] += 1
        total += 1

    peripherals.stats.gauge('tiles-of-interest.count', total)
    for zoom, count in count_by_zoom.items():
        peripherals.stats.gauge(
            'tiles-of-interest.by-zoom.z{:02d}'.format(zoom),
            count
        )


def tilequeue_prune_tiles_of_interest(cfg, peripherals):
    logger = make_logger(cfg, 'prune_tiles_of_interest')
    logger.info('Pruning tiles of interest ...')

    time_overall = peripherals.stats.timer('gardener.overall')
    time_overall.start()

    logger.info('Fetching tiles recently requested ...')
    import psycopg2

    prune_cfg = cfg.yml.get('toi-prune', {})

    tile_history_cfg = prune_cfg.get('tile-history', {})
    db_conn_info = tile_history_cfg.get('database-uri')
    assert db_conn_info, ("A postgres-compatible connection URI must "
                          "be present in the config yaml")

    redshift_days_to_query = tile_history_cfg.get('days')
    assert redshift_days_to_query, ("Number of days to query "
                                    "redshift is not specified")

    redshift_zoom_cutoff = int(tile_history_cfg.get('max-zoom', '16'))

    # flag indicating that s3 entry in toi-prune is used for s3 store
    legacy_fallback = 's3' in prune_cfg
    store_parts = prune_cfg.get('s3') or prune_cfg.get('store')
    assert store_parts, (
        'The configuration of a store containing tiles to delete must be '
        'specified under toi-prune:store or toi-prune:s3')
    # explictly override the store configuration with values provided
    # in toi-prune:s3
    if legacy_fallback:
        cfg.store_type = 's3'
        cfg.s3_bucket = store_parts['bucket']
        cfg.s3_date_prefix = store_parts['date-prefix']
        cfg.s3_path = store_parts['path']

    redshift_results = defaultdict(int)
    with psycopg2.connect(db_conn_info) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                select x, y, z, tilesize, count(*)
                from tile_traffic_v4
                where (date >= (current_timestamp - interval '{days} days'))
                  and (z between 0 and {max_zoom})
                  and (x between 0 and pow(2,z)-1)
                  and (y between 0 and pow(2,z)-1)
                  and (service = 'vector-tiles')
                group by z, x, y, tilesize
                order by z, x, y, tilesize
                """.format(
                    days=redshift_days_to_query,
                    max_zoom=redshift_zoom_cutoff
            ))
            for (x, y, z, tile_size, count) in cur:
                coord = create_coord(x, y, z)
                coord_int = coord_marshall_int(coord)

                if not tile_size or tile_size == '256':
                    # "uplift" a tile that is not explicitly a '512' size tile
                    coord_int = coord_int_zoom_up(coord_int)

                # Sum the counts from the 256 and 512 tile requests into the
                # slot for the 512 tile.
                redshift_results[coord_int] += count

    logger.info('Fetching tiles recently requested ... done. %s found',
                len(redshift_results))

    cutoff_cfg = prune_cfg.get('cutoff', {})
    cutoff_requests = cutoff_cfg.get('min-requests', 0)
    cutoff_tiles = cutoff_cfg.get('max-tiles', 0)

    logger.info('Finding %s tiles requested %s+ times ...',
                cutoff_tiles,
                cutoff_requests,
                )

    new_toi = set()
    for coord_int, count in sorted(
            redshift_results.iteritems(),
            key=operator.itemgetter(1),
            reverse=True)[:cutoff_tiles]:
        if count >= cutoff_requests:
            new_toi.add(coord_int)

    redshift_results = None

    logger.info('Finding %s tiles requested %s+ times ... done. Found %s',
                cutoff_tiles,
                cutoff_requests,
                len(new_toi),
                )

    for name, info in prune_cfg.get('always-include', {}).items():
        logger.info('Adding in tiles from %s ...', name)

        immortal_tiles = set()
        if 'bbox' in info:
            bounds = map(float, info['bbox'].split(','))
            for coord in tile_generator_for_single_bounds(
                            bounds, info['min_zoom'], info['max_zoom']):
                coord_int = coord_marshall_int(coord)
                immortal_tiles.add(coord_int)
        elif 'tiles' in info:
            tiles = map(deserialize_coord, info['tiles'])
            tiles = map(coord_marshall_int, tiles)
            immortal_tiles.update(tiles)
        elif 'file' in info:
            with open(info['file'], 'r') as f:
                immortal_tiles.update(
                    coord_marshall_int(deserialize_coord(l.strip()))
                    for l in f
                )
        elif 'bucket' in info:
            from boto import connect_s3
            from boto.s3.bucket import Bucket
            s3_conn = connect_s3()
            bucket = Bucket(s3_conn, info['bucket'])
            key = bucket.get_key(info['key'])
            raw_coord_data = key.get_contents_as_string()
            for line in raw_coord_data.splitlines():
                coord = deserialize_coord(line.strip())
                if coord:
                    # NOTE: the tiles in the file should be of the
                    # same size as the toi
                    coord_int = coord_marshall_int(coord)
                    immortal_tiles.add(coord_int)

        # Filter out nulls that might sneak in for various reasons
        immortal_tiles = filter(None, immortal_tiles)

        n_inc = len(immortal_tiles)
        new_toi = new_toi.union(immortal_tiles)

        # ensure that the new coordinates have valid zooms
        new_toi_valid_range = set()
        for coord_int in new_toi:
            coord = coord_unmarshall_int(coord_int)
            if coord_is_valid(coord, cfg.max_zoom):
                new_toi_valid_range.add(coord_int)
        new_toi = new_toi_valid_range

        logger.info('Adding in tiles from %s ... done. %s found', name, n_inc)

    logger.info('New tiles of interest set includes %s tiles', len(new_toi))

    logger.info('Fetching existing tiles of interest ...')
    tiles_of_interest = peripherals.toi.fetch_tiles_of_interest()
    n_toi = len(tiles_of_interest)
    logger.info('Fetching existing tiles of interest ... done. %s found',
                n_toi)

    logger.info('Computing tiles to remove ...')
    toi_to_remove = tiles_of_interest - new_toi
    logger.info('Computing tiles to remove ... done. %s found',
                len(toi_to_remove))
    peripherals.stats.gauge('gardener.removed', len(toi_to_remove))

    store = make_store(cfg.store_type, cfg.s3_bucket, cfg)
    if not toi_to_remove:
        logger.info('Skipping TOI remove step because there are '
                    'no tiles to remove')
    else:
        logger.info('Removing %s tiles from TOI and S3 ...',
                    len(toi_to_remove))

        for coord_ints in grouper(toi_to_remove, 1000):
            removed = store.delete_tiles(
                map(coord_unmarshall_int, coord_ints),
                lookup_format_by_extension(
                    store_parts['format']), store_parts['layer'])
            logger.info('Removed %s tiles from S3', removed)

        logger.info('Removing %s tiles from TOI and S3 ... done',
                    len(toi_to_remove))

    logger.info('Computing tiles to add ...')
    toi_to_add = new_toi - tiles_of_interest
    logger.info('Computing tiles to add ... done. %s found',
                len(toi_to_add))
    peripherals.stats.gauge('gardener.added', len(toi_to_add))

    if not toi_to_add:
        logger.info('Skipping TOI add step because there are '
                    'no tiles to add')
    else:
        logger.info('Enqueueing %s tiles to SQS ...',
                    len(toi_to_add))

        sqs_queue = peripherals.queue
        enqueuer = ThreadedEnqueuer(sqs_queue, cfg.seed_n_threads, logger)
        n_queued, n_in_flight = enqueuer(
            coord_unmarshall_int(coord_int) for coord_int in toi_to_add
        )

        logger.info('Enqueueing %s tiles to SQS ... done',
                    len(toi_to_add))

    if toi_to_add or toi_to_remove:
        logger.info('Setting new tiles of interest ... ')

        peripherals.toi.set_tiles_of_interest(new_toi)
        emit_toi_stats(new_toi, peripherals)

        logger.info('Setting new tiles of interest ... done')
    else:
        logger.info('Tiles of interest did not change, '
                    'so not setting new tiles of interest')

    logger.info('Pruning tiles of interest ... done')
    time_overall.stop()


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
        fetcher, model, peripherals.toi, peripherals.queue,
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


def tilequeue_dump_tiles_of_interest(cfg, peripherals):
    logger = make_logger(cfg, 'dump_tiles_of_interest')
    logger.info('Dumping tiles of interest')

    logger.info('Fetching tiles of interest ...')
    toi_set = peripherals.toi.fetch_tiles_of_interest()
    n_toi = len(toi_set)
    logger.info('Fetching tiles of interest ... done')

    toi_filename = "toi.txt"

    logger.info('Writing %d tiles of interest to %s ...', n_toi, toi_filename)

    with open(toi_filename, "w") as f:
        save_set_to_fp(toi_set, f)

    logger.info(
        'Writing %d tiles of interest to %s ... done',
        n_toi,
        toi_filename
    )


def tilequeue_load_tiles_of_interest(cfg, peripherals):
    """
    Given a newline-delimited file containing tile coordinates in
    `zoom/column/row` format, load those tiles into the tiles of interest.
    """
    logger = make_logger(cfg, 'load_tiles_of_interest')

    toi_filename = "toi.txt"
    logger.info('Loading tiles of interest from %s ... ', toi_filename)

    with open(toi_filename, 'r') as f:
        new_toi = load_set_from_fp(f)

    logger.info('Loading tiles of interest from %s ... done', toi_filename)
    logger.info('Setting new TOI (with %s tiles) ... ', len(new_toi))

    peripherals.toi.set_tiles_of_interest(new_toi)
    emit_toi_stats(new_toi, peripherals)

    logger.info('Setting new TOI (with %s tiles) ... done', len(new_toi))

    logger.info('Loading tiles of interest ... done')


class TileArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        sys.stderr.write('error: %s\n' % message)
        self.print_help()
        sys.exit(2)


class FakeStatsd(object):
    def __init__(self, *args, **kwargs):
        pass

    def incr(self, *args, **kwargs):
        pass

    def decr(self, *args, **kwargs):
        pass

    def gauge(self, *args, **kwargs):
        pass

    def set(self, *args, **kwargs):
        pass

    def timing(self, *args, **kwargs):
        pass

    def timer(self, *args, **kwargs):
        return FakeStatsTimer()


class FakeStatsTimer(object):
    def __init__(self, *args, **kwargs):
        pass

    def start(self):
        pass

    def stop(self):
        pass


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
        ('dump-tiles-of-interest',
            create_command_parser(tilequeue_dump_tiles_of_interest)),
        ('load-tiles-of-interest',
            create_command_parser(tilequeue_load_tiles_of_interest)),
        ('enqueue-tiles-of-interest',
         create_command_parser(tilequeue_enqueue_tiles_of_interest)),
        ('prune-tiles-of-interest',
         create_command_parser(tilequeue_prune_tiles_of_interest)),
        ('tile-size', create_command_parser(tilequeue_tile_sizes)),
        ('wof-process-neighbourhoods', create_command_parser(
            tilequeue_process_wof_neighbourhoods)),
        ('wof-load-initial-neighbourhoods', create_command_parser(
            tilequeue_initial_load_wof_neighbourhoods)),
        ('consume-tile-traffic', create_command_parser(
            tilequeue_consume_tile_traffic))
    )
    for parser_name, parser_func in parser_config:
        subparser = subparsers.add_parser(parser_name)
        parser_func(subparser)

    args = parser.parse_args(argv_args)
    assert os.path.exists(args.config), \
        'Config file {} does not exist!'.format(args.config)
    with open(args.config) as fh:
        cfg = make_config_from_argparse(fh)
    redis_client = make_redis_client(cfg)
    Peripherals = namedtuple('Peripherals', 'toi queue stats')

    toi_helper = make_toi_helper(cfg)

    queue = make_queue(
        cfg.queue_type, cfg.queue_name, cfg.queue_cfg, redis_client,
        aws_access_key_id=cfg.aws_access_key_id,
        aws_secret_access_key=cfg.aws_secret_access_key)

    if cfg.statsd_host:
        import statsd
        stats = statsd.StatsClient(cfg.statsd_host, cfg.statsd_port,
                                   prefix=cfg.statsd_prefix)
    else:
        stats = FakeStatsd()

    peripherals = Peripherals(toi_helper, queue, stats)
    args.func(cfg, peripherals)
