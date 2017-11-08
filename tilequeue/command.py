from collections import defaultdict
from collections import namedtuple
from contextlib import closing
from itertools import chain
from multiprocessing.pool import ThreadPool
from tilequeue.config import create_query_bounds_pad_fn
from tilequeue.config import make_config_from_argparse
from tilequeue.format import lookup_format_by_extension
from tilequeue.metro_extract import city_bounds
from tilequeue.metro_extract import parse_metro_extract
from tilequeue.process import convert_source_data_to_feature_layers
from tilequeue.process import process_coord
from tilequeue.query import DBConnectionPool
from tilequeue.query import make_data_fetcher
from tilequeue.queue import make_sqs_queue
from tilequeue.store import make_store
from tilequeue.tile import coord_children_range
from tilequeue.tile import coord_int_zoom_up
from tilequeue.tile import coord_is_valid
from tilequeue.tile import coord_marshall_int
from tilequeue.tile import coord_to_mercator_bounds
from tilequeue.tile import coord_unmarshall_int
from tilequeue.tile import create_coord
from tilequeue.tile import deserialize_coord
from tilequeue.tile import parse_expired_coord_string
from tilequeue.tile import seed_tiles
from tilequeue.tile import serialize_coord
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
from tilequeue.worker import TileQueueReader
from tilequeue.worker import TileQueueWriter
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


def make_queue_mapper(queue_mapper_yaml, tile_queue_name_map):
    queue_mapper_type = queue_mapper_yaml.get('type')
    assert queue_mapper_type, 'Missing queue mapper type'
    if queue_mapper_type == 'single':
        queue_name = queue_mapper_yaml.get('queue-name')
        assert queue_name, 'Missing queue name in queue mapper config'
        tile_queue = tile_queue_name_map.get(queue_name)
        assert tile_queue, 'No queue found in mapping for %s' % queue_name
        return make_single_queue_mapper(queue_name, tile_queue)
    elif queue_mapper_type == 'multiple':
        multi_queue_map_yaml = queue_mapper_yaml.get('multiple')
        assert multi_queue_map_yaml, \
            'Missing yaml config for multiple queue mapper'
        assert isinstance(multi_queue_map_yaml, list), \
            'Mulitple queue mapper config should be a list'
        return make_multi_queue_group_mapper_from_cfg(
            multi_queue_map_yaml, tile_queue_name_map)
    else:
        assert 0, 'Unknown queue mapper type: %s' % queue_mapper_type


def make_multi_queue_group_mapper_from_cfg(
        multi_queue_map_yaml, tile_queue_name_map):
    from tilequeue.queue.mapper import ZoomRangeAndZoomGroupQueueMapper
    from tilequeue.queue.mapper import ZoomRangeQueueSpec
    zoom_range_specs = []
    for zoom_range_spec_yaml in multi_queue_map_yaml:
        start_zoom = zoom_range_spec_yaml.get('start-zoom')
        end_zoom = zoom_range_spec_yaml.get('end-zoom')
        if start_zoom is not None and end_zoom is not None:
            assert isinstance(start_zoom, int)
            assert isinstance(end_zoom, int)
            assert start_zoom < end_zoom
        else:
            start_zoom = None
            end_zoom = None
        queue_name = zoom_range_spec_yaml['queue-name']
        queue = tile_queue_name_map[queue_name]
        group_by_zoom = zoom_range_spec_yaml.get('group-by-zoom')
        assert group_by_zoom is None or isinstance(group_by_zoom, int)
        zrs = ZoomRangeQueueSpec(
                start_zoom, end_zoom, queue_name, queue, group_by_zoom)
        zoom_range_specs.append(zrs)
    queue_mapper = ZoomRangeAndZoomGroupQueueMapper(zoom_range_specs)
    return queue_mapper


def make_single_queue_mapper(queue_name, tile_queue):
    from tilequeue.queue.mapper import SingleQueueMapper
    queue_mapper = SingleQueueMapper(queue_name, tile_queue)
    return queue_mapper


def make_message_marshaller(msg_marshall_yaml_cfg):
    msg_mar_type = msg_marshall_yaml_cfg.get('type')
    assert msg_mar_type, 'Missing message marshall type in config'
    if msg_mar_type == 'single':
        from tilequeue.queue.message import SingleMessageMarshaller
        return SingleMessageMarshaller()
    elif msg_mar_type == 'multiple':
        from tilequeue.queue.message import CommaSeparatedMarshaller
        return CommaSeparatedMarshaller()
    else:
        assert 0, 'Unknown message marshall type: %s' % msg_mar_type


def make_inflight_manager(inflight_yaml, redis_client=None):
    if not inflight_yaml:
        from tilequeue.queue.inflight import NoopInFlightManager
        return NoopInFlightManager()
    inflight_type = inflight_yaml.get('type')
    assert inflight_type, 'Missing inflight type config'
    if inflight_type == 'redis':
        assert redis_client, 'redis client required for redis inflight manager'
        inflight_key = 'tilequeue.in-flight'
        inflight_redis_cfg = inflight_yaml.get('redis')
        if inflight_redis_cfg:
            inflight_key = inflight_redis_cfg.get('key') or inflight_key
        from tilequeue.queue.inflight import RedisInFlightManager
        return RedisInFlightManager(redis_client, inflight_key)
    else:
        assert 0, 'Unknown inflight type: %s' % inflight_type


def make_tile_queue(queue_yaml_cfg, all_cfg, redis_client=None):
    # return a tile_queue, name instance, or list of tilequeue, name pairs
    # alternatively maybe should force queue implementations to know
    # about their names?
    if isinstance(queue_yaml_cfg, list):
        result = []
        for queue_item_cfg in queue_yaml_cfg:
            tile_queue, name = make_tile_queue(
                    queue_item_cfg, all_cfg, redis_client)
            result.append((tile_queue, name))
        return result
    else:
        queue_name = queue_yaml_cfg.get('name')
        assert queue_name, 'Missing queue name'
        queue_type = queue_yaml_cfg.get('type')
        assert queue_type, 'Missing queue type'
        if queue_type == 'sqs':
            aws_access_key_id = None
            aws_secret_access_key = None
            aws_cfg = all_cfg.get('aws')
            if aws_cfg:
                aws_access_key_id = aws_cfg.get('aws_access_key_id')
                aws_secret_access_key = aws_cfg.get('aws_secret_access_key')
            tile_queue = make_sqs_queue(
                queue_name, aws_access_key_id, aws_secret_access_key)
        elif queue_type == 'mem':
            from tilequeue.queue import MemoryQueue
            tile_queue = MemoryQueue()
        elif queue_type == 'file':
            from tilequeue.queue import OutputFileQueue
            if os.path.exists(queue_name):
                assert os.path.isfile(queue_name), \
                    ('Could not create file queue. `./{}` is not a '
                     'file!'.format(queue_name))
            fp = open(queue_name, 'a+')
            tile_queue = OutputFileQueue(fp)
        elif queue_type == 'stdout':
            # only support writing
            from tilequeue.queue import OutputFileQueue
            tile_queue = OutputFileQueue(sys.stdout)
        elif queue_type == 'redis':
            assert redis_client, 'redis_client required for redis tile_queue'
            from tilequeue.queue import make_redis_queue
            tile_queue = make_redis_queue(redis_client, queue_name)
        else:
            raise ValueError('Unknown queue type: %s' % queue_type)
        return tile_queue, queue_name


def make_msg_tracker(msg_tracker_yaml):
    if not msg_tracker_yaml:
        from tilequeue.queue.message import SingleMessagePerCoordTracker
        return SingleMessagePerCoordTracker()
    else:
        msg_tracker_type = msg_tracker_yaml.get('type')
        assert msg_tracker_type, 'Missing message tracker type'
        if msg_tracker_type == 'single':
            from tilequeue.queue.message import SingleMessagePerCoordTracker
            return SingleMessagePerCoordTracker()
        elif msg_tracker_type == 'multiple':
            from tilequeue.queue.message import MultipleMessagesPerCoordTracker
            return MultipleMessagesPerCoordTracker()
        else:
            assert 0, 'Unknown message tracker type: %s' % msg_tracker_type


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


def make_logger(cfg, logger_name, loglevel=logging.INFO):
    if getattr(cfg, 'logconfig') is not None:
        logging.config.fileConfig(cfg.logconfig)
    logger = logging.getLogger(logger_name)
    logger.setLevel(loglevel)
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
        n_toi=len(tiles_of_interest),
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

        except Exception:
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


SourcesConfig = namedtuple('SourcesConfig', 'sources queries_generator')


def parse_layer_data(query_cfg, buffer_cfg, cfg_path):
    all_layer_names = query_cfg['all']
    layers_config = query_cfg['layers']
    post_process_config = query_cfg.get('post_process', [])
    layer_data = []
    all_layer_data = []
    post_process_data = []

    for layer_name, layer_config in layers_config.items():
        area_threshold = int(layer_config.get('area-inclusion-threshold', 1))
        layer_datum = dict(
            name=layer_name,
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


def make_output_calc_mapping(process_yaml_cfg):
    output_calc_mapping = {}
    if process_yaml_cfg['type'] == 'parse':
        parse_cfg = process_yaml_cfg['parse']
        yaml_path = parse_cfg['path']
        assert os.path.isdir(yaml_path), 'Invalid yaml path: %s' % yaml_path
        from vectordatasource.meta.python import make_function_name_props
        from vectordatasource.meta.python import output_kind
        from vectordatasource.meta.python import parse_layers
        layer_parse_result = parse_layers(
            yaml_path, output_kind, make_function_name_props)
        for layer_datum in layer_parse_result.layer_data:
            output_calc_mapping[layer_datum.layer] = layer_datum.fn
    elif process_yaml_cfg['type'] == 'callable':
        callable_cfg = process_yaml_cfg['callable']
        dotted_name = callable_cfg['dotted-name']
        fn = resolve(dotted_name)
        output_calc_mapping = fn(*callable_cfg['args'])
    else:
        raise ValueError('Invalid process yaml config: %s' % process_yaml_cfg)

    return output_calc_mapping


def make_min_zoom_calc_mapping(process_yaml_cfg):
    # can't handle "callable" type - how do we get the min zoom fn?
    assert process_yaml_cfg['type'] == 'parse'

    min_zoom_calc_mapping = {}

    parse_cfg = process_yaml_cfg['parse']
    yaml_path = parse_cfg['path']
    assert os.path.isdir(yaml_path), 'Invalid yaml path: %s' % yaml_path
    from vectordatasource.meta.python import make_function_name_min_zoom
    from vectordatasource.meta.python import output_min_zoom
    from vectordatasource.meta.python import parse_layers
    layer_parse_result = parse_layers(
        yaml_path, output_min_zoom, make_function_name_min_zoom)
    for layer_datum in layer_parse_result.layer_data:
        min_zoom_calc_mapping[layer_datum.layer] = layer_datum.fn

    return min_zoom_calc_mapping


def tilequeue_process(cfg, peripherals):
    from tilequeue.log import JsonTileProcessingLogger
    logger = make_logger(cfg, 'process')
    tile_proc_logger = JsonTileProcessingLogger(logger)
    tile_proc_logger.lifecycle('tilequeue processing started')

    assert os.path.exists(cfg.query_cfg), \
        'Invalid query config path'

    with open(cfg.query_cfg) as query_cfg_fp:
        query_cfg = yaml.load(query_cfg_fp)

    all_layer_data, layer_data, post_process_data = (
        parse_layer_data(
            query_cfg, cfg.buffer_cfg, os.path.dirname(cfg.query_cfg)))

    formats = lookup_formats(cfg.output_formats)

    store = make_store(cfg.yml['store'],
                       credentials=cfg.subtree('aws credentials'))

    assert cfg.postgresql_conn_info, 'Missing postgresql connection info'

    from shapely import speedups
    if speedups.available:
        speedups.enable()
        tile_proc_logger.lifecycle('Shapely speedups enabled')
    else:
        tile_proc_logger.lifecycle(
            'Shapely speedups not enabled, they were not available')

    output_calc_mapping = make_output_calc_mapping(cfg.process_yaml_cfg)

    n_cpu = multiprocessing.cpu_count()
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
    feature_fetcher = make_data_fetcher(cfg, layer_data, query_cfg, io_pool)

    # create all queues used to manage pipeline

    # holds coordinate messages from tile queue reader
    # TODO can move this hardcoded value to configuration
    # having a little less than the value is beneficial
    # ie prefer to read on-demand from queue rather than hold messages
    # in waiting while others are processed, can become stale faster
    tile_input_queue = Queue.Queue(10)

    # holds raw sql results - no filtering or processing done on them
    sql_data_fetch_queue = multiprocessing.Queue(sql_queue_buffer_size)

    # holds data after it has been filtered and processed
    # this is where the cpu intensive part of the operation will happen
    # the results will be data that is formatted for each necessary format
    processor_queue = multiprocessing.Queue(proc_queue_buffer_size)

    # holds data after it has been sent to s3
    s3_store_queue = Queue.Queue(s3_queue_buffer_size)

    # create worker threads/processes
    thread_tile_queue_reader_stop = threading.Event()

    queue_mapper = peripherals.queue_mapper
    msg_marshaller = peripherals.msg_marshaller
    msg_tracker = peripherals.msg_tracker
    tile_queue_reader = TileQueueReader(
        queue_mapper, msg_marshaller, msg_tracker, tile_input_queue,
        tile_proc_logger, thread_tile_queue_reader_stop, cfg.max_zoom
    )

    data_fetch = DataFetch(
        feature_fetcher, tile_input_queue, sql_data_fetch_queue, io_pool,
        tile_proc_logger, cfg.metatile_zoom, cfg.max_zoom)

    data_processor = ProcessAndFormatData(
        post_process_data, formats, sql_data_fetch_queue, processor_queue,
        cfg.buffer_cfg, output_calc_mapping, layer_data, tile_proc_logger)

    s3_storage = S3Storage(processor_queue, s3_store_queue, io_pool, store,
                           tile_proc_logger, cfg.metatile_size)

    from tilequeue.stats import TileProcessingStatsHandler
    stats_handler = TileProcessingStatsHandler(peripherals.stats)
    thread_tile_writer_stop = threading.Event()
    tile_queue_writer = TileQueueWriter(
        queue_mapper, s3_store_queue, peripherals.inflight_mgr,
        peripherals.msg_tracker, tile_proc_logger, stats_handler,
        thread_tile_writer_stop)

    def create_and_start_thread(fn, *args):
        t = threading.Thread(target=fn, args=args)
        t.start()
        return t

    thread_tile_queue_reader = create_and_start_thread(tile_queue_reader)

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

    thread_tile_writer = create_and_start_thread(tile_queue_writer)

    if cfg.log_queue_sizes:
        assert(cfg.log_queue_sizes_interval_seconds > 0)
        queue_data = (
            (tile_input_queue, 'queue'),
            (sql_data_fetch_queue, 'sql'),
            (processor_queue, 'proc'),
            (s3_store_queue, 's3'),
        )
        queue_printer_thread_stop = threading.Event()
        queue_printer = QueuePrint(
            cfg.log_queue_sizes_interval_seconds, queue_data, tile_proc_logger,
            queue_printer_thread_stop)
        queue_printer_thread = create_and_start_thread(queue_printer)
    else:
        queue_printer_thread = None
        queue_printer_thread_stop = None

    def stop_all_workers(signum, stack):
        tile_proc_logger.lifecycle('tilequeue processing shutdown ...')

        tile_proc_logger.lifecycle(
            'requesting all workers (threads and processes) stop ...')

        # each worker guards its read loop with an event object
        # ask all these to stop first

        thread_tile_queue_reader_stop.set()
        for thread_data_fetch_stop in threads_data_fetch_stop:
            thread_data_fetch_stop.set()
        for data_processor_stop in data_processors_stop:
            data_processor_stop.set()
        for thread_s3_storage_stop in threads_s3_storage_stop:
            thread_s3_storage_stop.set()
        thread_tile_writer_stop.set()

        if queue_printer_thread_stop:
            queue_printer_thread_stop.set()

        tile_proc_logger.lifecycle(
            'requesting all workers (threads and processes) stop ... done')

        # Once workers receive a stop event, they will keep reading
        # from their queues until they receive a sentinel value. This
        # is mandatory so that no messages will remain on queues when
        # asked to join. Otherwise, we never terminate.

        tile_proc_logger.lifecycle('joining all workers ...')

        tile_proc_logger.lifecycle('joining tile queue reader ...')
        thread_tile_queue_reader.join()
        tile_proc_logger.lifecycle('joining tile queue reader ... done')
        tile_proc_logger.lifecycle(
            'enqueueing sentinels for data fetchers ...')
        for i in range(len(threads_data_fetch)):
            tile_input_queue.put(None)
        tile_proc_logger.lifecycle(
            'enqueueing sentinels for data fetchers ... done')
        tile_proc_logger.lifecycle('joining data fetchers ...')
        for thread_data_fetch in threads_data_fetch:
            thread_data_fetch.join()
        tile_proc_logger.lifecycle('joining data fetchers ... done')
        tile_proc_logger.lifecycle(
            'enqueueing sentinels for data processors ...')
        for i in range(len(data_processors)):
            sql_data_fetch_queue.put(None)
        tile_proc_logger.lifecycle(
            'enqueueing sentinels for data processors ... done')
        tile_proc_logger.lifecycle('joining data processors ...')
        for data_processor in data_processors:
            data_processor.join()
        tile_proc_logger.lifecycle('joining data processors ... done')
        tile_proc_logger.lifecycle('enqueueing sentinels for s3 storage ...')
        for i in range(len(threads_s3_storage)):
            processor_queue.put(None)
        tile_proc_logger.lifecycle(
            'enqueueing sentinels for s3 storage ... done')
        tile_proc_logger.lifecycle('joining s3 storage ...')
        for thread_s3_storage in threads_s3_storage:
            thread_s3_storage.join()
        tile_proc_logger.lifecycle('joining s3 storage ... done')
        tile_proc_logger.lifecycle(
            'enqueueing sentinel for tile queue writer ...')
        s3_store_queue.put(None)
        tile_proc_logger.lifecycle(
            'enqueueing sentinel for tile queue writer ... done')
        tile_proc_logger.lifecycle('joining tile queue writer ...')
        thread_tile_writer.join()
        tile_proc_logger.lifecycle('joining tile queue writer ... done')
        if queue_printer_thread:
            tile_proc_logger.lifecycle('joining queue printer ...')
            queue_printer_thread.join()
            tile_proc_logger.lifecycle('joining queue printer ... done')

        tile_proc_logger.lifecycle('joining all workers ... done')

        tile_proc_logger.lifecycle('joining io pool ...')
        io_pool.close()
        io_pool.join()
        tile_proc_logger.lifecycle('joining io pool ... done')

        tile_proc_logger.lifecycle('joining multiprocess data fetch queue ...')
        sql_data_fetch_queue.close()
        sql_data_fetch_queue.join_thread()
        tile_proc_logger.lifecycle(
            'joining multiprocess data fetch queue ... done')

        tile_proc_logger.lifecycle('joining multiprocess process queue ...')
        processor_queue.close()
        processor_queue.join_thread()
        tile_proc_logger.lifecycle(
            'joining multiprocess process queue ... done')

        tile_proc_logger.lifecycle('tilequeue processing shutdown ... done')
        sys.exit(0)

    signal.signal(signal.SIGTERM, stop_all_workers)
    signal.signal(signal.SIGINT, stop_all_workers)
    signal.signal(signal.SIGQUIT, stop_all_workers)

    tile_proc_logger.lifecycle('all tilequeue threads and processes started')

    # this is necessary for the main thread to receive signals
    # when joining on threads/processes, the signal is never received
    # http://www.luke.maurits.id.au/blog/post/threads-and-signals-in-python.html
    while True:
        time.sleep(1024)


def coords_generator_from_queue(queue):
    """given a python queue, read from it and yield coordinates"""
    while True:
        coord = queue.get()
        if coord is None:
            break
        yield coord


def tilequeue_seed(cfg, peripherals):
    logger = make_logger(cfg, 'seed')
    logger.info('Seeding tiles ...')
    queue_writer = peripherals.queue_writer

    # based on cfg, create tile generator
    tile_generator = make_seed_tile_generator(cfg)

    queue_buf_size = 1024
    tile_queue_queue = Queue.Queue(queue_buf_size)

    # updating tile queue happens in background threads
    def tile_queue_enqueue():
        coords = coords_generator_from_queue(tile_queue_queue)
        queue_writer.enqueue_batch(coords)

    logger.info('Enqueueing ... ')
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
    logger.info('Enqueueing ... done')

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

    logger.info('Fetching tiles of interest ...')
    tiles_of_interest = peripherals.toi.fetch_tiles_of_interest()
    n_toi = len(tiles_of_interest)
    logger.info('Fetching tiles of interest ... done')

    coords = []
    for coord_int in tiles_of_interest:
        coord = coord_unmarshall_int(coord_int)
        if coord.zoom <= cfg.max_zoom:
            coords.append(coord)

    queue_writer = peripherals.queue_writer
    n_queued, n_in_flight = queue_writer.enqueue_batch(coords)

    logger.info('%d enqueued - %d in flight' % (n_queued, n_in_flight))
    logger.info('%d tiles of interest processed' % n_toi)


def coord_pyramids(coords, zoom_start, zoom_stop):
    """
    generate full pyramid for coords

    Generate the full pyrmaid for the list of coords. Note that zoom_stop is
    exclusive.
    """
    for coord in coords:
        if zoom_start <= coord.zoom:
            yield coord
        for child_coord in coord_children_range(coord, zoom_stop):
            if zoom_start <= child_coord.zoom:
                yield child_coord


def tilequeue_enqueue_full_pyramid_from_toi(cfg, peripherals, args):
    """enqueue a full pyramid from the z10 toi"""
    logger = make_logger(cfg, 'enqueue_tiles_of_interest')
    logger.info('Enqueueing tiles of interest')

    logger.info('Fetching tiles of interest ...')
    tiles_of_interest = peripherals.toi.fetch_tiles_of_interest()
    n_toi = len(tiles_of_interest)
    logger.info('Fetching tiles of interest ... done')

    rawr_yaml = cfg.yml.get('rawr')
    assert rawr_yaml, 'Missing rawr yaml'
    group_by_zoom = rawr_yaml.get('group-zoom')
    assert group_by_zoom, 'Missing rawr group-zoom'
    assert isinstance(group_by_zoom, int), 'Invalid rawr group-zoom'

    if args.zoom_start is None:
        zoom_start = group_by_zoom
    else:
        zoom_start = args.zoom_start

    if args.zoom_stop is None:
        zoom_stop = cfg.max_zoom + 1  # +1 because exclusive
    else:
        zoom_stop = args.zoom_stop

    assert zoom_start >= group_by_zoom
    assert zoom_stop > zoom_start

    ungrouped = []
    coords_at_group_zoom = set()
    for coord_int in tiles_of_interest:
        coord = coord_unmarshall_int(coord_int)
        if coord.zoom < zoom_start:
            ungrouped.append(coord)
        if coord.zoom >= group_by_zoom:
            coord_at_group_zoom = coord.zoomTo(group_by_zoom).container()
            coords_at_group_zoom.add(coord_at_group_zoom)

    pyramid = coord_pyramids(coords_at_group_zoom, zoom_start, zoom_stop)

    coords_to_enqueue = chain(ungrouped, pyramid)

    queue_writer = peripherals.queue_writer
    n_queued, n_in_flight = queue_writer.enqueue_batch(coords_to_enqueue)

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
    sql_conn_pool = DBConnectionPool(dbnames, conn_info, False)
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
        logger.info('Enqueueing %s tiles ...', len(toi_to_add))

        queue_writer = peripherals.queue_writer
        n_queued, n_in_flight = queue_writer.enqueue_batch(
            coord_unmarshall_int(coord_int) for coord_int in toi_to_add
        )

        logger.info('Enqueueing %s tiles ... done', len(toi_to_add))

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


def tilequeue_process_wof_neighbourhoods(cfg, peripherals):
    from tilequeue.stats import RawrTileEnqueueStatsHandler
    from tilequeue.wof import make_wof_model
    from tilequeue.wof import make_wof_url_neighbourhood_fetcher
    from tilequeue.wof import make_wof_processor
    from tilequeue.rawr import make_rawr_enqueuer_from_cfg

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

    current_date = datetime.date.today()
    stats = make_statsd_client_from_cfg(cfg)
    stats_handler = RawrTileEnqueueStatsHandler(stats)
    rawr_enqueuer = make_rawr_enqueuer_from_cfg(
        cfg, logger, stats_handler, peripherals.msg_marshaller)
    processor = make_wof_processor(
        fetcher, model, peripherals.toi, rawr_enqueuer, logger, current_date)

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


def tilequeue_stuck_tiles(cfg, peripherals):
    """
    Check which files exist on s3 but are not in toi.
    """
    store = make_store(cfg.store_type, cfg.s3_bucket, cfg)
    format = lookup_format_by_extension('zip')
    layer = 'all'

    assert peripherals.toi, 'Missing toi'
    toi = peripherals.toi.fetch_tiles_of_interest()

    for coord in store.list_tiles(format, layer):
        coord_int = coord_marshall_int(coord)
        if coord_int not in toi:
            print serialize_coord(coord)


def tilequeue_delete_stuck_tiles(cfg, peripherals):
    logger = make_logger(cfg, 'delete_stuck_tiles')

    format = lookup_format_by_extension('zip')
    layer = 'all'

    store = make_store(cfg.store_type, cfg.s3_bucket, cfg)

    logger.info('Removing tiles from S3 ...')
    total_removed = 0
    for coord_strs in grouper(sys.stdin, 1000):
        coords = []
        for coord_str in coord_strs:
            coord = deserialize_coord(coord_str)
            if coord:
                coords.append(coord)
        if coords:
            n_removed = store.delete_tiles(coords, format, layer)
            total_removed += n_removed
            logger.info('Removed %s tiles from S3', n_removed)

    logger.info('Total removed: %d', total_removed)
    logger.info('Removing tiles from S3 ... DONE')


def tilequeue_tile_status(cfg, peripherals, args):
    """
    Report the status of the given tiles in the store, queue and TOI.
    """
    logger = make_logger(cfg, 'tile_status')

    # friendly warning to avoid confusion when this command outputs nothing
    # at all when called with no positional arguments.
    if not args.coords:
        logger.warning('No coordinates given on the command line.')
        return

    # pre-load TOI to avoid having to do it for each coordinate
    toi = None
    if peripherals.toi:
        toi = peripherals.toi.fetch_tiles_of_interest()

    # TODO: make these configurable!
    tile_format = lookup_format_by_extension('zip')
    tile_layer = 'all'
    store = make_store(cfg.store_type, cfg.s3_bucket, cfg)

    for coord_str in args.coords:
        coord = deserialize_coord(coord_str)

        # input checking! make sure that the coordinate is okay to use in
        # the rest of the code.
        if not coord:
            logger.warning('Could not deserialize %r as coordinate', coord_str)
            continue

        if not coord_is_valid(coord):
            logger.warning('Coordinate is not valid: %r (parsed from %r)',
                           coord, coord_str)
            continue

        # now we think we probably have a valid coordinate. go look up
        # whether it exists in various places.

        logger.info("=== %s ===", coord_str)
        coord_int = coord_marshall_int(coord)

        if peripherals.inflight_mgr:
            is_inflight = peripherals.inflight_mgr.is_inflight(coord)
            logger.info('inflight: %r', is_inflight)

        if toi:
            in_toi = coord_int in toi
            logger.info('in TOI: %r' % (in_toi,))

        data = store.read_tile(coord, tile_format, tile_layer)
        logger.info('tile in store: %r', bool(data))


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

    def pipeline(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        pass


class FakeStatsTimer(object):
    def __init__(self, *args, **kwargs):
        pass

    def start(self):
        pass

    def stop(self):
        pass


def tilequeue_process_tile(cfg, peripherals, args):
    if not args.coord:
        print >> sys.stderr, 'Missing coord argument'
        sys.exit(1)

    coord_str = args.coord
    coord = deserialize_coord(coord_str)
    if not coord:
        print >> sys.stderr, 'Invalid coordinate: %s' % coord_str
        sys.exit(2)

    with open(cfg.query_cfg) as query_cfg_fp:
        query_cfg = yaml.load(query_cfg_fp)

    all_layer_data, layer_data, post_process_data = (
        parse_layer_data(
            query_cfg, cfg.buffer_cfg, os.path.dirname(cfg.query_cfg)))

    output_calc_mapping = make_output_calc_mapping(cfg.process_yaml_cfg)
    io_pool = ThreadPool(len(layer_data))

    data_fetcher = make_data_fetcher(cfg, layer_data, query_cfg, io_pool)

    unpadded_bounds = coord_to_mercator_bounds(coord)
    for fetch, _ in data_fetcher.fetch_tiles([dict(coord=coord)]):
        source_rows = fetch(coord.zoom, unpadded_bounds)
    feature_layers = convert_source_data_to_feature_layers(
        source_rows, layer_data, unpadded_bounds, coord.zoom)
    cut_coords = []
    formats = lookup_formats(cfg.output_formats)
    formatted_tiles, extra_data = process_coord(
        coord, coord.zoom, feature_layers, post_process_data, formats,
        unpadded_bounds, cut_coords, cfg.buffer_cfg, output_calc_mapping)

    # can think about making this configurable
    # but this is intended for debugging anyway
    json_tile = [x for x in formatted_tiles
                 if x['format'].extension == 'json']
    assert json_tile
    json_tile = json_tile[0]
    tile_data = json_tile['tile']
    print tile_data


def tilequeue_rawr_enqueue(cfg, args):
    """command to take tile expiry path and enqueue for rawr tile generation"""
    from tilequeue.stats import RawrTileEnqueueStatsHandler
    from tilequeue.rawr import make_rawr_enqueuer_from_cfg

    msg_marshall_yaml = cfg.yml.get('message-marshall')
    assert msg_marshall_yaml, 'Missing message-marshall config'
    msg_marshaller = make_message_marshaller(msg_marshall_yaml)

    logger = make_logger(cfg, 'rawr_enqueue')
    stats = make_statsd_client_from_cfg(cfg)
    stats_handler = RawrTileEnqueueStatsHandler(stats)
    rawr_enqueuer = make_rawr_enqueuer_from_cfg(
        cfg, logger, stats_handler, msg_marshaller)

    with open(args.expiry_path) as fh:
        coords = create_coords_generator_from_tiles_file(fh)
        rawr_enqueuer(coords)


def tilequeue_rawr_process(cfg, peripherals):
    """command to read from rawr queue and generate rawr tiles"""
    from tilequeue.rawr import make_rawr_queue_from_yaml

    rawr_yaml = cfg.yml.get('rawr')
    assert rawr_yaml is not None, 'Missing rawr configuration in yaml'

    group_by_zoom = rawr_yaml.get('group-zoom')
    assert group_by_zoom is not None, 'Missing group-zoom rawr config'

    msg_marshall_yaml = cfg.yml.get('message-marshall')
    assert msg_marshall_yaml, 'Missing message-marshall config'
    msg_marshaller = make_message_marshaller(msg_marshall_yaml)

    rawr_queue_yaml = rawr_yaml.get('queue')
    assert rawr_queue_yaml, 'Missing rawr queue config'
    rawr_queue = make_rawr_queue_from_yaml(rawr_queue_yaml, msg_marshaller)

    rawr_postgresql_yaml = rawr_yaml.get('postgresql')
    assert rawr_postgresql_yaml, 'Missing rawr postgresql config'

    from raw_tiles.formatter.msgpack import Msgpack
    from raw_tiles.gen import RawrGenerator
    from raw_tiles.source.conn import ConnectionContextManager
    from raw_tiles.source.osm import OsmSource
    from tilequeue.log import JsonRawrProcessingLogger
    from tilequeue.rawr import RawrS3Sink
    from tilequeue.rawr import RawrStoreSink
    from tilequeue.rawr import RawrTileGenerationPipeline
    from tilequeue.rawr import RawrToiIntersector
    from tilequeue.rawr import EmptyToiIntersector
    from tilequeue.stats import RawrTilePipelineStatsHandler
    import boto3
    # pass through the postgresql yaml config directly
    conn_ctx = ConnectionContextManager(rawr_postgresql_yaml)

    rawr_store = rawr_yaml.get('store')
    if rawr_store:
        store = make_store(rawr_store,
                           credentials=cfg.subtree('aws credentials'))
        rawr_sink = RawrStoreSink(store)

    else:
        rawr_sink_yaml = rawr_yaml.get('sink')
        assert rawr_sink_yaml, 'Missing rawr sink config'
        bucket = rawr_sink_yaml.get('bucket')
        assert bucket, 'Missing rawr sink bucket'
        sink_region = rawr_sink_yaml.get('region')
        assert sink_region, 'Missing rawr sink region'
        prefix = rawr_sink_yaml.get('prefix')
        assert prefix, 'Missing rawr sink prefix'
        suffix = rawr_sink_yaml.get('suffix')
        assert suffix, 'Missing rawr sink suffix'

        s3_client = boto3.client('s3', region_name=sink_region)
        rawr_sink = RawrS3Sink(s3_client, bucket, prefix, suffix)

    toi_yaml = cfg.yml.get('toi-store')
    toi_type = toi_yaml.get('type')
    if toi_type == 's3':
        toi_s3_yaml = toi_yaml.get('s3')
        assert toi_s3_yaml, 'Missing toi-store s3 config'
        toi_bucket = toi_s3_yaml.get('bucket')
        toi_key = toi_s3_yaml.get('key')
        assert toi_bucket, 'Missing toi-store s3 bucket'
        assert toi_key, 'Missing toi-store s3 key'

        rawr_toi_intersector = RawrToiIntersector(
            s3_client, toi_bucket, toi_key)

    elif toi_type == 'none':
        rawr_toi_intersector = EmptyToiIntersector()

    else:
        assert False, 'TOI type %r is not known. Options are s3 or none.' \
            % (toi_type,)

    logger = make_logger(cfg, 'rawr_process')
    rawr_osm_source = OsmSource(conn_ctx)
    rawr_formatter = Msgpack()
    rawr_gen = RawrGenerator(rawr_osm_source, rawr_formatter, rawr_sink)
    stats_handler = RawrTilePipelineStatsHandler(peripherals.stats)
    rawr_proc_logger = JsonRawrProcessingLogger(logger)
    rawr_pipeline = RawrTileGenerationPipeline(
            rawr_queue, msg_marshaller, group_by_zoom, rawr_gen,
            peripherals.queue_writer, rawr_toi_intersector, stats_handler,
            rawr_proc_logger)
    rawr_pipeline()


def tilequeue_rawr_seed_toi(cfg, peripherals):
    """command to read the toi and enqueue the corresponding rawr tiles"""
    from tilequeue.stats import RawrTileEnqueueStatsHandler
    from tilequeue.rawr import make_rawr_enqueuer_from_cfg

    logger = make_logger(cfg, 'rawr_seed')
    stats_handler = RawrTileEnqueueStatsHandler(peripherals.stats)
    rawr_enqueuer = make_rawr_enqueuer_from_cfg(
        cfg, logger, stats_handler, peripherals.msg_marshaller)

    tiles_of_interest = peripherals.toi.fetch_tiles_of_interest()
    # high zoom level coordinates get enqueued onto rawr queue
    # low zoom get enqueued for tile processing directly
    lo_zoom = []
    hi_zoom = []
    for coord_int in tiles_of_interest:
        coord = coord_unmarshall_int(coord_int)
        if coord.zoom >= rawr_enqueuer.group_by_zoom:
            hi_zoom.append(coord)
        else:
            lo_zoom.append(coord)

    rawr_enqueuer(hi_zoom)
    peripherals.queue_writer.enqueue_batch(lo_zoom)
    logger.info('%d coords enqueued for rawr tile processing', len(hi_zoom))
    logger.info('%d coords enqueued for tilequeue processing', len(lo_zoom))


Peripherals = namedtuple(
    'Peripherals',
    'toi stats redis_client '
    'queue_mapper msg_tracker msg_marshaller inflight_mgr queue_writer'
)


def make_statsd_client_from_cfg(cfg):
    if cfg.statsd_host:
        import statsd
        stats = statsd.StatsClient(cfg.statsd_host, cfg.statsd_port,
                                   prefix=cfg.statsd_prefix)
    else:
        stats = FakeStatsd()
    return stats


def tilequeue_main(argv_args=None):
    if argv_args is None:
        argv_args = sys.argv[1:]

    parser = TileArgumentParser()
    subparsers = parser.add_subparsers()

    # these are all the "standard" parsers which just take a config argument
    # that is already included at the top level.
    cfg_commands = (
        ('process', tilequeue_process),
        ('seed', tilequeue_seed),
        ('dump-tiles-of-interest', tilequeue_dump_tiles_of_interest),
        ('load-tiles-of-interest', tilequeue_load_tiles_of_interest),
        ('enqueue-tiles-of-interest', tilequeue_enqueue_tiles_of_interest),
        ('prune-tiles-of-interest', tilequeue_prune_tiles_of_interest),
        ('wof-process-neighbourhoods', tilequeue_process_wof_neighbourhoods),
        ('wof-load-initial-neighbourhoods',
            tilequeue_initial_load_wof_neighbourhoods),
        ('consume-tile-traffic', tilequeue_consume_tile_traffic),
        ('stuck-tiles', tilequeue_stuck_tiles),
        ('delete-stuck-tiles', tilequeue_delete_stuck_tiles),
        ('rawr-process', tilequeue_rawr_process),
        ('rawr-seed-toi', tilequeue_rawr_seed_toi),
    )

    def _make_peripherals(cfg):
        redis_client = make_redis_client(cfg)

        toi_helper = make_toi_helper(cfg)

        tile_queue_result = make_tile_queue(
                cfg.queue_cfg, cfg.yml, redis_client)
        tile_queue_name_map = {}
        if isinstance(tile_queue_result, tuple):
            tile_queue, queue_name = tile_queue_result
            tile_queue_name_map[queue_name] = tile_queue
        else:
            assert isinstance(tile_queue_result, list), \
                'Unknown tile_queue result: %s' % tile_queue_result
            for tile_queue, queue_name in tile_queue_result:
                tile_queue_name_map[queue_name] = tile_queue

        queue_mapper_yaml = cfg.yml.get('queue-mapping')
        assert queue_mapper_yaml, 'Missing queue-mapping configuration'
        queue_mapper = make_queue_mapper(
                queue_mapper_yaml, tile_queue_name_map)

        msg_marshall_yaml = cfg.yml.get('message-marshall')
        assert msg_marshall_yaml, 'Missing message-marshall config'
        msg_marshaller = make_message_marshaller(msg_marshall_yaml)

        inflight_yaml = cfg.yml.get('in-flight')
        inflight_mgr = make_inflight_manager(inflight_yaml, redis_client)

        enqueue_batch_size = 10
        from tilequeue.queue.writer import QueueWriter
        queue_writer = QueueWriter(
            queue_mapper, msg_marshaller, inflight_mgr, enqueue_batch_size)

        msg_tracker_yaml = cfg.yml.get('message-tracker')
        msg_tracker = make_msg_tracker(msg_tracker_yaml)

        stats = make_statsd_client_from_cfg(cfg)

        peripherals = Peripherals(
            toi_helper, stats, redis_client, queue_mapper, msg_tracker,
            msg_marshaller, inflight_mgr, queue_writer
        )
        return peripherals

    def _make_peripherals_command(func):
        def command_fn(cfg, args):
            peripherals = _make_peripherals(cfg)
            return func(cfg, peripherals)
        return command_fn

    def _make_peripherals_with_args_command(func):
        def command_fn(cfg, args):
            peripherals = _make_peripherals(cfg)
            return func(cfg, peripherals, args)
        return command_fn

    for parser_name, func in cfg_commands:
        subparser = subparsers.add_parser(parser_name)

        # config parameter is shared amongst all parsers, but appears here so
        # that it can be given _after_ the name of the command.
        subparser.add_argument('--config', required=True,
                               help='The path to the tilequeue config file.')
        command_fn = _make_peripherals_command(func)
        subparser.set_defaults(func=command_fn)

    # add "special" commands which take arguments
    subparser = subparsers.add_parser('tile-status')
    subparser.add_argument('--config', required=True,
                           help='The path to the tilequeue config file.')
    subparser.add_argument('coords', nargs='*',
                           help='Tile coordinates as "z/x/y".')
    subparser.set_defaults(
            func=_make_peripherals_with_args_command(tilequeue_tile_status))

    subparser = subparsers.add_parser('tile')
    subparser.add_argument('--config', required=True,
                           help='The path to the tilequeue config file.')
    subparser.add_argument('coord',
                           help='Tile coordinate as "z/x/y".')
    subparser.set_defaults(
            func=_make_peripherals_with_args_command(tilequeue_process_tile))

    subparser = subparsers.add_parser('enqueue-tiles-of-interest-pyramids')
    subparser.add_argument('--config', required=True,
                           help='The path to the tilequeue config file.')
    subparser.add_argument('--zoom-start', type=int, required=False,
                           default=None, help='Zoom start')
    subparser.add_argument('--zoom-stop', type=int, required=False,
                           default=None, help='Zoom stop, exclusive')
    subparser.set_defaults(
            func=_make_peripherals_with_args_command(
                tilequeue_enqueue_full_pyramid_from_toi))

    subparser = subparsers.add_parser('rawr-enqueue')
    subparser.add_argument('--config', required=True,
                           help='The path to the tilequeue config file.')
    subparser.add_argument('--expiry-path', required=True,
                           help='path to tile expiry file')
    subparser.set_defaults(func=tilequeue_rawr_enqueue)

    args = parser.parse_args(argv_args)
    assert os.path.exists(args.config), \
        'Config file {} does not exist!'.format(args.config)
    with open(args.config) as fh:
        cfg = make_config_from_argparse(fh)

    args.func(cfg, args)
