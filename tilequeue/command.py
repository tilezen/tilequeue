from collections import namedtuple
from contextlib import closing
from itertools import chain
from Queue import Queue
from threading import Lock
from threading import Thread
from tilequeue.cache import deserialize_redis_value_to_coord
from tilequeue.cache import RedisCacheIndex
from tilequeue.cache import serialize_coord_to_redis_value
from tilequeue.config import make_config_from_argparse
from tilequeue.format import lookup_format_by_extension
from tilequeue.metro_extract import city_bounds
from tilequeue.metro_extract import parse_metro_extract
from tilequeue.queue import get_sqs_queue
from tilequeue.render import make_feature_fetcher
from tilequeue.render import RenderJobCreator
from tilequeue.store import make_s3_store
from tilequeue.tile import explode_serialized_coords
from tilequeue.tile import parse_expired_coord_string
from tilequeue.tile import seed_tiles
from tilequeue.tile import tile_generator_for_multiple_bounds
from tilequeue.top_tiles import parse_top_tiles
from tilequeue.utils import trap_signal
from tilequeue.worker import Worker
from TileStache import parseConfigfile
from urllib2 import urlopen
import argparse
import logging
import logging.config
import multiprocessing
import os
import sys


def create_command_parser(fn):
    def create_parser_fn(parser):
        parser.add_argument('--config')
        parser.set_defaults(func=fn)
        return parser
    return create_parser_fn


def serialize_coords_redis_values(coords):
    for coord in coords:
        serialized_coord = serialize_coord_to_redis_value(coord)
        yield serialized_coord


def deserialize_coords_redis_values(serialized_coords):
    for serialized_coord in serialized_coords:
        coord = deserialize_redis_value_to_coord(serialized_coord)
        yield coord


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


def make_queue(queue_type, queue_name, cfg):
    if queue_type == 'sqs':
        return get_sqs_queue(cfg.queue_name,
                             cfg.redis_host, cfg.redis_port, cfg.redis_db,
                             cfg.aws_access_key_id, cfg.aws_secret_access_key)
    elif queue_type == 'mem':
        from tilequeue.queue import MemoryQueue
        return MemoryQueue()
    elif queue_type == 'file':
        # only support file queues for writing
        # useful for testing
        from tilequeue.queue import OutputFileQueue
        fp = open(queue_name, 'w')
        return OutputFileQueue(fp)
    elif queue_type == 'stdout':
        # only support writing
        from tilequeue.queue import OutputFileQueue
        return OutputFileQueue(sys.stdout)
    else:
        raise ValueError('Unknown queue type: %s' % queue_type)


def make_redis_cache_index(cfg):
    from redis import StrictRedis
    redis_client = StrictRedis(cfg.redis_host, cfg.redis_port, cfg.redis_db)
    redis_cache_index = RedisCacheIndex(redis_client, cfg.redis_cache_set_key)
    return redis_cache_index


def make_logger(cfg, logger_name):
    if getattr(cfg, 'logconfig') is not None:
        logging.config.fileConfig(cfg.logconfig)
    logger = logging.getLogger(logger_name)
    return logger


def make_seed_tile_generator(cfg):
    if cfg.metro_extract_url:
        assert cfg.filter_metro_zoom is not None, \
            '--filter-metro-zoom is required when specifying a ' \
            'metro extract url'
        assert cfg.filter_metro_zoom <= cfg.zoom_until
        with closing(urlopen(cfg.metro_extract_url)) as fp:
            # will raise a MetroExtractParseError on failure
            metro_extracts = parse_metro_extract(fp)
        multiple_bounds = city_bounds(metro_extracts)
        filtered_tiles = tile_generator_for_multiple_bounds(
            multiple_bounds, cfg.filter_metro_zoom, cfg.zoom_until)
        # unique tiles will force storing a set in memory
        if cfg.unique_tiles:
            filtered_tiles = uniquify_generator(filtered_tiles)
        unfiltered_end_zoom = cfg.filter_metro_zoom - 1
    else:
        assert not cfg.filter_metro_zoom, \
            '--metro-extract-url is required when specifying ' \
            '--filter-metro-zoom'
        filtered_tiles = ()
        unfiltered_end_zoom = cfg.zoom_until

    if cfg.top_tiles_url:
        assert cfg.top_tiles_zoom_start, 'Missing top tiles zoom start'
        assert cfg.top_tiles_zoom_until, 'Missing top tiles zoom until'
        with closing(urlopen(cfg.top_tiles_url)) as fp:
            top_tiles = parse_top_tiles(
                fp, cfg.top_tiles_zoom_start, cfg.top_tiles_zoom_until)
    else:
        top_tiles = ()

    assert cfg.zoom_start <= unfiltered_end_zoom

    unfiltered_tiles = seed_tiles(cfg.zoom_start, unfiltered_end_zoom)

    dynamic_tiles = chain(filtered_tiles, top_tiles)
    if cfg.unique_tiles:
        dynamic_tiles = uniquify_generator(dynamic_tiles)

    tile_generator = chain(unfiltered_tiles, dynamic_tiles)

    return tile_generator


def tilequeue_drain(cfg, peripherals):
    queue = make_queue(cfg.queue_type, cfg.queue_name, cfg)
    logger = make_logger(cfg, 'drain')
    logger.info('Draining queue ...')
    n = queue.clear()
    logger.info('Draining queue ... done')
    logger.info('Removed %d messages' % n)


def tilequeue_intersect(cfg, peripherals):
    logger = make_logger(cfg, 'intersect')
    logger.info("Intersecting expired tiles with tiles of interest")
    sqs_queue = peripherals.queue

    assert cfg.expired_tiles_location, \
        'Missing tiles expired-location configuration'
    assert os.path.isdir(cfg.expired_tiles_location), \
        'tiles expired-location is not a directory'

    file_names = os.listdir(cfg.expired_tiles_location)
    if not file_names:
        logger.info('No expired tiles found, terminating.')
        return
    file_names.sort()
    expired_tile_paths = [os.path.join(cfg.expired_tiles_location, x)
                          for x in file_names]

    logger.info('Fetching tiles of interest ...')
    tiles_of_interest = peripherals.redis_cache_index.fetch_tiles_of_interest()
    logger.info('Fetching tiles of interest ... done')

    logger.info('Will process %d expired tile files.'
                % len(expired_tile_paths))

    lock = Lock()
    totals = dict(enqueued=0, in_flight=0)
    thread_queue_buffer_size = 1000
    thread_queue = Queue(thread_queue_buffer_size)

    # each thread will enqueue coords to sqs
    def enqueue_coords():
        buf = []
        buf_size = 10
        while True:
            coord = thread_queue.get()
            if coord is None:
                break
            buf.append(coord)
            if len(buf) >= buf_size:
                n_queued, n_in_flight = sqs_queue.enqueue_batch(buf)
                with lock:
                    totals['enqueued'] += n_queued
                    totals['in_flight'] += n_in_flight
                del buf[:]
        if buf:
            n_queued, n_in_flight = sqs_queue.enqueue_batch(buf)
            with lock:
                totals['enqueued'] += n_queued
                totals['in_flight'] += n_in_flight

    # clamp number of threads between 5 and 20
    n_threads = max(min(len(expired_tile_paths), 20), 5)
    # start up threads
    threads = []
    for i in range(n_threads):
        thread = Thread(target=enqueue_coords)
        thread.start()
        threads.append(thread)

    for expired_tile_path in expired_tile_paths:
        stat_result = os.stat(expired_tile_path)
        file_size = stat_result.st_size
        file_size_in_kilobytes = file_size / 1024
        logger.info('Processing %s. Size: %dK' %
                    (expired_tile_path, file_size_in_kilobytes))
        with open(expired_tile_path) as fp:
            expired_tiles = create_coords_generator_from_tiles_file(fp)
            serialized_coords = serialize_coords_redis_values(expired_tiles)
            exploded_serialized_coords = explode_serialized_coords(
                serialized_coords, cfg.explode_until,
                serialize_fn=serialize_coord_to_redis_value,
                deserialize_fn=deserialize_redis_value_to_coord)
            exploded_coords = deserialize_coords_redis_values(
                exploded_serialized_coords)

            # add work for threads
            coords_to_enqueue = peripherals.redis_cache_index.intersect(
                exploded_coords, tiles_of_interest)

            for coord in coords_to_enqueue:
                thread_queue.put(coord)

    for thread in threads:
        # threads stop on None sentinel
        thread_queue.put(None)

    # wait for all threads to terminate
    for thread in threads:
        thread.join()

    # print results
    for expired_tile_path in expired_tile_paths:
        logger.info('Processing complete: %s' % expired_tile_path)
        os.remove(expired_tile_path)
        logger.info('Removed: %s' % expired_tile_path)

    logger.info('%d tiles enqueued. %d tiles in flight.' %
                (totals['enqueued'], totals['in_flight']))

    logger.info('Intersection complete.')


def tilequeue_process(cfg, peripherals):
    logger = make_logger(cfg, 'process')

    assert os.path.exists(cfg.tilestache_config), \
        'Invalid tilestache config path'

    formats = lookup_formats(cfg.output_formats)

    queue = make_queue(cfg.queue_type, cfg.queue_name, cfg)

    tilestache_config = parseConfigfile(cfg.tilestache_config)

    store = make_s3_store(
        cfg.s3_bucket, cfg.aws_access_key_id, cfg.aws_secret_access_key,
        path=cfg.s3_path, reduced_redundancy=cfg.s3_reduced_redundancy)

    assert cfg.postgresql_conn_info, 'Missing postgresql connection info'
    feature_fetcher = make_feature_fetcher(
        cfg.postgresql_conn_info, tilestache_config, formats)
    job_creator = RenderJobCreator(
        tilestache_config, formats, store, feature_fetcher)

    workers = []
    for i in range(cfg.workers):
        worker = Worker(queue, job_creator)
        worker.logger = logger
        worker.daemonized = cfg.daemon
        p = multiprocessing.Process(target=worker.process,
                                    args=(cfg.messages_at_once,))
        workers.append(p)
        p.start()
    trap_signal()


def queue_generator(queue):
    while True:
        data = queue.get()
        if data is None:
            break
        yield data


def tilequeue_seed(cfg, peripherals):
    queue = make_queue(cfg.queue_type, cfg.queue_name, cfg)
    tile_generator = make_seed_tile_generator(cfg)

    # updating sqs and updating redis happen in background threads
    def sqs_enqueue(tile_gen):
        n_enqueued, n_in_flight = queue.enqueue_batch(tile_gen)

    def redis_add(tile_gen):
        peripherals.redis_cache_index.index_coords(tile_gen)

    queue_buf_size = 1000
    queue_sqs_coords = Queue(queue_buf_size)
    queue_redis_coords = Queue(queue_buf_size)

    # suppresses checking the in flight list while seeding
    queue.is_seeding = True

    # use multiple sqs threads
    n_sqs_threads = 3

    sqs_threads = [Thread(target=sqs_enqueue,
                          args=(queue_generator(queue_sqs_coords),))
                   for x in range(n_sqs_threads)]
    thread_redis = Thread(target=redis_add,
                          args=(queue_generator(queue_redis_coords),))

    logger = make_logger(cfg, 'seed')
    logger.info('Sqs ... ')
    logger.info('Tiles of interest ...')

    for thread_sqs in sqs_threads:
        thread_sqs.start()
    thread_redis.start()

    for tile in tile_generator:
        queue_sqs_coords.put(tile)
        queue_redis_coords.put(tile)

    # None is sentinel value
    for i in range(n_sqs_threads):
        queue_sqs_coords.put(None)
    queue_redis_coords.put(None)

    thread_redis.join()
    logger.info('Tiles of interest ... done')
    for thread_sqs in sqs_threads:
        thread_sqs.join()
    logger.info('Sqs ... done')


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
    )
    for parser_name, parser_func in parser_config:
        subparser = subparsers.add_parser(parser_name)
        parser_func(subparser)

    args = parser.parse_args(argv_args)
    cfg = make_config_from_argparse(args.config)
    Peripherals = namedtuple('Peripherals', 'redis_cache_index queue')
    peripherals = Peripherals(make_redis_cache_index(cfg),
                              make_queue(cfg.queue_type, cfg.queue_name, cfg))
    args.func(cfg, peripherals)
