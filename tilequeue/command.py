from collections import namedtuple
from contextlib import closing
from itertools import chain
from Queue import Queue
from threading import Lock
from threading import Thread
from tilequeue.cache import coord_int_zoom_up
from tilequeue.cache import deserialize_redis_value_to_coord
from tilequeue.cache import RedisCacheIndex
from tilequeue.cache import serialize_coord_to_redis_value
from tilequeue.cache.redis_cache_index import zoom_mask
from tilequeue.config import make_config_from_argparse
from tilequeue.format import lookup_format_by_extension
from tilequeue.metro_extract import city_bounds
from tilequeue.metro_extract import parse_metro_extract
from tilequeue.queue import get_sqs_queue
from tilequeue.render import make_feature_fetcher
from tilequeue.render import RenderJobCreator
from tilequeue.store import make_s3_store
from tilequeue.tile import parse_expired_coord_string
from tilequeue.tile import seed_tiles
from tilequeue.tile import tile_generator_for_multiple_bounds
from tilequeue.tile import tile_generator_for_single_bounds
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
                coord_int = serialize_coord_to_redis_value(coord)
                coord_set.add(coord_int)
    return coord_set


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
    # cap the total number of files that we process in one shot
    # this will limit memory usage, as well as keep progress moving
    # along more consistently rather than bursts
    expired_tile_files_cap = 20
    file_names = file_names[:expired_tile_files_cap]
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

        def _enqueue():
            n_queued, n_in_flight = sqs_queue.enqueue_batch(buf)
            with lock:
                totals['enqueued'] += n_queued
                totals['in_flight'] += n_in_flight

        while True:
            coord = thread_queue.get()
            if coord is None:
                break
            buf.append(coord)
            if len(buf) >= buf_size:
                _enqueue()
                del buf[:]
        if buf:
            _enqueue()

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

    # This will store all coords from all paths as integers in a
    # set. A set is used because if the same tile has been expired in
    # more than one file, we only process it once
    all_coord_ints_set = coord_ints_from_paths(expired_tile_paths)
    logger.info('Unique expired tiles read to process: %d' %
                len(all_coord_ints_set))
    for coord_int in explode_and_intersect(
            all_coord_ints_set, tiles_of_interest, until=cfg.explode_until):
        coord = deserialize_redis_value_to_coord(coord_int)
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


def tilequeue_enqueue_tiles_of_interest(cfg, peripherals):
    logger = make_logger(cfg, 'enqueue_tiles_of_interest')
    logger.info('Enqueueing tiles of interest')

    sqs_queue = make_queue(cfg.queue_type, cfg.queue_name, cfg)
    logger.info('Fetching tiles of interest ...')
    tiles_of_interest = peripherals.redis_cache_index.fetch_tiles_of_interest()
    logger.info('Fetching tiles of interest ... done')

    thread_queue_buffer_size = 5000
    thread_queue = Queue(thread_queue_buffer_size)
    n_threads = 50

    lock = Lock()
    totals = dict(enqueued=0, in_flight=0)

    def enqueue_coords_thread():
        buf = []
        buf_size = 10

        def _enqueue():
            n_queued, n_in_flight = sqs_queue.enqueue_batch(buf)
            with lock:
                totals['enqueued'] += n_queued
                totals['in_flight'] += n_in_flight

        while True:
            coord = thread_queue.get()
            if coord is None:
                break
            buf.append(coord)
            if len(buf) >= buf_size:
                _enqueue()
                del buf[:]
        if buf:
            _enqueue()

    logger.info('Starting %d enqueueing threads ...' % n_threads)
    threads = []
    for i in xrange(n_threads):
        thread = Thread(target=enqueue_coords_thread)
        thread.start()
        threads.append(thread)
    logger.info('Starting %d enqueueing threads ... done' % n_threads)

    logger.info('Starting to enqueue coordinates - will process %d tiles'
                % len(tiles_of_interest))

    def log_totals():
        with lock:
            logger.info('%d processed - %d enqueued, %d in flight' % (
                totals['enqueued'] + totals['in_flight'],
                totals['enqueued'], totals['in_flight']))

    progress_queue = Queue()
    progress_interval_seconds = 120

    def log_progress_thread():
        while True:
            try:
                progress_queue.get(timeout=progress_interval_seconds)
            except:
                log_totals()
            else:
                break

    progress_thread = Thread(target=log_progress_thread)
    progress_thread.start()

    for tile_of_interest_value in tiles_of_interest:
        coord = deserialize_redis_value_to_coord(tile_of_interest_value)
        thread_queue.put(coord)

    for i in xrange(n_threads):
        thread_queue.put(None)

    for thread in threads:
        thread.join()

    progress_queue.put(None)
    progress_thread.join()

    logger.info('All tiles of interest processed')
    log_totals()


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
    work = Queue(work_buffer_size)

    from boto import connect_s3
    from boto.s3.bucket import Bucket
    s3_conn = connect_s3(cfg.aws_access_key_id, cfg.aws_secret_access_key)
    bucket = Bucket(s3_conn, bucket_name)

    lock = Lock()

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
        worker_thread = Thread(target=process_work_data)
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
