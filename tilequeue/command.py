from contextlib import closing
from itertools import chain
from tilequeue.cache import deserialize_redis_value_to_coord
from tilequeue.cache import RedisCacheIndex
from tilequeue.cache import serialize_coord_to_redis_value
from tilequeue.config import make_config_from_argparse
from tilequeue.format import lookup_format_by_extension
from tilequeue.metro_extract import city_bounds
from tilequeue.metro_extract import parse_metro_extract
from tilequeue.queue import get_sqs_queue
from tilequeue.render import RenderJobCreator
from tilequeue.store import make_s3_store
from tilequeue.store import make_tile_file_store
from tilequeue.tile import deserialize_coord
from tilequeue.tile import explode_serialized_coords
from tilequeue.tile import explode_with_parents
from tilequeue.tile import parse_expired_coord_string
from tilequeue.tile import seed_tiles
from tilequeue.tile import serialize_coord
from tilequeue.tile import tile_generator_for_multiple_bounds
from tilequeue.worker import Worker
from TileStache import parseConfigfile
from urllib2 import urlopen
import argparse
import logging
import logging.config
import os
import sys


def add_config_options(parser):
    parser.add_argument('--config')
    return parser


def add_aws_cred_options(parser):
    parser.add_argument('--aws_access_key_id')
    parser.add_argument('--aws_secret_access_key')
    return parser


def add_queue_options(parser):
    parser.add_argument('--queue-name',
                        help='Name of the queue, should already exist.',
                        )
    parser.add_argument('--queue-type',
                        default='sqs',
                        choices=('sqs', 'mem', 'file', 'stdout'),
                        help='Queue type, useful to change for testing.',
                        )
    parser.add_argument('--sqs-read-timeout',
                        type=int,
                        default=20,
                        help='Read timeout in seconds when reading '
                             'sqs messages.',
                        )
    return parser


def add_s3_options(parser):
    parser.add_argument('--s3-bucket',
                        help='Name of aws s3 bucket, should already exist.',
                        )
    parser.add_argument('--s3-reduced-redundancy',
                        action='store_true',
                        default=False,
                        help='Store tile data in s3 with reduced redundancy.',
                        )
    parser.add_argument('--s3-path',
                        default='',
                        help='Store tile data in s3 with this path prefix.',
                        )
    return parser


def add_tilestache_config_options(parser):
    parser.add_argument('--tilestache-config',
                        help='Path to Tilestache config.',
                        )
    return parser


def add_output_format_options(parser):
    parser.add_argument('--output-formats',
                        nargs='+',
                        choices=('json', 'vtm', 'topojson', 'mapbox'),
                        default=('json', 'vtm'),
                        help='Output formats to produce for each tile.',
                        )
    return parser


def add_logging_options(parser):
    parser.add_argument('--logconfig',
                        help='Path to python logging config file.',
                        )
    return parser


def make_queue(queue_type, queue_name, cfg):
    if queue_type == 'sqs':
        return get_sqs_queue(cfg)
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


def add_expired_tiles_options(parser):
    parser.add_argument('--expired-tiles-file',
                        help='Path to file containing list of expired tiles. '
                             'Should be one per line, <zoom>/<column>/<row>',
                        )
    return parser


def tilequeue_parser_write(parser):
    parser = add_config_options(parser)
    parser = add_aws_cred_options(parser)
    parser = add_queue_options(parser)
    parser = add_logging_options(parser)
    parser = add_expired_tiles_options(parser)
    parser.set_defaults(func=tilequeue_write)
    return parser


def tilequeue_parser_read(parser):
    parser = add_config_options(parser)
    parser = add_aws_cred_options(parser)
    parser = add_queue_options(parser)
    parser = add_logging_options(parser)
    parser.set_defaults(func=tilequeue_read)
    return parser


def tilequeue_read(cfg):
    assert cfg.queue_name, 'Missing queue name'
    logger = make_logger(cfg, 'read')
    queue = make_queue(cfg.queue_type, cfg.queue_name, cfg)
    msgs = queue.read(max_to_read=1, timeout_seconds=cfg.read_timeout)
    if not msgs:
        logger.info('No messages found on queue: %s' % cfg.queue_name)
    for msg in msgs:
        coord = msg.coord
        logger.info('Received tile: %s' % serialize_coord(coord))


def tilequeue_parser_process(parser):
    parser = add_config_options(parser)
    parser = add_aws_cred_options(parser)
    parser = add_queue_options(parser)
    parser = add_s3_options(parser)
    parser = add_tilestache_config_options(parser)
    parser = add_output_format_options(parser)
    parser = add_logging_options(parser)
    parser.add_argument('--daemon',
                        action='store_true',
                        default=False,
                        help='Enable daemon mode, which will continue to poll '
                             'for messages',
                        )
    parser.set_defaults(func=tilequeue_process)
    return parser


def add_seed_options(parser):
    parser.add_argument('--zoom-start',
                        type=int,
                        default=0,
                        choices=xrange(21),
                        help='Zoom level to start seeding tiles with.',
                        )
    parser.add_argument('--zoom-until',
                        type=int,
                        default=0,
                        choices=xrange(21),
                        help='Zoom level to seed tiles until, inclusive.',
                        )
    parser.add_argument('--metro-extract-url',
                        help='Url to metro extracts json (or file://).',
                        )
    parser.add_argument('--filter-metro-zoom',
                        type=int,
                        default=0,
                        choices=xrange(21),
                        help='Zoom level to start filtering for '
                             'metro extracts.',
                        )
    parser.add_argument('--unique-tiles',
                        default=False,
                        action='store_true',
                        help='Only generate unique tiles. The bounding boxes '
                        'in metro extracts overlap, which will generate '
                        'duplicate tiles for the overlaps. This flag ensures '
                        'that the tiles will be unique.',
                        )
    return parser


def tilequeue_parser_seed(parser):
    parser = add_config_options(parser)
    parser = add_aws_cred_options(parser)
    parser = add_queue_options(parser)
    parser = add_logging_options(parser)
    parser = add_seed_options(parser)
    parser = add_redis_options(parser)
    parser.set_defaults(func=tilequeue_seed)
    return parser


def tilequeue_parser_generate_tile(parser):
    parser = add_config_options(parser)
    parser = add_aws_cred_options(parser)
    parser = add_s3_options(parser)
    parser = add_tilestache_config_options(parser)
    parser = add_output_format_options(parser)
    parser = add_logging_options(parser)
    parser.add_argument('--tile',
                        help='Tile coordinate used to generate a tile. Must '
                        'be of the form: <zoom>/<column>/<row>',
                        )
    parser.set_defaults(func=tilequeue_generate_tile)
    return parser


def assert_aws_config(cfg):
    if (cfg.aws_access_key_id is not None or
            cfg.aws_secret_access_key is not None):
        # assert that if either is specified, both are specified
        assert (cfg.aws_access_key_id is not None and
                cfg.aws_secret_access_key is not None), \
            'Must specify both aws key and secret'


def add_redis_options(parser):
    parser.add_argument('--redis-host',
                        help='Redis host',
                        )
    parser.add_argument('--redis-port',
                        type=int,
                        help='Redis port',
                        )
    parser.add_argument('--redis-db',
                        type=int,
                        help='Redis db',
                        )
    parser.add_argument('--redis-cache-set-key',
                        help='Redis key name of cache coordinates',
                        )
    parser.add_argument('--redis-diff-set-key',
                        help='Redis key name of diff coordinates',
                        )
    return parser


def tilequeue_parser_cache_index_diffs_load(parser):
    parser = add_config_options(parser)
    parser = add_redis_options(parser)
    parser = add_expired_tiles_options(parser)
    parser.set_defaults(func=tilequeue_cache_index_diffs_load)
    return parser


def tilequeue_parser_cache_index_diffs_intersect(parser):
    parser = add_config_options(parser)
    parser = add_redis_options(parser)
    parser.set_defaults(func=tilequeue_cache_index_diffs_intersect)
    return parser


def tilequeue_parser_cache_index_diffs_remove(parser):
    parser = add_config_options(parser)
    parser = add_redis_options(parser)
    parser.set_defaults(func=tilequeue_cache_index_diffs_remove)
    return parser


def tilequeue_parser_cache_index_seed(parser):
    parser = add_config_options(parser)
    parser = add_redis_options(parser)
    parser = add_seed_options(parser)
    parser.set_defaults(func=tilequeue_cache_index_seed)
    return parser


def tilequeue_parser_cache_index_tiles(parser):
    parser = add_config_options(parser)
    parser = add_redis_options(parser)
    parser.set_defaults(func=tilequeue_cache_index_tiles)
    return parser


def tilequeue_parser_explode(parser):
    parser = add_config_options(parser)
    parser = add_expired_tiles_options(parser)
    parser.add_argument('--explode-until',
                        type=int,
                        help='Generate tiles up until a particular zoom',
                        )
    parser.set_defaults(func=tilequeue_explode)
    return parser


def tilequeue_parser_drain(parser):
    parser = add_config_options(parser)
    parser = add_queue_options(parser)
    parser = add_logging_options(parser)
    parser.set_defaults(func=tilequeue_drain)
    return parser


def serialize_coords(coords):
    for coord in coords:
        serialized_coord = serialize_coord_to_redis_value(coord)
        yield serialized_coord


def deserialize_coords(serialized_coords):
    for serialized_coord in serialized_coords:
        coord = deserialize_redis_value_to_coord(serialized_coord)
        yield coord


def tilequeue_explode(cfg):
    assert cfg.expired_tiles_file, 'Missing expired tiles file'
    assert os.path.exists(cfg.expired_tiles_file), \
        'Invalid expired tiles path'
    with open(cfg.expired_tiles_file) as fp:
        expired_tiles = create_coords_generator_from_tiles_file(fp)

        # using serialized values in memory,
        # but need to pay for serialize/deserialize time
        serialized_coords = serialize_coords(expired_tiles)
        exploded_coords = explode_serialized_coords(
            serialized_coords, cfg.explode_until,
            serialize_fn=serialize_coord_to_redis_value,
            deserialize_fn=deserialize_redis_value_to_coord)
        for serialized_coord in exploded_coords:
            coord = deserialize_redis_value_to_coord(serialized_coord)
            coord_str = serialize_coord(coord)
            print coord_str

        # use direct coords
        # exploded_coords = explode_with_parents(
        #     expired_tiles, cfg.explode_until)
        # for coord in exploded_coords:
        #     coord_str = serialize_coord(coord)
        #     print coord_str


def tilequeue_drain(cfg):
    queue = make_queue(cfg.queue_type, cfg.queue_name, cfg)
    logger = make_logger(cfg, 'drain')
    logger.info('Draining queue ...')
    n = queue.clear()
    logger.info('Draining queue ... done')
    logger.info('Removed %d messages' % n)


def tilequeue_cache_index_seed(cfg):
    tile_generator = make_seed_tile_generator(cfg)
    redis_cache_index = make_redis_cache_index(cfg)
    out = sys.stdout
    redis_cache_index.write_coords_redis_protocol(
        out, cfg.redis_cache_set_key, tile_generator)


def tilequeue_cache_index_tiles(cfg):
    redis_cache_index = make_redis_cache_index(cfg)
    out = sys.stdout
    coords = redis_cache_index.cache_coords()
    for coord in coords:
        coord_str = serialize_coord(coord)
        out.write(coord_str + '\n')


def assert_redis_config(cfg):
    assert cfg.redis_host, 'Missing redis host'
    assert cfg.redis_port, 'Missing redis port'
    assert cfg.redis_cache_set_key, 'Missing redis cache set key name'


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


def make_redis_cache_index(cfg):
    assert_redis_config(cfg)
    from redis import StrictRedis
    redis_client = StrictRedis(cfg.redis_host, cfg.redis_port, cfg.redis_db)
    redis_cache_index = RedisCacheIndex(redis_client, cfg.redis_cache_set_key)
    return redis_cache_index


def deserialized_coords(serialized_coords):
    for serialized_coord in serialized_coords:
        coord = deserialize_redis_value_to_coord(serialized_coord)
        yield coord


def tilequeue_cache_index_diffs_load(cfg):
    assert cfg.expired_tiles_file, 'Missing expired tiles file'
    assert os.path.exists(cfg.expired_tiles_file), \
        'Invalid expired tiles path'
    assert cfg.redis_diff_set_key, 'Missing redis diff set key name'
    redis_cache_index = make_redis_cache_index(cfg)
    with open(cfg.expired_tiles_file) as fp:
        expired_tiles = create_coords_generator_from_tiles_file(fp)
        serialized_coords = serialize_coords(expired_tiles)
        exploded_serialized_coords = explode_serialized_coords(
            serialized_coords, cfg.explode_until,
            serialize_fn=serialize_coord_to_redis_value,
            deserialize_fn=deserialize_redis_value_to_coord)
        exploded_coords = deserialized_coords(exploded_serialized_coords)
        redis_cache_index.write_coords_redis_protocol(
            sys.stdout, cfg.redis_diff_set_key, exploded_coords)


def tilequeue_cache_index_diffs_intersect(cfg):
    assert cfg.redis_diff_set_key, 'Missing redis diff set key name'
    redis_cache_index = make_redis_cache_index(cfg)
    coords = redis_cache_index.find_intersection(cfg.redis_diff_set_key)
    for coord in coords:
        print serialize_coord(coord)


def tilequeue_cache_index_diffs_remove(cfg):
    assert cfg.redis_diff_set_key, 'Missing redis diff set key name'
    redis_cache_index = make_redis_cache_index(cfg)
    redis_cache_index.remove_key(cfg.redis_diff_set_key)


def tilequeue_write(cfg):

    assert_aws_config(cfg)

    assert os.path.exists(cfg.expired_tiles_file), \
        'Invalid expired tiles path'

    assert cfg.queue_name, 'Missing queue name'
    queue = make_queue(cfg.queue_type, cfg.queue_name, cfg)

    logger = make_logger(cfg, 'write')

    with open(cfg.expired_tiles_file) as fp:
        expired_tiles = create_coords_generator_from_tiles_file(fp, logger)
        exploded_coords = explode_with_parents(expired_tiles)

        logger.info('Queuing ... ')

        n_coords = queue.enqueue_batch(exploded_coords)

        logger.info('Queuing ... Done')
        logger.info('Queued %d tiles' % n_coords)


def lookup_formats(format_extensions):
    formats = []
    for extension in format_extensions:
        format = lookup_format_by_extension(extension)
        assert format is not None, 'Unknown extension: %s' % extension
        formats.append(format)
    return formats


def make_logger(cfg, logger_name):
    if getattr(cfg, 'logconfig') is not None:
        logging.config.fileConfig(cfg.logconfig)
    logger = logging.getLogger(logger_name)
    return logger


def tilequeue_process(cfg):
    assert_aws_config(cfg)

    logger = make_logger(cfg, 'process')

    assert os.path.exists(cfg.tilestache_config), \
        'Invalid tilestache config path'

    formats = lookup_formats(cfg.output_formats)

    queue = make_queue(cfg.queue_type, cfg.queue_name, cfg)

    tilestache_config = parseConfigfile(cfg.tilestache_config)

    store = make_s3_store(
        cfg.s3_bucket, cfg.aws_access_key_id, cfg.aws_secret_access_key,
        path=cfg.s3_path, reduced_redundancy=cfg.s3_reduced_redundancy)

    job_creator = RenderJobCreator(tilestache_config, formats, store)

    worker = Worker(queue, job_creator)
    worker.logger = logger
    worker.daemonized = cfg.daemon
    worker.process()


def uniquify_generator(generator):
    s = set(generator)
    for tile in s:
        yield tile


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

    assert cfg.zoom_start <= unfiltered_end_zoom

    unfiltered_tiles = seed_tiles(cfg.zoom_start, unfiltered_end_zoom)

    tile_generator = chain(unfiltered_tiles, filtered_tiles)

    return tile_generator


def tilequeue_seed(cfg):
    if cfg.queue_type == 'sqs':
        assert_aws_config(cfg)

    queue = make_queue(cfg.queue_type, cfg.queue_name, cfg)
    tile_generator = make_seed_tile_generator(cfg)

    logger = make_logger(cfg, 'seed')
    logger.info('Beginning to enqueue seed tiles')

    n_tiles = queue.enqueue_batch(tile_generator)

    logger.info('Queued %d tiles' % n_tiles)


def tilequeue_generate_tile(cfg):
    assert cfg.tile, 'Missing tile coordinate'
    tile_str = cfg.tile

    coord = deserialize_coord(tile_str)
    assert coord is not None, 'Could not parse tile from %s' % tile_str

    tilestache_config = parseConfigfile(cfg.tilestache_config)
    formats = lookup_formats(cfg.output_formats)

    if cfg.s3_bucket:
        store = make_s3_store(
            cfg.s3_bucket, cfg.aws_access_key_id, cfg.aws_secret_access_key,
            path=cfg.s3_path, reduced_redundancy=cfg.s3_reduced_redundancy)
    else:
        store = make_tile_file_store(sys.stdout)

    job_creator = RenderJobCreator(tilestache_config, formats, store)
    job_creator.process_jobs_for_coord(coord)

    sys.stdout = open("/dev/stdout", "w")
    logger = make_logger(cfg, 'generate_tile')
    logger.info('Generated tile for: %s' % tile_str)


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
        ('write', tilequeue_parser_write),
        ('process', tilequeue_parser_process),
        ('read', tilequeue_parser_read),
        ('seed', tilequeue_parser_seed),
        ('generate-tile', tilequeue_parser_generate_tile),
        ('explode', tilequeue_parser_explode),
        ('drain', tilequeue_parser_drain),
        ('cache-index-diffs-load', tilequeue_parser_cache_index_diffs_load),
        ('cache-index-diffs-intersect',
         tilequeue_parser_cache_index_diffs_intersect),
        ('cache-index-diffs-remove',
         tilequeue_parser_cache_index_diffs_remove),
        ('cache-index-seed', tilequeue_parser_cache_index_seed),
        ('cache-index-tiles', tilequeue_parser_cache_index_tiles),
    )
    for parser_name, parser_func in parser_config:
        subparser = subparsers.add_parser(parser_name)
        parser_func(subparser)

    args = parser.parse_args(argv_args)
    cfg = make_config_from_argparse(args)
    args.func(cfg)
