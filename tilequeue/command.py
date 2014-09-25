from contextlib import closing
from itertools import chain
from tilequeue.format import lookup_format_by_extension
from tilequeue.metro_extract import city_bounds
from tilequeue.metro_extract import parse_metro_extract
from tilequeue.queue import make_sqs_queue
from tilequeue.render import RenderJobCreator
from tilequeue.seed import seed_tiles
from tilequeue.store import make_s3_store
from tilequeue.tile import explode_with_parents
from tilequeue.tile import parse_expired_coord_string
from tilequeue.tile import tile_generator_for_multiple_bounds
from TileStache import parseConfigfile
from urllib2 import urlopen
import argparse
import os
import sys


def add_aws_cred_options(parser):
    parser.add_argument('--aws_access_key_id')
    parser.add_argument('--aws_secret_access_key')
    return parser


def add_queue_options(parser):
    parser.add_argument('--queue-name',
                        required=True,
                        help='Name of the queue, should already exist.',
                        )
    parser.add_argument('--queue-type',
                        default='sqs',
                        choices=('sqs', 'mem', 'file'),
                        help='Queue type, useful to change for testing.',
                        )
    return parser


def make_queue(queue_type, queue_name, parser_args):
    if queue_type == 'sqs':
        return make_sqs_queue(
            queue_name,
            parser_args.aws_access_key_id, parser_args.aws_secret_access_key
        )
    elif queue_type == 'mem':
        from tilequeue.queue import MemoryQueue
        return MemoryQueue()
    elif queue_type == 'file':
        # only support file queues for writing
        # useful for testing
        from tilequeue.queue import OutputFileQueue
        fp = open(queue_name, 'w')
        return OutputFileQueue(fp)
    else:
        raise ValueError('Unknown queue type: %s' % queue_type)


def queue_write_parser():
    parser = argparse.ArgumentParser()
    parser = add_aws_cred_options(parser)
    parser = add_queue_options(parser)
    parser.add_argument('--expired-tiles-file',
                        required=True,
                        help='Path to file containing list of expired tiles. '
                             'Should be one per line, <zoom>/<column>/<row>',
                        )
    return parser


def queue_read_parser():
    parser = argparse.ArgumentParser()
    parser = add_aws_cred_options(parser)
    parser = add_queue_options(parser)
    parser.add_argument('--s3-bucket',
                        required=True,
                        help='Name of aws s3 bucket, should already exist.',
                        )
    parser.add_argument('--tilestache-config',
                        required=True,
                        help='Path to Tilestache config.',
                        )
    parser.add_argument('--output-formats',
                        nargs='+',
                        choices=('json', 'vtm', 'topojson', 'mapbox'),
                        default=('json', 'vtm'),
                        help='Output formats to produce for each tile.',
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
    parser.add_argument('--sqs-read-timeout',
                        type=int,
                        default=20,
                        help='Read timeout in seconds when reading '
                             'sqs messages.',
                        )
    return parser


def queue_seed_parser():
    parser = argparse.ArgumentParser()
    parser = add_aws_cred_options(parser)
    parser = add_queue_options(parser)
    parser.add_argument('--zoom-start',
                        type=int,
                        default=0,
                        choices=xrange(22),
                        help='Zoom level to start seeding tiles with.',
                        )
    parser.add_argument('--zoom-until',
                        type=int,
                        choices=xrange(22),
                        required=True,
                        help='Zoom level to seed tiles until, inclusive.',
                        )
    parser.add_argument('--metro-extract-url',
                        help='Url to metro extracts json (or file://).',
                        required=True,
                        )
    parser.add_argument('--filter-metro-zoom',
                        type=int,
                        default=11,
                        choices=xrange(22),
                        required=True,
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


def assert_aws_config(args):
    if (args.aws_access_key_id is not None or
            args.aws_secret_access_key is not None):
        # assert that if either is specified, both are specified
        assert (args.aws_access_key_id is not None and
                args.aws_secret_access_key is not None), \
            'Must specify both aws key and secret'
    else:
        assert 'AWS_ACCESS_KEY_ID' in os.environ, \
            'Missing AWS_ACCESS_KEY_ID config'
        assert 'AWS_SECRET_ACCESS_KEY' in os.environ, \
            'Missing AWS_SECRET_ACCESS_KEY config'


def queue_write(argv_args=None):
    if argv_args is None:
        argv_args = sys.argv[1:]
    parser = queue_write_parser()
    args = parser.parse_args(argv_args)
    assert_aws_config(args)

    assert os.path.exists(args.expired_tiles_file), \
        'Invalid expired tiles path'

    queue = make_queue(args.queue_type, args.queue_name, args)

    expired_tiles = []
    with open(args.expired_tiles_file) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            coord = parse_expired_coord_string(line)
            if coord is None:
                print 'Could not parse coordinate from line: ' % line
                continue
            expired_tiles.append(coord)

    print 'Number of expired tiles: %d' % len(expired_tiles)

    exploded_coords = explode_with_parents(expired_tiles)
    print ('Number of total expired tiles with all parents: %d' %
           len(exploded_coords))

    print 'Queuing ... '

    # sort in any way?

    # zoom level strategy?
    # only enqueue work for zooms > 10 if in metro extract area?

    # exploded_coords is a set, but enqueue_batch expects a list for slicing
    exploded_coords = list(exploded_coords)

    queue.enqueue_batch(list(exploded_coords))

    print 'Queuing ... Done'


def queue_read(argv_args=None):
    if argv_args is None:
        argv_args = sys.argv[1:]
    parser = queue_read_parser()
    args = parser.parse_args(argv_args)
    assert_aws_config(args)

    assert os.path.exists(args.tilestache_config), \
        'Invalid tilestache config path'

    formats = []
    for extension in args.output_formats:
        format = lookup_format_by_extension(extension)
        assert format is not None, 'Unknown extension: %s' % extension
        formats.append(format)

    queue = make_queue(args.queue_type, args.queue_name, args)

    tilestache_config = parseConfigfile(args.tilestache_config)
    job_creator = RenderJobCreator(tilestache_config, formats)

    store = make_s3_store(
        args.s3_bucket, args.aws_access_key_id, args.aws_secret_access_key,
        path=args.s3_path, reduced_redundancy=args.s3_reduced_redundancy)

    n_msgs = 0
    while True:
        msgs = queue.read(max_to_read=1, timeout_seconds=args.sqs_read_timeout)
        if not msgs:
            break
        for msg in msgs:
            coord = msg.coord
            jobs = job_creator.create(coord)
            for job in jobs:
                result = job()
                with closing(store.output_fp(coord, job.format)) as s:
                    s.write(result)
            queue.job_done(msg.message_handle)
            n_msgs += 1

    print 'processed %d messages' % n_msgs


def queue_seed_process(tile_generator, queue):
    # enqueue in batches of 10
    batch = []
    n_tiles = 0
    for tile in tile_generator:
        batch.append(tile)
        n_tiles += 1
        if len(batch) >= 10:
            queue.enqueue_batch(batch)
            batch = []
    if batch:
        queue.enqueue_batch(batch)
    return n_tiles


def uniquify_generator(generator):
    s = set(generator)
    for tile in s:
        yield tile


def queue_seed(argv_args=None):
    if argv_args is None:
        argv_args = sys.argv[1:]
    parser = queue_seed_parser()
    args = parser.parse_args(argv_args)
    if args.queue_type == 'sqs':
        assert_aws_config(args)

    with closing(urlopen(args.metro_extract_url)) as fp:
        # will raise a MetroExtractParseError on failure
        metro_extracts = parse_metro_extract(fp)
    multiple_bounds = city_bounds(metro_extracts)

    assert args.zoom_start <= (args.filter_metro_zoom - 1)
    assert args.filter_metro_zoom <= args.zoom_until

    unfiltered_tiles = seed_tiles(args.zoom_start, args.filter_metro_zoom - 1)
    filtered_tiles = tile_generator_for_multiple_bounds(
        multiple_bounds, args.filter_metro_zoom, args.zoom_until)

    # unique tiles will force storing a set in memory
    if args.unique_tiles:
        filtered_tiles = uniquify_generator(filtered_tiles)

    tile_generator = chain(unfiltered_tiles, filtered_tiles)

    queue = make_queue(args.queue_type, args.queue_name, args)

    n_tiles = queue_seed_process(tile_generator, queue)

    print 'Queued %d tiles' % n_tiles

if __name__ == '__main__':
    # queue_read()
    # queue_write()
    # queue_seed()
    pass
