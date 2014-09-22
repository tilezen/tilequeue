from functools import partial
from itertools import ifilter
from tilequeue.format import lookup_format_by_extension
from tilequeue.metro_extract import city_bboxes
from tilequeue.metro_extract import create_spatial_index
from tilequeue.metro_extract import make_metro_extract_predicate
from tilequeue.metro_extract import parse_metro_extract
from tilequeue.queue import make_sqs_queue
from tilequeue.render import RenderJobCreator
from tilequeue.seed import seed_tiles
from tilequeue.store import make_s3_store
from tilequeue.tile import explode_with_parents
from tilequeue.tile import parse_expired_coord_string
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
    parser.add_argument('--queue',
                        required=True,
                        help='Name of aws sqs queue, should already exist.',
                        )
    return parser

def queue_write_parser():
    parser = argparse.ArgumentParser()
    parser = add_aws_cred_options(parser)
    parser = add_queue_options(parser)
    parser.add_argument('--expired-tiles-file',
                        required=True,
                        help='Path to file containing list of expired tiles. Should be one per line, <zoom>/<column>/<row>',
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
                        help='Read timeout in seconds when reading sqs messages.',
                        )
    return parser

def queue_seed_parser():
    parser = argparse.ArgumentParser()
    parser = add_aws_cred_options(parser)
    parser = add_queue_options(parser)
    parser.add_argument('--until-zoom',
                        type=int,
                        default=10,
                        choices=xrange(22),
                        required=True,
                        help='Zoom level to seed tiles until, inclusive.',
                        )
    parser.add_argument('--metro-extract-url',
                        help='Url to metro extracts (or file://).',
                        )
    parser.add_argument('--filter-metro-zoom',
                        type=int,
                        default=11,
                        choices=xrange(22),
                        help='Zoom level to start filtering for metro extracts.',
                        )
    return parser


def assert_aws_config(args):
    if (args.aws_access_key_id is not None or
        args.aws_secret_access_key is not None):
        # assert that if either is specified, both are specified
        assert (args.aws_access_key_id is not None and
                args.aws_secret_access_key is not None), 'Must specify both aws key and secret'
    else:
        assert 'AWS_ACCESS_KEY_ID' in os.environ, 'Missing AWS_ACCESS_KEY_ID config'
        assert 'AWS_SECRET_ACCESS_KEY' in os.environ, 'Missing AWS_SECRET_ACCESS_KEY config'


def queue_write(argv_args=None):
    if argv_args is None:
        argv_args = sys.argv[1:]
    parser = queue_write_parser()
    args = parser.parse_args(argv_args)
    assert_aws_config(args)

    assert os.path.exists(args.expired_tiles_file), 'Invalid expired tiles path'

    queue = make_sqs_queue(
            args.queue, args.aws_access_key_id, args.aws_secret_access_key)

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
    print 'Number of total expired tiles with all parents: %d' % len(exploded_coords)

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

    assert os.path.exists(args.tilestache_config), 'Invalid tilestache config path'

    formats = []
    for extension in args.output_formats:
        format = lookup_format_by_extension(extension)
        assert format is not None, 'Unknown extension: %s' % extension
        formats.append(format)

    queue = make_sqs_queue(
            args.queue, args.aws_access_key_id, args.aws_secret_access_key)

    tilestache_config = parseConfigfile(args.tilestache_config)
    job_creator = RenderJobCreator(tilestache_config, formats)

    store = make_s3_store(args.s3_bucket, args.aws_access_key_id, args.aws_secret_access_key, path=args.s3_path, reduced_redundancy=args.s3_reduced_redundancy)

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
                with store.output_fp(coord, job.format) as s:
                    s.write(result)
            queue.job_done(msg.message_handle)
            n_msgs += 1

    print 'processed %d messages' % n_msgs

def queue_seed(argv_args=None):
    if argv_args is None:
        argv_args = sys.argv[1:]
    parser = queue_seed_parser()
    args = parser.parse_args(argv_args)
    assert_aws_config(args)

    tile_generator = partial(seed_tiles, args.until_zoom)

    if args.metro_extract_url:
        assert args.filter_metro_zoom is not None, 'Need to specify --filter-metro-zoom if specifying metro extract url'
        with urlopen(args.metro_extract_url) as fp:
            # will raise a MetroExtractParseError on failure
            metro_extracts = parse_metro_extract(fp)
        bboxes = city_bboxes(metro_extracts)
        spatial_index = create_spatial_index(bboxes)
        predicate = make_metro_extract_predicate(spatial_index, args.filter_metro_zoom)
        tile_generator = partial(ifilter, predicate, tile_generator)

    queue = make_sqs_queue(
            args.queue, args.aws_access_key_id, args.aws_secret_access_key)

    # enqueue in batches of 10
    batch = []
    n_tiles = 0
    for tile in tile_generator():
        batch.append(tile)
        n_tiles += 1
        if len(batch) >= 10:
            queue.enqueue_batch(batch)
            batch = []

    print 'Queued %d tiles' % n_tiles

if __name__ == '__main__':
    #queue_read()
    #queue_write()
    #queue_seed()
    pass
