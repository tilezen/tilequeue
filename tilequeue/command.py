from tilequeue.format import lookup_format_by_extension
from tilequeue.queue import make_sqs_queue
from tilequeue.render import RenderJobCreator
from tilequeue.store import make_s3_store
from tilequeue.tile import explode_with_parents
from tilequeue.tile import parse_expired_coord_string
from TileStache import parseConfigfile
import argparse
import os
import sys

def add_aws_cred_options(arg_parser):
    arg_parser.add_argument('--aws_access_key_id')
    arg_parser.add_argument('--aws_secret_access_key')
    return arg_parser

def queue_write_parser():
    parser = argparse.ArgumentParser()
    parser = add_aws_cred_options(parser)
    parser.add_argument('--queue',
                        required=True,
                        help='Name of aws sqs queue, should already exist.',
                        )
    parser.add_argument('--expired-tiles-file',
                        required=True,
                        help='Path to file containing list of expired tiles. Should be one per line, <zoom>/<column>/<row>',
                        )
    return parser

def queue_read_parser():
    parser = argparse.ArgumentParser()
    parser = add_aws_cred_options(parser)
    parser.add_argument('--queue',
                        required=True,
                        help='Name of aws sqs queue, should already exist.',
                        )
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

if __name__ == '__main__':
    #queue_read()
    #queue_write()
    pass
