from tilequeue.queues import make_sqs_queue
from tilequeue.tile import explode_with_parents
from tilequeue.tile import parse_expired_coord_string
import argparse
import os

def add_aws_cred_options(arg_parser):
    arg_parser.add_argument('--aws_access_key_id')
    arg_parser.add_argument('--aws_secret_access_key')
    return arg_parser

def enqueue_arg_parser():
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

def assert_aws_config(args):
    if (args.aws_access_key_id is not None or
        args.aws_secret_access_key is not None):
        # assert that if either is specified, both are specified
        assert (args.aws_access_key_id is not None and
                args.aws_secret_access_key is not None), 'Must specify both aws key and secret'
    else:
        assert 'AWS_ACCESS_KEY_ID' in os.environ, 'Missing AWS_ACCESS_KEY_ID config'
        assert 'AWS_SECRET_ACCESS_KEY' in os.environ, 'Missing AWS_SECRET_ACCESS_KEY config'


def enqueue_process_main():
    parser = enqueue_arg_parser()
    args = parser.parse_args()
    assert_aws_config(args)

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

if __name__ == '__main__':
    enqueue_process_main()
