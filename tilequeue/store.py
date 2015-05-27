# define locations to store the rendered data

from boto import connect_s3
from boto.s3.bucket import Bucket
from TileStache.S3 import tile_key
import os


class S3(object):

    def __init__(self, bucket, layer_name, path='', reduced_redundancy=False):
        self.bucket = bucket
        self.layer = StubLayer(layer_name)
        self.path = path
        self.reduced_redundancy = reduced_redundancy

    def write_tile(self, tile_data, coord, format):
        key_name = tile_key(self.layer, coord, format.extension, self.path)
        key = self.bucket.new_key(key_name)
        key.set_contents_from_string(
            tile_data,
            headers={'Content-Type': format.mimetype},
            policy='public-read',
            reduced_redundancy=self.reduced_redundancy,
        )


class StubLayer(object):

    def __init__(self, layer_name):
        self.layer_name = layer_name

    def name(self):
        return self.layer_name


class TileDirectory(object):
    """
    Writes tiles to individual files in a local directory.
    """

    def __init__(self, dir_path):
        if os.path.exists(dir_path):
            if not os.path.isdir(dir_path):
                raise IOError(
                    '`{}` exists and is not a directory!'.format(dir_path))
        else:
            os.makedirs(dir_path)

        self.dir_path = dir_path

    def write_tile(self, tile_data, coord, format):
        filename = '{0}/{1}-{2}-{3}.{4}'.format(
            self.dir_path, coord.zoom, coord.column, coord.row,
            format.extension)
        with open(filename, 'w') as tile_fp:
            tile_fp.write(tile_data)


def make_tile_file_store(dir_path=None):
    if dir_path is None:
        dir_path = "tiles"
    return TileDirectory(dir_path)


class Memory(object):

    def __init__(self):
        self.data = None

    def write_tile(self, tile_data, coord, format):
        self.data = tile_data, coord, format


def make_s3_store(bucket_name,
                  aws_access_key_id=None, aws_secret_access_key=None,
                  layer_name='all', path='', reduced_redundancy=False):
    conn = connect_s3(aws_access_key_id, aws_secret_access_key)
    bucket = Bucket(conn, bucket_name)
    return S3(bucket, layer_name, path, reduced_redundancy)
