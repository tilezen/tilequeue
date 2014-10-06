# define locations to store the rendered data

from boto import connect_s3
from boto.s3.bucket import Bucket
from cStringIO import StringIO
from TileStache.S3 import tile_key
import sys


class S3(object):

    def __init__(self, bucket, layer_name, path='', reduced_redundancy=False):
        self.bucket = bucket
        self.layer = StubLayer(layer_name)
        self.path = path
        self.reduced_redundancy = reduced_redundancy

    def output_fp(self, coord, format):
        key_name = tile_key(self.layer, coord, format.extension, self.path)
        key = self.bucket.new_key(key_name)
        return S3FileObj(key, format.mimetype, self.reduced_redundancy)


class StubLayer(object):

    def __init__(self, layer_name):
        self.layer_name = layer_name

    def name(self):
        return self.layer_name


class TileFile(object):

    def __init__(self, fp):
        self.fp = fp

    def output_fp(self, coord, format):
        return self.fp


def make_tile_file_store(fp=None):
    if fp is None:
        fp = sys.stdout
    return TileFile(fp)


class Memory(object):

    def output_fp(self, coord, format):
        return StringIO()


class S3FileObj(object):

    def __init__(self, key, mimetype, reduced_redundancy):
        self.key = key
        self.headers = {'Content-Type': mimetype}
        self.buffer = StringIO()
        self.reduced_redundancy = reduced_redundancy

    def write(self, *args):
        self.buffer.write(*args)

    def close(self):
        self.buffer.seek(0)
        self.key.set_contents_from_file(
            self.buffer,
            headers=self.headers,
            policy='public-read',
            reduced_redundancy=self.reduced_redundancy,
        )

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()


def make_s3_store(bucket_name,
                  aws_access_key_id=None, aws_secret_access_key=None,
                  layer_name='all', path='', reduced_redundancy=False):
    conn = connect_s3(aws_access_key_id, aws_secret_access_key)
    bucket = Bucket(conn, bucket_name)
    return S3(bucket, layer_name, path, reduced_redundancy)
