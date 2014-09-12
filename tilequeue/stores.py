# define locations to store the rendered data

from boto.s3.bucket import Bucket as S3Bucket
from boto.s3.connection import S3Connection
from cStringIO import StringIO
from TileStache.S3 import tile_key


class S3(object):

    def __init__(self, layer, bucket, access=None, secret=None, path='', reduced_redundancy=False):
        self.layer = StubLayer(layer)
        self.bucket = S3Bucket(S3Connection(access, secret), bucket)
        self.path = path
        self.reduced_redundancy = reduced_redundancy

    def output_fp(self, coord, extension, mimetype):
        key_name = tile_key(self.layer, coord, extension, self.path)
        key = self.bucket.new_key(key_name)
        return S3FileObj(key, mimetype, self.reduced_redundancy)


class StubLayer(object):

    def __init__(self, layer):
        self.layer = layer

    def name(self):
        return self.layer

class Memory(object):

    def __init__(self):
        pass

    def output_fp(self, coord, extension, mimetype):
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
                headers = self.headers,
                policy = 'public-read',
                reduced_redundancy = self.reduced_redundancy,
                )

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()
