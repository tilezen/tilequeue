import boto
import gzip
from cStringIO import StringIO
from tilequeue.tile import (
    coord_marshall_int,
    coord_unmarshall_int,
    deserialize_coord,
    serialize_coord,
)


class S3TilesOfInterestSet(object):
    def __init__(self, bucket, key):
        s3 = boto.connect_s3()
        buk = s3.get_bucket(bucket)
        self.key = buk.get_key(key)

    def fetch_tiles_of_interest(self):
        toi_data_gz = StringIO()
        self.key.get_contents_to_file(toi_data_gz)
        gz = gzip.GzipFile(fileobj=toi_data_gz, mode='r')

        toi_set = set()

        for coord_str in gz:
            coord = deserialize_coord(coord_str)
            coord_int = coord_marshall_int(coord)
            toi_set.add(coord_int)

        return toi_set

    def set_tiles_of_interest(self, new_set):
        toi_data_gz = StringIO()
        gz = gzip.GzipFile(fileobj=toi_data_gz, mode='w')

        for coord_int in sorted(new_set):
            coord = coord_unmarshall_int(coord_int)
            gz.write(serialize_coord(coord))
            gz.write('\n')

        gz.close()

        self.key.set_contents_from_string(gz.getvalue())
