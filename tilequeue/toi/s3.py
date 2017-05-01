import boto
from cStringIO import StringIO
from tilequeue.toi import (
    load_set_from_gzipped_fp,
    save_set_to_gzipped_fp,
)


class S3TilesOfInterestSet(object):
    def __init__(self, bucket, key):
        s3 = boto.connect_s3()
        buk = s3.get_bucket(bucket)
        self.key = buk.get_key(key, validate=False)

    def fetch_tiles_of_interest(self):
        toi_data_gz = StringIO()
        self.key.get_contents_to_file(toi_data_gz)
        toi_data_gz.seek(0)

        return load_set_from_gzipped_fp(toi_data_gz)

    def set_tiles_of_interest(self, new_set):
        toi_data_gz = StringIO()
        save_set_to_gzipped_fp(new_set, toi_data_gz)
        self.key.set_contents_from_string(toi_data_gz.getvalue())
