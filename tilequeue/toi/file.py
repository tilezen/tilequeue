from tilequeue.tile import (
    coord_marshall_int,
    coord_unmarshall_int,
    deserialize_coord,
    serialize_coord,
)
import gzip


def save_set_to_fp(the_set, fp):
    for coord_int in sorted(the_set):
        coord = coord_unmarshall_int(coord_int)
        fp.write(serialize_coord(coord))
        fp.write('\n')


def load_set_from_fp(fp):
    toi_set = set()

    for coord_str in fp:
        coord = deserialize_coord(coord_str)
        coord_int = coord_marshall_int(coord)
        toi_set.add(coord_int)

    return toi_set


def load_set_from_gzipped_fp(gzipped_fp):
    fp = gzip.GzipFile(fileobj=gzipped_fp, mode='r')
    return load_set_from_fp(fp)


def save_set_to_gzipped_fp(the_set, fp):
    gzipped_fp = gzip.GzipFile(fileobj=fp, mode='w')
    save_set_to_fp(the_set, gzipped_fp)
    gzipped_fp.close()


class FileTilesOfInterestSet(object):
    def __init__(self, filename):
        self.filename = filename

    def fetch_tiles_of_interest(self):
        toi_set = set()

        with open(self.filename, 'r') as toi_data:
            toi_set = load_set_from_gzipped_fp(toi_data)

        return toi_set

    def set_tiles_of_interest(self, new_set):
        with open(self.filename, 'w') as toi_data:
            save_set_to_gzipped_fp(new_set, toi_data)
