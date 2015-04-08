from tilequeue.tile import coord_marshall_int
from tilequeue.tile import coord_unmarshall_int


class RedisCacheIndex(object):

    def __init__(self, redis_client,
                 cache_set_key='tilequeue.tiles-of-interest'):
        self.redis_client = redis_client
        self.cache_set_key = cache_set_key

    def intersect(self, coords, tiles_of_interest=None):
        if tiles_of_interest is None:
            tiles_of_interest = self.fetch_tiles_of_interest()
        for coord in coords:
            serialized_coord = coord_marshall_int(coord)
            if serialized_coord in tiles_of_interest:
                yield coord

    def fetch_tiles_of_interest(self):
        raw_tiles_of_interest = self.redis_client.smembers(self.cache_set_key)
        tiles_of_interest = set()
        for raw_tile in raw_tiles_of_interest:
            raw_tile_int = int(raw_tile)
            tiles_of_interest.add(raw_tile_int)
        return tiles_of_interest

    def index_coord(self, coord):
        self.index_coords([coord])

    def index_coords(self, coords):
        batch_size = 100
        buf = []
        for coord in coords:
            redis_coord_value = coord_marshall_int(coord)
            buf.append(redis_coord_value)
            if len(buf) >= batch_size:
                self.redis_client.sadd(self.cache_set_key, *buf)
                del buf[:]
        if buf:
            self.redis_client.sadd(self.cache_set_key, *buf)

    def write_coords_redis_protocol(self, out, set_key, coords):
        # coords is expected to be an iterable of coord objects
        # this is meant to be called with out sent to stdout and then piped to
        # redis-cli --pipe
        key_len = len(set_key)
        for coord in coords:
            coord_int = coord_marshall_int(coord)

            # http://redis.io/topics/protocol
            # An attempt was made to send over integers directly via the
            # protocol, but it looks like redis wants strings. It seems like it
            # ends up storing the strings as integers anyway.
            coord_int = str(coord_int)
            val_len = len(coord_int)
            coord_insert = (
                '*3\r\n'
                '$4\r\nSADD\r\n'
                '$%(key_len)d\r\n%(key)s\r\n'
                '$%(val_len)d\r\n%(val)s\r\n' % dict(
                    key_len=key_len,
                    val_len=val_len,
                    key=set_key,
                    val=coord_int,
                )
            )

            out.write(coord_insert)

    def find_intersection(self, diff_set_key):
        intersection = self.redis_client.sinter(
            self.cache_set_key, diff_set_key)
        for coord_int in intersection:
            coord = coord_unmarshall_int(coord_int)
            yield coord

    def remove_key(self, diff_set_key):
        self.redis_client.delete(diff_set_key)
