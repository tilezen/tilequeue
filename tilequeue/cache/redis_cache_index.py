from ModestMaps.Core import Coordinate


class RedisCacheIndex(object):

    def __init__(self, redis_client, cache_set_key='tilestache.cache'):
        self.redis_client = redis_client
        self.redis_client.set_response_callback('SMEMBERS',
                                                lambda l: [int(i) for i in l])
        self.cache_set_key = cache_set_key

    def get_list(self):
        return self.redis_client.smembers(self.cache_set_key)

    def index_coord(self, coord):
        redis_coord_value = serialize_coord_to_redis_value(coord)
        self.redis_client.sadd(self.cache_set_key, redis_coord_value)

    def write_coords_redis_protocol(self, out, set_key, coords):
        # coords is expected to be an iterable of coord objects
        # this is meant to be called with out sent to stdout and then piped to
        # redis-cli --pipe
        key_len = len(set_key)
        for coord in coords:
            redis_value = serialize_coord_to_redis_value(coord)

            # http://redis.io/topics/protocol
            # An attempt was made to send over integers directly via the
            # protocol, but it looks like redis wants strings. It seems like it
            # ends up storing the strings as integers anyway.
            redis_value = str(redis_value)
            val_len = len(redis_value)
            coord_insert = (
                '*3\r\n'
                '$4\r\nSADD\r\n'
                '$%(key_len)d\r\n%(key)s\r\n'
                '$%(val_len)d\r\n%(val)s\r\n' % dict(
                    key_len=key_len,
                    val_len=val_len,
                    key=set_key,
                    val=redis_value,
                )
            )

            out.write(coord_insert)

    def find_intersection(self, diff_set_key):
        intersection = self.redis_client.sinter(
            self.cache_set_key, diff_set_key)
        for redis_value in intersection:
            coord = deserialize_redis_value_to_coord(redis_value)
            yield coord

    def remove_key(self, diff_set_key):
        self.redis_client.delete(diff_set_key)


# The tiles will get encoded into integers suitable for redis to store. When
# redis is given integers, it is able to store them efficiently. Note that the
# integers are sent over to redis as a string. Another format was tried which
# packed the data into 6 bytes and then sent those 6 bytes as a string, but
# that actually took more memory in redis, presumably because raw integers can
# be stored more efficiently.

# This is how the data is encoded into a 64 bit integer:
# 9 bits unused | 25 bits column | 25 bits row | 5 bits zoom
zoom_mask = int('1' * 5, 2)
row_mask = int(('1' * 25), 2)
col_mask = row_mask
row_offset = 5
col_offset = 25 + 5


def serialize_coord_to_redis_value(coord):
    zoom = int(coord.zoom)
    column = int(coord.column)
    row = int(coord.row)
    val = zoom | (row << row_offset) | (column << col_offset)
    return val


def deserialize_redis_value_to_coord(redis_value):
    if isinstance(redis_value, (str, unicode)):
        redis_value = int(redis_value)
    zoom = zoom_mask & redis_value
    row = row_mask & (redis_value >> row_offset)
    column = col_mask & (redis_value >> col_offset)
    return Coordinate(column=column, row=row, zoom=zoom)
