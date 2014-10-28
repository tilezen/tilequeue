import unittest


class RedisSerializeTest(unittest.TestCase):

    def _makeone(self):
        from tilequeue.cache import RedisCacheIndex
        return RedisCacheIndex(None)

    def test_serialize_one(self):
        cache_index = self._makeone()
        from StringIO import StringIO
        out = StringIO()
        from ModestMaps.Core import Coordinate
        coords = [
            Coordinate(1, 2, 3),
        ]
        cache_index.write_coords_redis_protocol(out, 'diffs', coords)
        exp = '*3\r\n$4\r\nSADD\r\n$5\r\ndiffs\r\n$10\r\n2147483683\r\n'
        self.assertEquals(exp, out.getvalue())

    def test_roundtrip_serialization(self):
        from tilequeue.cache import serialize_coord_to_redis_value
        from tilequeue.cache import deserialize_redis_value_to_coord
        from tilequeue.tile import seed_tiles
        coords = seed_tiles(0, 5)
        for coord in coords:
            self.assertEquals(
                coord,
                deserialize_redis_value_to_coord(
                    serialize_coord_to_redis_value(coord)))
