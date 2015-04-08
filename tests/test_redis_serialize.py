import unittest


class RedisSerializeTest(unittest.TestCase):

    def _makeone(self):
        from tilequeue.cache import RedisCacheIndex
        from mock import MagicMock
        redis_client_mock = MagicMock()
        return RedisCacheIndex(redis_client_mock)

    def test_serialize_one(self):
        cache_index = self._makeone()
        from StringIO import StringIO
        out = StringIO()
        from ModestMaps.Core import Coordinate
        coords = [
            Coordinate(1, 2, 3),
        ]
        cache_index.write_coords_redis_protocol(out, 'diffs', coords)
        exp = '*3\r\n$4\r\nSADD\r\n$5\r\ndiffs\r\n$11\r\n34359738403\r\n'
        self.assertEquals(exp, out.getvalue())

    def test_roundtrip_serialization(self):
        from tilequeue.tile import coord_marshall_int
        from tilequeue.tile import coord_unmarshall_int
        from tilequeue.tile import seed_tiles
        from ModestMaps.Core import Coordinate
        from itertools import chain
        seed_coords = seed_tiles(0, 5)
        example_coords = [
            Coordinate(zoom=20, column=1002463, row=312816),
            Coordinate(zoom=30, column=12345678, row=12345678),
        ]
        coords = chain(seed_coords, example_coords)
        for coord in coords:
            self.assertEquals(
                coord,
                coord_unmarshall_int(
                    coord_marshall_int(coord)))


class CoordIntZoomTest(unittest.TestCase):

    def test_verify_low_seed_tiles(self):
        from tilequeue.tile import coord_int_zoom_up
        from tilequeue.tile import coord_marshall_int
        from tilequeue.tile import seed_tiles
        seed_coords = seed_tiles(1, 5)
        for coord in seed_coords:
            coord_int = coord_marshall_int(coord)
            parent_coord = coord.zoomTo(coord.zoom - 1).container()
            exp_int = coord_marshall_int(parent_coord)
            act_int = coord_int_zoom_up(coord_int)
            self.assertEquals(exp_int, act_int)

    def test_verify_examples(self):
        from ModestMaps.Core import Coordinate
        from tilequeue.tile import coord_int_zoom_up
        from tilequeue.tile import coord_marshall_int
        test_coords = (
            Coordinate(zoom=20, column=1002463, row=312816),
            Coordinate(zoom=20, column=(2 ** 20)-1, row=(2 ** 20)-1),
            Coordinate(zoom=10, column=(2 ** 10)-1, row=(2 ** 10)-1),
            Coordinate(zoom=5, column=20, row=20),
            Coordinate(zoom=1, column=0, row=0),
        )
        for coord in test_coords:
            coord_int = coord_marshall_int(coord)
            parent_coord = coord.zoomTo(coord.zoom - 1).container()
            exp_int = coord_marshall_int(parent_coord)
            act_int = coord_int_zoom_up(coord_int)
            self.assertEquals(exp_int, act_int)
