from itertools import groupby
from math import pow
from ModestMaps.Core import Coordinate
from operator import attrgetter
import unittest

class TestSeedTiles(unittest.TestCase):

    def _call_fut(self, until_zoom):
        from tilequeue.seed import seed_tiles
        return list(seed_tiles(until_zoom=until_zoom))

    def _assert_tilelist(self, expected_tiles, actual_tiles):
        expected_tiles.sort()
        actual_tiles.sort()
        self.assertEqual(expected_tiles, actual_tiles)

    def test_zoom_0(self):
        tiles = self._call_fut(0)
        self.assertEqual([Coordinate(0, 0, 0)], tiles)

    def test_zoom_1(self):
        tiles = self._call_fut(1)
        self.assertEqual(5, len(tiles))
        expected_tiles = [
            Coordinate(0, 0, 0),
            Coordinate(0, 0, 1),
            Coordinate(1, 0, 1),
            Coordinate(0, 1, 1),
            Coordinate(1, 1, 1),
            ]
        self._assert_tilelist(expected_tiles, tiles)

    def test_zoom_5(self):
        tiles = self._call_fut(5)
        # sorts by zoom (first), which is why group by zoom will work
        tiles.sort()
        for zoom, tiles_per_zoom in groupby(tiles, attrgetter('zoom')):
            expected_num_tiles = pow(4, zoom)
            actual_num_tiles = len(list(tiles_per_zoom))
            self.assertEqual(expected_num_tiles, actual_num_tiles)
