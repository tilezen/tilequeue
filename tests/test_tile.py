from itertools import groupby
from math import pow
from ModestMaps.Core import Coordinate
from operator import attrgetter
import unittest


class TestSeedTiles(unittest.TestCase):

    def _call_fut(self, zoom_until):
        from tilequeue.tile import seed_tiles
        return list(seed_tiles(zoom_until=zoom_until))

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


class TestCoordToBounds(unittest.TestCase):

    def test_convert_coord(self):
        from tilequeue.tile import coord_to_bounds
        from ModestMaps.Core import Coordinate
        coord = Coordinate(zoom=14, column=4824, row=6160)
        bounds = coord_to_bounds(coord)
        exp_bounds = (-74.00390625, 40.69729900863674,
                      -73.98193359375, 40.713955826286046)
        self.assertEqual(exp_bounds, bounds)


class TestTileGeneration(unittest.TestCase):

    def _is_zoom(self, zoom):
        return lambda coord: zoom == coord.zoom

    def test_tiles_for_coord(self):
        from ModestMaps.Core import Coordinate
        from tilequeue.tile import coord_to_bounds
        from tilequeue.tile import tile_generator_for_single_bounds
        coord = Coordinate(1, 1, 1)
        bounds = coord_to_bounds(coord)
        tile_generator = tile_generator_for_single_bounds(bounds, 1, 1)
        tiles = list(tile_generator)
        self.assertEqual(1, len(tiles))

    def test_tiles_for_bounds_firsttile_two_zooms(self):
        from tilequeue.tile import tile_generator_for_single_bounds
        bounds = (-180, 0.1, -0.1, 85)
        tile_generator = tile_generator_for_single_bounds(bounds, 1, 2)
        tiles = list(tile_generator)
        self.assertEqual(5, len(tiles))
        self.assertEqual(1, len(filter(self._is_zoom(1), tiles)))
        self.assertEqual(4, len(filter(self._is_zoom(2), tiles)))

    def test_tiles_for_bounds_lasttile_two_zooms(self):
        from tilequeue.tile import tile_generator_for_single_bounds
        bounds = (0.1, -85, 180, -0.1)
        tile_generator = tile_generator_for_single_bounds(bounds, 1, 2)
        tiles = list(tile_generator)
        self.assertEqual(5, len(tiles))
        self.assertEqual(1, len(filter(self._is_zoom(1), tiles)))
        self.assertEqual(4, len(filter(self._is_zoom(2), tiles)))

    def test_tiles_for_two_bounds_two_zooms(self):
        from tilequeue.tile import tile_generator_for_multiple_bounds
        bounds1 = (-180, 0.1, -0.1, 85)
        bounds2 = (0.1, -85, 180, -0.1)
        tile_generator = tile_generator_for_multiple_bounds(
            (bounds1, bounds2), 1, 2)
        tiles = list(tile_generator)
        self.assertEqual(10, len(tiles))
        self.assertEqual(2, len(filter(self._is_zoom(1), tiles)))
        self.assertEqual(8, len(filter(self._is_zoom(2), tiles)))
