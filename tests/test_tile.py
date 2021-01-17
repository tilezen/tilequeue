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
        self.assertEqual(tuple, type(bounds))
        self.assertEqual(len(exp_bounds), len(bounds))
        for i in range(0, len(exp_bounds)):
            exp = exp_bounds[i]
            act = bounds[i]
            self.assertAlmostEqual(
                exp, act, msg="Expected %r but got %r at index %d" %
                (exp, act, i))


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

    def test_tiles_children(self):
        from tilequeue.tile import coord_children
        from ModestMaps.Core import Coordinate
        coord = Coordinate(0, 0, 0)
        children = coord_children(coord)
        self.assertEqual(4, len(children))
        self.assertEqual(Coordinate(0, 0, 1), children[0])
        self.assertEqual(Coordinate(1, 0, 1), children[1])
        self.assertEqual(Coordinate(0, 1, 1), children[2])
        self.assertEqual(Coordinate(1, 1, 1), children[3])

    def test_tiles_children_range(self):
        from tilequeue.tile import coord_children
        from tilequeue.tile import coord_children_range
        from ModestMaps.Core import Coordinate
        coord = Coordinate(3, 4, 2)
        actual = list(coord_children_range(coord, 4))
        self.assertEqual(20, len(actual))
        children = list(coord_children(coord))
        grandchildren_list = map(coord_children, children)
        from itertools import chain
        grandchildren = list(chain(*grandchildren_list))
        exp = children + grandchildren
        actual.sort()
        exp.sort()
        for actual_child, exp_child in zip(actual, exp):
            self.assertEqual(exp_child, actual_child)

    def test_tiles_children_subrange(self):
        from tilequeue.tile import coord_children_subrange as subrange
        from tilequeue.tile import coord_children

        def _c(z, x, y):
            return Coordinate(zoom=z, column=x, row=y)

        # for any given coord, the subrange from and until the tile's zoom
        # should just return the coord itself.
        for coord in (_c(0, 0, 0), _c(1, 1, 1), _c(10, 163, 395)):
            z = coord.zoom
            self.assertEqual(set([coord]), set(subrange(coord, z, z)))

        # when until zoom > coordinate zoom, it should generate the whole
        # pyramid.
        expect = set([_c(0, 0, 0)])
        zoom = 0
        while zoom < 5:
            self.assertEqual(expect, set(subrange(_c(0, 0, 0), 0, zoom)))
            children = []
            for c in expect:
                children.extend(coord_children(c))
            expect |= set(children)
            zoom += 1

        # when both start and until are >, then should generate a slice of
        # the pyramid.
        for z in range(0, 5):
            max_coord = 2 ** z
            all_tiles = set(_c(z, x, y)
                            for x in range(0, max_coord)
                            for y in range(0, max_coord))

            self.assertEqual(all_tiles, set(subrange(_c(0, 0, 0), z, z)))

        # when start > until, then nothing is generated
        for z in range(0, 5):
            self.assertEqual(set(), set(subrange(_c(0, 0, 0), z+1, z)))

    def test_tiles_low_zooms(self):
        from tilequeue.tile import tile_generator_for_single_bounds
        bounds = -1.115, 50.941, 0.895, 51.984
        tile_generator = tile_generator_for_single_bounds(bounds, 0, 5)
        tiles = list(tile_generator)
        self.assertEqual(11, len(tiles))


class TestReproject(unittest.TestCase):

    def test_reproject(self):
        from tilequeue.tile import reproject_lnglat_to_mercator
        coord = reproject_lnglat_to_mercator(0, 0)
        self.assertAlmostEqual(0, coord[0])
        self.assertAlmostEqual(0, coord[1])

    def test_reproject_with_z(self):
        from tilequeue.tile import reproject_lnglat_to_mercator
        coord = reproject_lnglat_to_mercator(0, 0, 0)
        self.assertAlmostEqual(0, coord[0])
        self.assertAlmostEqual(0, coord[1])


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


class TestMetatileZoom(unittest.TestCase):

    def test_zoom_from_size(self):
        from tilequeue.tile import metatile_zoom_from_size as func
        self.assertEqual(0, func(None))
        self.assertEqual(0, func(1))
        self.assertEqual(1, func(2))
        self.assertEqual(2, func(4))

        with self.assertRaises(AssertionError):
            func(3)

    def test_zoom_from_str(self):
        from tilequeue.tile import metatile_zoom_from_str as func
        self.assertEqual(0, func(None))
        self.assertEqual(0, func(""))
        self.assertEqual(0, func("256"))
        self.assertEqual(1, func("512"))
        self.assertEqual(2, func("1024"))
